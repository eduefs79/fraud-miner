from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from mylibs.aws_secrets import get_secret
from datetime import datetime
import json
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import col, when
from mylibs.utilities import is_numeric_udf
from airflow.models import Variable

PYTHON_PATH = Variable.get("PYSPARK_PYTHON", "/usr/bin/python3.8")
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH



def save_to_s3(local_path: str, s3_key: str, bucket_name: str = "fraud-miner", aws_conn_id: str = "aws_s3_conn_fraud"):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_file(filename=local_path, key=s3_key, bucket_name=bucket_name, replace=True)

def train_with_spark():
    # Get secrets and config
    secret_raw = get_secret("databricks/token")
    parsed = json.loads(secret_raw)
    token = parsed.get("databricks")
    if not token:
        raise ValueError("❌ Databricks token not found")

    host = Variable.get("DATABRICKS_HOST")
    http_path = Variable.get("DATABRICKS_HTTP_PATH").split("/")[-1]  # get warehouse ID

    # Init SparkSession
    spark = SparkSession.builder \
        .appName("FraudLogisticRegression") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/opt/spark/jars/databricks-jdbc.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/databricks-jdbc.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/databricks-jdbc.jar") \
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3.8") \
        .config("spark.pyspark.python", "/usr/bin/python3.8") \
        .getOrCreate()

    jdbc_url = (
            f"jdbc:databricks://{host}:443/default"
            f";transportMode=http"
            f";ssl=1"
            f";AuthMech=3"
            f";httpPath=/sql/1.0/warehouses/{http_path}"
            f";UID=token"
            f";PWD={token}"
        )

    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "fraud_miner.silver.fraud_geo_table") \
        .option("driver", "com.databricks.client.jdbc.Driver") \
        .option("fetchsize", "1000") \
        .load()

    print("Record count" ,df.count())

    df.select("Transaction_Amount").distinct().filter(~col("Transaction_Amount").rlike("^[0-9.]+$")).show()

    df.printSchema()

    df = df.withColumn("Transaction_Amount", is_numeric_udf(col("Transaction_Amount")))
    df = df.withColumn("Card_Credit_Limit", is_numeric_udf(col("Card_Credit_Limit")))


    df = df.fillna({
                "Transaction_Amount": 0.0,
                "geo_country":"Unknown",
                "Merchant_Country":"Unknown",
                "Transaction_Fraud": 0,
                "Card_Provider":"Unknown",
                "Card_Credit_Limit":0
            })

    df = df.filter(
                col("Transaction_Amount").isNotNull() &
                col("Card_Credit_Limit").isNotNull() &
                col("Transaction_Fraud").isNotNull()
            )

    

    print("Record count after filtering" ,df.count())

    #Feature Engineering
    df = df.withColumn("geo_matches_merchant", (df["geo_country"] == df["Merchant_Country"]).cast("int"))

    categorical_cols = ["Card_Provider", "Merchant_Country", "geo_country"]
    numeric_cols = ["Transaction_Amount", "geo_matches_merchant","Card_Credit_Limit"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec") for col in categorical_cols]

    assembler = VectorAssembler(
        inputCols=[f"{col}_vec" for col in categorical_cols] + numeric_cols,
        outputCol="features",
        handleInvalid="skip" 
    )

    lr = LogisticRegression(labelCol="Transaction_Fraud", featuresCol="features", maxIter=100)

    pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])
    df_repartitioned = df.repartition(5)
    model = pipeline.fit(df_repartitioned)


    predictions = model.transform(df)
    accuracy = predictions.filter(predictions.prediction == predictions.Transaction_Fraud).count() / predictions.count()

    print(f"✅ PySpark Logistic Regression Accuracy: {accuracy:.4f}")

    # Local model save (to be zipped before upload)
    model_dir = "/tmp/logreg_model"
    model.write().overwrite().save(model_dir)
    
    # Zip model folder
    import shutil
    shutil.make_archive("/tmp/logreg_model", 'zip', model_dir)
    
    # Save accuracy
    eval_path = "/tmp/accuracy.txt"
    with open(eval_path, "w") as f:
        f.write(f"Accuracy: {accuracy:.4f} - {datetime.now()}")
    
    # Upload both to S3
    save_to_s3("/tmp/logreg_model.zip", "model/logreg_model.zip")
    save_to_s3(eval_path, "model/accuracy.txt")

    spark.stop()

    shutil.rmtree(model_dir)
    os.remove("/tmp/logreg_model.zip")
    os.remove(eval_path)

# DAG definition
with DAG(
    dag_id="pyspark_logreg_from_databricks",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "pyspark", "databricks"]
) as dag:

    train_task = PythonOperator(
        task_id="train_logreg_model",
        python_callable=train_with_spark
    )
