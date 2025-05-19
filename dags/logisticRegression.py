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

    # customSchema = StructType([
    #         StructField("HK_LINK_TRANSACTION", StringType(), True),
    #         StructField("Transaction_Date", StringType(), True),
    #         StructField("Transaction_Amount", StringType(), True),
    #         StructField("Transaction_Currency", StringType(), True),
    #         StructField("Transaction_IP", StringType(), True),
    #         StructField("Transaction_Fraud", LongType(), True),
    #         StructField("Card_Number", StringType(), True),
    #         StructField("Card_Expiry_Date", StringType(), True),
    #         StructField("Card_Provider", StringType(), True),
    #         StructField("Card_Issued_Date", StringType(), True),
    #         StructField("Card_Last_Update", StringType(), True),
    #         StructField("Card_Credit_Limit", StringType(), True),
    #         StructField("Customer_Name", StringType(), True),
    #         StructField("Customer_Email", StringType(), True),
    #         StructField("Customer_Address", StringType(), True),
    #         StructField("Customer_Birth_Date", StringType(), True),
    #         StructField("Customer_Last_Update", StringType(), True),
    #         StructField("Merchant_Address", StringType(), True),
    #         StructField("Merchant_Category", StringType(), True),
    #         StructField("Merchant_City", StringType(), True),
    #         StructField("Merchant_Country", StringType(), True),
    #         StructField("Merchant_Created_At", StringType(), True),
    #         StructField("Merchant_Name", StringType(), True),
    #         StructField("geo_city", StringType(), True),
    #         StructField("geo_region", StringType(), True),
    #         # StructField("geo_lat", DoubleType(), True),
    #         # StructField("geo_lon", DoubleType(), True),
    #         StructField("geo_country", StringType(), True),
    #     ])

    query = """
          SELECT 
          CAST(HK_LINK_TRANSACTION AS STRING) AS HK_LINK_TRANSACTION,
          CAST(Transaction_Date AS STRING) AS Transaction_Date,
          CAST(Transaction_Amount AS STRING) AS Transaction_Amount,
          CAST(Transaction_Currency AS STRING) AS Transaction_Currency,
          CAST(Transaction_IP AS STRING) AS Transaction_IP,
          CAST(Transaction_Fraud AS STRING) AS Transaction_Fraud,
          CAST(Card_Number AS STRING) AS Card_Number,
          CAST(Card_Expiry_Date AS STRING) AS Card_Expiry_Date,
          CAST(Card_Provider AS STRING) AS Card_Provider,
          CAST(Card_Issued_Date AS STRING) AS Card_Issued_Date,
          CAST(Card_Last_Update AS STRING) AS Card_Last_Update,
          CAST(Card_Credit_Limit AS STRING) AS Card_Credit_Limit,
          CAST(Customer_Name AS STRING) AS Customer_Name,
          CAST(Customer_Email AS STRING) AS Customer_Email,
          CAST(Customer_Address AS STRING) AS Customer_Address,
          CAST(Customer_Birth_Date AS STRING) AS Customer_Birth_Date,
          CAST(Customer_Last_Update AS STRING) AS Customer_Last_Update,
          CAST(Merchant_Address AS STRING) AS Merchant_Address,
          CAST(Merchant_Category AS STRING) AS Merchant_Category,
          CAST(Merchant_City AS STRING) AS Merchant_City,
          CAST(Merchant_Country AS STRING) AS Merchant_Country,
          CAST(Merchant_Created_At AS STRING) AS Merchant_Created_At,
          CAST(Merchant_Name AS STRING) AS Merchant_Name,
          CAST(geo_city AS STRING) AS geo_city,
          CAST(geo_region AS STRING) AS geo_region,
          CAST(geo_country AS STRING) AS geo_country
          FROM fraud_miner.silver.fraud_geo_view"""
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", query) \
        .option("driver", "com.databricks.client.jdbc.Driver") \
        .load()

    df = df.withColumn("Transaction_Amount", col("Transaction_Amount").cast("double"))
    df = df.withColumn("Card_Credit_Limit", col("Card_Credit_Limit").cast("double"))
    df = df.withColumn("Transaction_Fraud", col("Transaction_Fraud").cast("int"))


    # Feature Engineering
    df = df.withColumn("geo_matches_merchant", (df["geo_country"] == df["Merchant_Country"]).cast("int"))

    df = df.fillna({
                "Transaction_Amount": 0.0,
                "geo_matches_merchant": 0
            })

    categorical_cols = ["Card_Provider", "Merchant_Category", "Merchant_Country", "geo_country"]
    numeric_cols = ["Transaction_Amount", "geo_matches_merchant"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec") for col in categorical_cols]
    df = df.filter(df["Transaction_Fraud"].isNotNull())
    assembler = VectorAssembler(
        inputCols=[f"{col}_vec" for col in categorical_cols] + numeric_cols,
        outputCol="features",
        handleInvalid="skip"  # <--- ADD THIS LINE
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
