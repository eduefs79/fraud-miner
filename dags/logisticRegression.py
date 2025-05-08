from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from io import BytesIO
import os
import joblib
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import create_map, col, lit
from itertools import chain



def train_and_store_scores():
    # Step 1: Connect to Databricks
    conn = BaseHook.get_connection("fraud_databricks")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /opt/spark/jars/databricks-jdbc.jar --driver-class-path /opt/spark/jars/databricks-jdbc.jar pyspark-shell"

    spark = SparkSession.builder \
    .appName("FraudDetectionModel") \
    .config("spark.jars", "/opt/spark/jars/databricks-jdbc.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/databricks-jdbc.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/databricks-jdbc.jar") \
    .getOrCreate()




    # Step 2: Load data from view
    df = spark.read \
    .format("jdbc") \
    .option("driver", "com.simba.spark.jdbc.Driver") \
    .option("url", f"jdbc:spark://{host}:443/default;transportMode=http;ssl=1;httpPath={http_path};AuthMech=3;UID=token;PWD={token}") \
    .option("query", "SELECT * FROM fraud_miner.silver.fraud_geo_view") \
    .load()



    # Step 3: Feature engineering
    df = df.withColumn("geo_matches_merchant", (df.geo_country == df.Merchant_Country).cast("int"))
    target = "Transaction_Fraud"

    numeric_cols = ["Transaction_Amount", "geo_matches_merchant"]
    categorical_cols = ["Card_Provider", "Merchant_Category", "Merchant_Country", "geo_country"]

    for col_name in categorical_cols:
        distinct_values = df.select(col_name).distinct().rdd.flatMap(lambda x: x).collect()
        mapping = {val: idx for idx, val in enumerate(sorted(distinct_values))}
        mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
        df = df.withColumn(f"{col_name}_idx", mapping_expr.getItem(col(col_name)).cast("int"))

    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec", handleInvalid="keep")
        for c in categorical_cols
    ]

    assembler = VectorAssembler(inputCols=[f"{c}_vec" for c in categorical_cols] + numeric_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    lr = LogisticRegression(labelCol=target, featuresCol="features")

    pipeline = Pipeline(stages=encoders + [assembler, scaler, lr])
    model = pipeline.fit(df)

    predictions = model.transform(df)

    evaluator = MulticlassClassificationEvaluator(labelCol=target, predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"✅ Spark model accuracy: {accuracy:.4f}")

    # Save predictions
    predictions.select(target, "prediction", "probability") \
        .write.mode("overwrite").saveAsTable("fraud_miner.gold.model_predictions")

    # Save evaluation result
    from pyspark.sql import Row
    result_row = Row(run_date=str(datetime.now()), accuracy=float(accuracy))
    spark.createDataFrame([result_row]) \
        .write.mode("append").saveAsTable("fraud_miner.gold.model_evaluation")

    # Save model
    os.makedirs("/tmp/fraud_models", exist_ok=True)
    model_path = "/tmp/fraud_models/logreg_model_spark"
    model.write().overwrite().save(model_path)

    s3 = S3Hook(aws_conn_id="aws_default")
    metadata_path = os.path.join(model_path, "metadata")
    for file in os.listdir(metadata_path):
        if file.startswith("part-"):
            with open(os.path.join(metadata_path, file), "rb") as f:
                s3.load_file_obj(f, key="model/logreg_model_spark_metadata", bucket_name="fraud-miner", replace=True)
            break

    spark.stop()


with DAG(
    dag_id="logistic_fraud_model_with_s3",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "modeling", "s3"]
) as dag:

    run_model = PythonOperator(
        task_id="train_and_store_model_scores",
        python_callable=train_and_store_scores
    )

    run_model
