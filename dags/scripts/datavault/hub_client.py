from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit

# Step 1: Read raw customer data
df_client = spark.read.parquet("/Volumes/fraud-miner/bronze/fraud_raw/customer/customers.parquet")

# Step 2: Create Hub table (Hub_Client)
df_hub_client = df_client.withColumn("HK_CLIENT", sha2(concat_ws("||", "client_id"), 256)) \
                         .withColumn("LOAD_DATE", current_timestamp()) \
                         .withColumn("RECORD_SOURCE", lit("fraud_source"))

# Step 3: Save Hub table as Delta
df_hub_client.select("HK_CLIENT", "client_id", "LOAD_DATE", "RECORD_SOURCE") \
             .write.format("delta").mode("overwrite").partitionBy("LOAD_DATE").saveAsTable("HUB_CLIENT")
