df_hub_transaction = df_transaction.withColumn("HK_TRANSACTION", sha2(concat_ws("||", "transaction_id"), 256)) \
                                   .withColumn("LOAD_DATE", current_timestamp()) \
                                   .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_hub_transaction.select("HK_TRANSACTION", "transaction_id", "LOAD_DATE", "RECORD_SOURCE") \
                  .write.format("delta").mode("overwrite").saveAsTable("HUB_TRANSACTION")