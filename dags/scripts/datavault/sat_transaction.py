df_sat_transaction = df_transaction.withColumn("HK_TRANSACTION", sha2(concat_ws("||", "transaction_id"), 256)) \
                                   .withColumn("LOAD_DATE", current_timestamp()) \
                                   .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_sat_transaction.select("HK_TRANSACTION", "amount", "transaction_date", "fraud_flag", "LOAD_DATE", "RECORD_SOURCE") \
                  .write.format("delta").mode("overwrite").saveAsTable("SAT_TRANSACTION")