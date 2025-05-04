df_hub_merchant = df_merchant.withColumn("HK_MERCHANT", sha2(concat_ws("||", "merchant_id"), 256)) \
                             .withColumn("LOAD_DATE", current_timestamp()) \
                             .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_hub_merchant.select("HK_MERCHANT", "merchant_id", "LOAD_DATE", "RECORD_SOURCE") \
               .write.format("delta").mode("overwrite").saveAsTable("HUB_MERCHANT")