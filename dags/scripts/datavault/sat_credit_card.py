df_sat_card = df_card.withColumn("HK_CREDIT_CARD", sha2(concat_ws("||", "card_id"), 256)) \
                     .withColumn("LOAD_DATE", current_timestamp()) \
                     .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_sat_card.select("HK_CREDIT_CARD", "card_number", "card_type", "card_status", "LOAD_DATE", "RECORD_SOURCE") \
           .write.format("delta").mode("overwrite").saveAsTable("SAT_CREDIT_CARD")