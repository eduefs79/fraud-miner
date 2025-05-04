df_link_client_card = df_card.withColumn("HK_CLIENT", sha2(concat_ws("||", "client_id"), 256)) \
                             .withColumn("HK_CREDIT_CARD", sha2(concat_ws("||", "card_id"), 256)) \
                             .withColumn("HK_LINK_CLIENT_CARD", sha2(concat_ws("||", "client_id", "card_id"), 256)) \
                             .withColumn("LOAD_DATE", current_timestamp()) \
                             .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_link_client_card.select("HK_LINK_CLIENT_CARD", "HK_CLIENT", "HK_CREDIT_CARD", "LOAD_DATE", "RECORD_SOURCE") \
                   .write.format("delta").mode("overwrite").saveAsTable("LINK_CLIENT_CARD")