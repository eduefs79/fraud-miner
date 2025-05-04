df_link_card_merchant = df_transaction.withColumn("HK_CREDIT_CARD", sha2(concat_ws("||", "card_id"), 256)) \
                                      .withColumn("HK_MERCHANT", sha2(concat_ws("||", "merchant_id"), 256)) \
                                      .withColumn("HK_LINK_CARD_MERCHANT", sha2(concat_ws("||", "card_id", "merchant_id"), 256)) \
                                      .withColumn("LOAD_DATE", current_timestamp()) \
                                      .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_link_card_merchant.select("HK_LINK_CARD_MERCHANT", "HK_CREDIT_CARD", "HK_MERCHANT", "LOAD_DATE", "RECORD_SOURCE") \
                     .write.format("delta").mode("overwrite").saveAsTable("LINK_CARD_MERCHANT")