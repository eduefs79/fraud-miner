df_link_card_transaction = df_transaction.withColumn("HK_CREDIT_CARD", sha2(concat_ws("||", "card_id"), 256)) \
                                         .withColumn("HK_TRANSACTION", sha2(concat_ws("||", "transaction_id"), 256)) \
                                         .withColumn("HK_LINK_CARD_TRANSACTION", sha2(concat_ws("||", "card_id", "transaction_id"), 256)) \
                                         .withColumn("LOAD_DATE", current_timestamp()) \
                                         .withColumn("RECORD_SOURCE", lit("fraud_source"))
df_link_card_transaction.select("HK_LINK_CARD_TRANSACTION", "HK_CREDIT_CARD", "HK_TRANSACTION", "LOAD_DATE", "RECORD_SOURCE") \
                        .write.format("delta").mode("overwrite").saveAsTable("LINK_CARD_TRANSACTION")