from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

# Init
spark = SparkSession.builder.appName("FraudDetectionModels").getOrCreate()
df = spark.read.table("fraud_miner.silver.fraud_geo_table")

def clean_and_cast_numeric(colname):
    return when(regexp_replace(col(colname), "[^0-9.]", "") == "", None) \
        .otherwise(regexp_replace(col(colname), "[^0-9.]", "").cast("double"))

# Clean + encode
df = df.withColumn("Transaction_Amount", clean_and_cast_numeric("Transaction_Amount")) \
       .withColumn("Card_Credit_Limit", clean_and_cast_numeric("Card_Credit_Limit")) \
       .withColumn("geo_lat", clean_and_cast_numeric("geo_lat")) \
       .withColumn("geo_lon", clean_and_cast_numeric("geo_lon")) \
       .withColumn("Transaction_Fraud", when(col("Transaction_Fraud").isin("0", "1"), col("Transaction_Fraud").cast("int")).otherwise(None)) \
       .fillna({
           "Transaction_Amount": 0.0,
           "geo_country": "Unknown",
           "Merchant_Country": "Unknown",
           "Transaction_Fraud": 0,
           "Card_Provider": "Unknown",
           "Card_Credit_Limit": 0
       })

# Filter + encode categories
df = df.filter(col("Transaction_Amount").isNotNull() & col("Card_Credit_Limit").isNotNull() & col("Transaction_Fraud").isNotNull())
df = df.withColumn("Card_Provider_idx", when(col("Card_Provider") == "VISA", 1).when(col("Card_Provider") == "MASTERCARD", 2).otherwise(0)) \
       .withColumn("Merchant_Country_idx", when(col("Merchant_Country") == "US", 1).when(col("Merchant_Country") == "UK", 2).otherwise(0)) \
       .withColumn("geo_country_idx", when(col("geo_country") == "US", 1).when(col("geo_country") == "UK", 2).otherwise(0)) \
       .withColumn("geo_matches_merchant", (col("geo_country") == col("Merchant_Country")).cast("int"))

# Feature assembly
assembler = VectorAssembler(
    inputCols=["Card_Provider_idx", "Merchant_Country_idx", "geo_country_idx", "Transaction_Amount", "geo_matches_merchant", "Card_Credit_Limit"],
    outputCol="features"
)
df_vector = assembler.transform(df)

# Define models
models = {
    "Logistic Regression": LogisticRegression(labelCol="Transaction_Fraud", featuresCol="features", maxIter=100),
    "Random Forest": RandomForestClassifier(labelCol="Transaction_Fraud", featuresCol="features", numTrees=50),
    "Decision Tree": DecisionTreeClassifier(labelCol="Transaction_Fraud", featuresCol="features")
}

for name, model in models.items():
    print(f"\n⚙️ Training: {name}")
    fitted_model = model.fit(df_vector)
    preds = fitted_model.transform(df_vector)
    pred_and_labels = preds.select("prediction", "Transaction_Fraud") \
                           .rdd.map(lambda row: (float(row[0]), float(row[1])))
    metrics = MulticlassMetrics(pred_and_labels)

    print("📊 Confusion Matrix:")
    print(metrics.confusionMatrix().toArray())
    print(f"✅ Accuracy: {preds.filter(preds.prediction == preds.Transaction_Fraud).count() / preds.count():.4f}")
    print(f"🔍 Precision: {metrics.precision(1.0):.4f} | Recall: {metrics.recall(1.0):.4f} | F1: {metrics.fMeasure(1.0):.4f}")
