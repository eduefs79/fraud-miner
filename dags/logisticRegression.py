from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from io import BytesIO
import os
import pandas as pd
import joblib
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
import databricks.sql as sql

def upload_df_to_s3(df, bucket, key, aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.load_file_obj(buffer, key=key, bucket_name=bucket, replace=True)
    print(f"✅ Uploaded to s3://{bucket}/{key}")

def train_and_store_scores():
    # Step 1: Connect to Databricks
    conn = BaseHook.get_connection("fraud_databricks")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    connection = sql.connect(server_hostname=host, http_path=http_path, access_token=token)

    # Step 2: Load data from view
    query = "SELECT * FROM fraud_miner.silver.fraud_geo_view"
    df = pd.read_sql(query, connection)

    # Step 3: Feature engineering
    df["geo_matches_merchant"] = (df["geo_country"] == df["Merchant_Country"]).astype(int)
    target = "Transaction_Fraud"
    features = [
        "Transaction_Amount",
        "Card_Provider",
        "Merchant_Category",
        "Merchant_Country",
        "geo_country",
        "geo_matches_merchant"
    ]
    X = df[features]
    y = df[target]

    numeric_features = ["Transaction_Amount", "geo_matches_merchant"]
    categorical_features = list(set(features) - set(numeric_features))

    numeric_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(handle_unknown="ignore", sparse=False)

    preprocessor = ColumnTransformer([
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features)
    ])

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", LogisticRegression(max_iter=1000))
    ])

    # Step 4: K-Fold validation
    kf = KFold(n_splits=10, shuffle=True, random_state=42)
    scores = cross_val_score(pipeline, X, y, cv=kf, scoring='accuracy')
    results_df = pd.DataFrame({
        "run_date": [datetime.now()] * len(scores),
        "fold": list(range(1, len(scores) + 1)),
        "accuracy": scores
    })

    # Step 5: Fit model and save test predictions
    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=42)
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    preds_df = pd.DataFrame({
        "run_date": [datetime.now()] * len(y_test),
        "true_label": y_test.tolist(),
        "predicted_label": y_pred.tolist()
    })

    # Step 6: Save model locally
    os.makedirs("/tmp/fraud_models", exist_ok=True)
    model_path = "/tmp/fraud_models/logreg_model.pkl"
    joblib.dump(pipeline, model_path)

    bucket = 'fraud-miner'
    # Step 7: Save model to S3
    s3 = S3Hook(aws_conn_id="aws_default")
    with open(model_path, "rb") as f:
        s3.load_file_obj(f, key="model/logreg_model.pkl", bucket_name=bucket, replace=True)
        print("✅ Model uploaded to S3")

    # Step 8: Save evaluation results to Databricks
    cursor = connection.cursor()

    cursor.execute("""CREATE SCHEMA IF NOT EXISTS fraud_miner.gold;""")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_miner.gold.model_evaluation (
            run_date TIMESTAMP,
            fold INT,
            accuracy DOUBLE
        )
    """)
    for _, row in results_df.iterrows():
        cursor.execute(f"""
            INSERT INTO fraud_miner.gold.model_evaluation (run_date, fold, accuracy)
            VALUES ('{row['run_date']}', {int(row['fold'])}, {row['accuracy']})
        """)

    # Step 9: Save predictions to Databricks
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_miner.gold.model_predictions (
            run_date TIMESTAMP,
            true_label INT,
            predicted_label INT
        )
    """)
    for _, row in preds_df.iterrows():
        cursor.execute(f"""
            INSERT INTO fraud_miner.gold.model_predictions (run_date, true_label, predicted_label)
            VALUES ('{row['run_date']}', {int(row['true_label'])}, {int(row['predicted_label'])})
        """)

    cursor.close()
    connection.close()

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
