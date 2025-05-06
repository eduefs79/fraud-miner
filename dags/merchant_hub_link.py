from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from databricks import sql
from datetime import datetime
import pandas as pd
import hashlib
from io import BytesIO

# ------------------ Helpers ------------------
def hash_key(*values):
    concat = '||'.join(str(v) for v in values)
    return hashlib.sha256(concat.encode('utf-8')).hexdigest()

def upload_df_to_s3(df, bucket, key, aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.load_file_obj(buffer, key=key, bucket_name=bucket, replace=True)
    print(f"✅ Uploaded to s3://{bucket}/{key}")

# ------------------ Silver Transformation ------------------
def process_merchant_to_silver(aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'
    key = 'bronze/fraud_raw/merchant/merchants.parquet'
    buffer = BytesIO(s3.get_key(key, bucket).get()['Body'].read())
    df = pd.read_parquet(buffer)

    df['HK_MERCHANT'] = df['merchant_id'].apply(lambda x: hash_key(x))
    load_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df['LOAD_DATE'] = load_date
    df['RECORD_SOURCE'] = 'bronze.merchant'

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('datetime64[us]')

    df_hub = df[['HK_MERCHANT', 'merchant_id', 'LOAD_DATE', 'RECORD_SOURCE']].drop_duplicates()
    df_sat = df[['HK_MERCHANT', 'merchant_name', 'category', 'address', 'city', 'country',
                 'created_at', 'updated_at', 'LOAD_DATE', 'RECORD_SOURCE']]

    upload_df_to_s3(df_hub, bucket, 'silver/hub/HUB_MERCHANT.parquet', aws_conn_id)
    upload_df_to_s3(df_sat, bucket, 'silver/sat/SAT_MERCHANT.parquet', aws_conn_id)

# ------------------ Unity Catalog Registration ------------------
def register_merchant_tables():
    conn = BaseHook.get_connection("fraud_databricks")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")

    connection = sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    )

    cursor = connection.cursor()

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_hub_merchant AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_hub/HUB_MERCHANT.parquet`
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_sat_merchant AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_sat/SAT_MERCHANT.parquet`
    """)

    cursor.close()
    connection.close()
    print("✅ Registered HUB and SAT views for merchant")

# ------------------ DAG Definition ------------------
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('silver_merchant_modeling',
         schedule_interval=None,
         default_args=default_args,
         description='Transform merchant data to silver layer (HUB and SAT)',
         tags=['silver', 'merchant']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    transform_merchant = PythonOperator(
        task_id='process_merchant_data',
        python_callable=process_merchant_to_silver,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    register_merchant = PythonOperator(
        task_id='register_merchant_views',
        python_callable=register_merchant_tables
    )

    transform_merchant >> register_merchant
