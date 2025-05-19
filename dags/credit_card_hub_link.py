from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from databricks import sql
from datetime import datetime
import pandas as pd
import hashlib
from io import BytesIO
from mylibs.databricks_utils import get_databricks_connection

# --------- Helpers ---------
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

# --------- Transform to Silver ---------
def process_credit_card_to_silver(aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'
    key = 'bronze/fraud_raw/creditcard/credit_cards.parquet'
    buffer = BytesIO(s3.get_key(key, bucket).get()['Body'].read())
    df = pd.read_parquet(buffer)

    df['HK_CREDIT_CARD'] = df['card_id'].apply(lambda x: hash_key(x))
    df['HK_CUSTOMER'] = df['customer_id'].apply(lambda x: hash_key(x))
    load_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df['LOAD_DATE'] = load_date
    df['RECORD_SOURCE'] = 'bronze.credit_card'

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('datetime64[us]')

    df_hub = df[['HK_CREDIT_CARD','LOAD_DATE', 'RECORD_SOURCE']].drop_duplicates()
    df_sat = df[['HK_CREDIT_CARD', 'card_id','customer_id', 'card_number', 'expiry_date', 'card_provider',
                 'cvv', 'issued_date', 'last_update', 'credit_limit', 'LOAD_DATE', 'RECORD_SOURCE']]

    df_link = df[['HK_CREDIT_CARD', 'HK_CUSTOMER']].drop_duplicates()

    upload_df_to_s3(df_hub, bucket, 'silver/hub/HUB_CREDIT_CARD.parquet', aws_conn_id)
    upload_df_to_s3(df_sat, bucket, 'silver/sat/SAT_CREDIT_CARD.parquet', aws_conn_id)
    upload_df_to_s3(df_link, bucket, 'silver/link/link_CREDIT_CARD.parquet', aws_conn_id)

# --------- Register Volumes & Views ---------
def register_credit_card_tables():

    connection = get_databricks_connection()

    cursor = connection.cursor()

    cursor.execute("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraud_miner.silver.rdv_hub
    LOCATION 's3://fraud-miner/silver/hub/'
    COMMENT 'silver layer for HUB'
    """)

    cursor.execute("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraud_miner.silver.rdv_sat
    LOCATION 's3://fraud-miner/silver/sat/'
    COMMENT 'silver layer for SAT'
    """)

    cursor.execute("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraud_miner.silver.rdv_link
    LOCATION 's3://fraud-miner/silver/link/'
    COMMENT 'silver layer for link'
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_hub_credit_card AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_hub/HUB_CREDIT_CARD.parquet`
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_sat_credit_card AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_sat/SAT_CREDIT_CARD.parquet`
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_link_credit_card AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_link/link_CREDIT_CARD.parquet`
    """)

    cursor.close()
    connection.close()
    print("✅ Registered HUB and SAT credit card views")

# --------- DAG Definition ---------
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('silver_credit_card_modeling',
         schedule_interval=None,
         default_args=default_args,
         description='Transform credit card data to silver layer (HUB and SAT)',
         tags=['silver', 'credit_card']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    transform_credit_card = PythonOperator(
        task_id='process_credit_card_data',
        python_callable=process_credit_card_to_silver,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    register_credit_card = PythonOperator(
        task_id='register_credit_card_views',
        python_callable=register_credit_card_tables
    )

    transform_credit_card >> register_credit_card
