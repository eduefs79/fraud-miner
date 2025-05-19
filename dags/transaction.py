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
def process_transaction_to_silver(aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'
    base_prefix = 'bronze/fraud_raw/transaction/'
    keys = s3.list_keys(bucket, prefix=base_prefix)

    all_records = []

    for key in keys:
        if key.endswith('.parquet'):
            buffer = BytesIO(s3.get_key(key, bucket).get()['Body'].read())
            df = pd.read_parquet(buffer)
            all_records.append(df)

    df_all = pd.concat(all_records, ignore_index=True)

    # Parse timestamp as datetime
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
    df_all['HK_CREDIT_CARD'] = df_all['card_id'].apply(lambda x: hash_key(x))
    df_all['HK_MERCHANT'] = df_all['merchant_id'].apply(lambda x: hash_key(x))

    # Link key
    df_all['HK_LINK_TRANSACTION'] = df_all.apply(
        lambda row: hash_key(row['card_id'], row['merchant_id'], row['timestamp']), axis=1
    )
    load_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df_all['LOAD_DATE'] = load_date
    df_all['RECORD_SOURCE'] = 'bronze.transaction'

    # Ensure datetime consistency
    for col in df_all.columns:
        if pd.api.types.is_datetime64_any_dtype(df_all[col]):
            df_all[col] = df_all[col].astype('datetime64[us]')

    df_link = df_all[['HK_LINK_TRANSACTION', 'HK_CREDIT_CARD', 'HK_MERCHANT']].drop_duplicates()
    df_sat = df_all[['HK_LINK_TRANSACTION','timestamp', 'amount', 'currency', 'ip_address', 'is_fraud', 'LOAD_DATE', 'RECORD_SOURCE']]

    upload_df_to_s3(df_link, bucket, 'silver/link/LINK_TRANSACTION.parquet', aws_conn_id)
    upload_df_to_s3(df_sat, bucket, 'silver/sat/SAT_TRANSACTION.parquet', aws_conn_id)

# ------------------ Unity Catalog Registration ------------------
def register_transaction_tables():

    connection = get_databricks_connection()
    cursor = connection.cursor()

    cursor.execute("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraud_miner.silver.rdv_link
    LOCATION 's3://fraud-miner/silver/link/'
    COMMENT 'silver layer for Link'
    """)

    # Views
    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_link_transaction AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_link/LINK_TRANSACTION.parquet`
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_sat_transaction AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_sat/SAT_TRANSACTION.parquet`
    """)


    # cursor.execute("""
    # CREATE OR REPLACE VIEW fraud_miner.silver.fraud_flat_view AS
    # SELECT
    #   tlink.HK_LINK_TRANSACTION,
    #   tlink.card_id,
    #   csat.name AS customer_name,
    #   csat.email,
    #   csat.birth_date,
    #   csat.address AS customer_address,
    #   ccsat.card_number,
    #   ccsat.card_provider,
    #   ccsat.credit_limit,
    #   msat.merchant_name,
    #   msat.category AS merchant_category,
    #   msat.city AS merchant_city,
    #   msat.country AS merchant_country,
    #   tsat.amount,
    #   tsat.currency,
    #   tsat.ip_address,
    #   tlink.timestamp,
    #   tsat.is_fraud
    # FROM fraud_miner.silver.rdv_link_transaction tlink
    # LEFT JOIN fraud_miner.silver.rdv_sat_transaction tsat
    #   ON tlink.HK_LINK_TRANSACTION = tsat.HK_LINK_TRANSACTION
    # LEFT JOIN fraud_miner.silver.rdv_sat_credit_card ccsat
    #   ON tlink.card_id = ccsat.HK_CREDIT_CARD
    # LEFT JOIN fraud_miner.silver.rdv_sat_customer csat
    #   ON ccsat.customer_id = csat.HK_CUSTOMER
    # LEFT JOIN fraud_miner.silver.rdv_sat_merchant msat
    #   ON tlink.merchant_id = msat.HK_MERCHANT
    # """)

    cursor.close()
    connection.close()
    print("✅ Registered LINK, SAT and BDV views for transactions")

# ------------------ DAG Definition ------------------
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('silver_transaction_modeling',
         schedule_interval=None,
         default_args=default_args,
         description='Transform transaction data to silver layer (LINK and SAT)',
         tags=['silver', 'transaction']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    transform_transaction = PythonOperator(
        task_id='process_transaction_data',
        python_callable=process_transaction_to_silver,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    register_transaction = PythonOperator(
        task_id='register_transaction_views',
        python_callable=register_transaction_tables
    )

    transform_transaction >> register_transaction
