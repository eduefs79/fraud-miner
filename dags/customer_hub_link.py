from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from io import BytesIO
import pandas as pd
import hashlib
from airflow.hooks.base import BaseHook
from databricks import sql
from airflow.hooks.base import BaseHook
from databricks import sql
from mylibs.databricks_utils import get_databricks_connection

def register_customer_tables():
    # conn = BaseHook.get_connection("fraud_databricks")
    # host = conn.host
    # token = conn.password
    # http_path = conn.extra_dejson.get("http_path")

    # print("🔍 Connecting to Databricks with:")
    # print(f"Host: {host}")
    # print(f"Token exists: {bool(token)}")
    # print(f"HTTP Path: {http_path}")

    # connection = sql.connect(
    #     server_hostname=host,
    #     http_path=http_path,
    #     access_token=token
    # )
    connection = get_databricks_connection()

    cursor = connection.cursor()

    # Create external volumes
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

    # Create views
    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_hub_customer AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_hub/HUB_CUSTOMER.parquet`
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_sat_customer AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_sat/SAT_CUSTOMER.parquet`
    """)

    cursor.close()
    connection.close()
    print("✅ Registered external volumes and views in Unity Catalog")






def hash_key(*values):
    concat = '||'.join(str(v) for v in values)
    return hashlib.sha256(concat.encode('utf-8')).hexdigest()

def process_customer_data_to_silver(aws_conn_id):
    # Load bronze data from S3
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'
    bronze_key = 'bronze/fraud_raw/customer/customers.parquet'

    buffer = BytesIO(s3.get_key(bronze_key, bucket).get()['Body'].read())
    df = pd.read_parquet(buffer)

    # Create HK_CUSTOMER
    df['HK_CUSTOMER'] = df['customer_id'].apply(lambda x: hash_key(x))
    load_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df['LOAD_DATE'] = load_date
    #df['LOAD_DATE'] = pd.to_datetime(df['LOAD_DATE']).astype('datetime64[ms]')
    df['RECORD_SOURCE'] = 'bronze.customer'
    df['created_at'] = pd.to_datetime(df['created_at']).astype('datetime64[us]')
    df['updated_at'] = pd.to_datetime(df['updated_at']).astype('datetime64[us]')

    # Hub
    df_hub = df[['HK_CUSTOMER', 'customer_id', 'LOAD_DATE', 'RECORD_SOURCE']].drop_duplicates()

    # Satellite
    df_sat = df[['HK_CUSTOMER', 'name', 'email', 'address', 'birth_date', 'created_at', 'updated_at', 'LOAD_DATE', 'RECORD_SOURCE']]

    # Save to Silver S3 path
    for name, data in [('hub/HUB_CUSTOMER.parquet', df_hub), ('sat/SAT_CUSTOMER.parquet', df_sat)]:
        buffer_out = BytesIO()
        data.to_parquet(buffer_out, index=False, engine='pyarrow')
        buffer_out.seek(0)
        silver_key = f'silver/{name}'
        s3.load_file_obj(buffer_out, key=silver_key, bucket_name=bucket, replace=True)
        print(f"✅ Uploaded {silver_key}")

# DAG definition
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('silver_customer_modeling',
         schedule_interval=None,
         default_args=default_args,
         description='Transform customer data from bronze to silver (Hub & Satellite)',
         tags=['silver', 'data-vault']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    transform_customer = PythonOperator(
        task_id='process_customer_data',
        python_callable=process_customer_data_to_silver,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    register_tables = PythonOperator(
        task_id='register_customer_tables',
        python_callable=register_customer_tables
    )

transform_customer >> register_tables

