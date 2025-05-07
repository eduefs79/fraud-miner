from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from databricks import sql
from datetime import datetime
import pandas as pd
import hashlib
from io import BytesIO

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

def register_transaction_tables():
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
    Create or replace view fraud_miner.silver.Customer_history as
    select rsc.HK_CUSTOMER ,
      rsc.name,
      rsc.email,
      rsc.address,
      rsc.birth_date,
      rsc.updated_at as customer_updated_at
    from fraud_miner.silver.rdv_sat_customer rsc inner join fraud_miner.silver.rdv_hub_customer rhc on rsc.HK_CUSTOMER = rhc.HK_CUSTOMER 
    """)

    # Views
    cursor.execute("""
    Create or Replace view fraud_miner.silver.Credit_card_history as

    select rsc.HK_CREDIT_CARD,
       rsc.card_number,
       rsc.expiry_date,
       rsc.card_provider,
       rsc.cvv,
       rsc.issued_date,
       rsc.last_update,
       rsc.credit_limit
    from fraud_miner.silver.rdv_sat_credit_card rsc
    """)

    cursor.execute("""
    Create or replace view fraud_miner.silver.Merchant_history as

    select rsm.HK_MERCHANT,
      rsm.address,
      rsm.category,
      rsm.city,
      rsm.country,
      rsm.created_at as merchant_created_at,
      rsm.merchant_name as Merchant_Name
    from fraud_miner.silver.rdv_sat_merchant rsm
    """)

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.fraud_flat_view AS

    select rst.HK_LINK_TRANSACTION,
      rst.timestamp as Transaction_Date,
      rst.amount as Transaction_Amount,
      rst.currency as Transaction_Currency,
      rst.ip_address as Transaction_IP,
      rst.is_fraud as Transaction_Fraud,
      cch.card_number as Card_Number,
      cch.expiry_date as Card_Expiry_Date,
      cch.card_provider as Card_Provider,
      cch.issued_date as Card_Issued_Date,
      cch.last_update as Card_Last_Update,
      cch.credit_limit as Card_Credit_Limit,
      ch.name as Customer_Name,
      ch.email as Customer_Email,
      ch.address as Customer_Address,
      ch.birth_date as Customer_Birth_Date,
      ch.customer_updated_at as Customer_Last_Update,
      mch.address as Merchant_Address,
      mch.category as Merchant_Category,
      mch.city as Merchant_City,
      mch.country as Merchant_Country,
      mch.merchant_created_at as Merchant_Created_At,
      mch.Merchant_Name as Merchant_Name
      
    from fraud_miner.silver.rdv_sat_transaction rst inner join fraud_miner.silver.rdv_link_transaction rlt on rst.HK_LINK_TRANSACTION = rlt.HK_LINK_TRANSACTION
        inner join fraud_miner.silver.Credit_card_history cch on rlt.HK_CREDIT_CARD = cch.HK_CREDIT_CARD
        inner join fraud_miner.silver.rdv_link_credit_card rlc on cch.HK_CREDIT_CARD = rlc.HK_CREDIT_CARD
        inner join fraud_miner.silver.Customer_history ch on rlc.HK_CUSTOMER = ch.HK_CUSTOMER
        inner join fraud_miner.silver.Merchant_history mch on rlt.HK_MERCHANT = mch.HK_MERCHANT""")
    

    cursor.close()
    connection.close()
    print("✅ Registered BDV views for transactions")

# ------------------ DAG Definition ------------------
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('bdv_modeling',
         schedule_interval=None,
         default_args=default_args,
         description='Transform transaction data to silver layer (LINK and SAT)',
         tags=['silver', 'transaction']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    register_transaction_views = PythonOperator(
        task_id='register_transaction_views',
        python_callable=register_transaction_tables
    )

    register_transaction_views
