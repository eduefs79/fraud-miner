from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.helpers import chain
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import uuid
import random
from faker import Faker
from io import BytesIO

fake = Faker()

# Helper to upload DataFrame to S3
def upload_df_to_s3(df, bucket, key, aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.load_file_obj(buffer, key=key, bucket_name=bucket, replace=True)
    print(f"✅ Uploaded to s3://{bucket}/{key}")

# CUSTOMER
def generate_customers_to_s3(aws_conn_id):
    customers = []
    for _ in range(20000):
        created_at = fake.date_time_between(start_date='-3y', end_date='-6m')
        updated_at = created_at
        customers.append({
            'customer_id': str(uuid.uuid4()),
            'name': fake.name(),
            'email': fake.email(),
            'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            'address': fake.address().replace('\n', ', '),
            'created_at': created_at,
            'updated_at': updated_at
        })
    df = pd.DataFrame(customers)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('datetime64[ms]')
    upload_df_to_s3(df, 'fraud-miner', 'bronze/fraud_raw/customer/customers.parquet', aws_conn_id)

# MERCHANT
def generate_merchants_to_s3(aws_conn_id):
    merchants = []
    for _ in range(5000):
        created_at = fake.date_time_between(start_date='-10y', end_date='-6m')
        updated_at = created_at
        merchants.append({
            'merchant_id': str(uuid.uuid4()),
            'merchant_name': fake.company(),
            'category': fake.bs(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': fake.country(),
            'created_at': created_at,
            'updated_at': updated_at
        })
    df_merchants = pd.DataFrame(merchants)
    for col in df_merchants.columns:
        if pd.api.types.is_datetime64_any_dtype(df_merchants[col]):
            df_merchants[col] = df_merchants[col].astype('datetime64[ms]')
    upload_df_to_s3(df_merchants, 'fraud-miner', 'bronze/fraud_raw/merchant/merchants.parquet', aws_conn_id)

# CREDIT CARD
def generate_credit_cards_to_s3(aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'
    key = 'bronze/fraud_raw/customer/customers.parquet'
    buffer = BytesIO(s3.get_key(key, bucket).get()['Body'].read())
    df_customers = pd.read_parquet(buffer)

    cards = []
    for customer_id in df_customers['customer_id']:
        for _ in range(random.randint(1, 3)):
            issued_date = fake.date_between(start_date='-3y', end_date='-1d')
            last_update = issued_date + timedelta(days=random.randint(30, 900))
            cards.append({
                'card_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'card_number': fake.credit_card_number(),
                'expiry_date': fake.credit_card_expire(),
                'card_provider': fake.credit_card_provider(),
                'cvv': fake.credit_card_security_code(),
                'issued_date': issued_date,
                'last_update': min(last_update, datetime.today().date()),
                'credit_limit': round(random.uniform(500, 10000), 2)
            })

    df_cards = pd.DataFrame(cards)
    for col in df_cards.columns:
        if pd.api.types.is_datetime64_any_dtype(df_cards[col]):
            df_cards[col] = df_cards[col].astype('datetime64[ms]')
    upload_df_to_s3(df_cards, bucket, 'bronze/fraud_raw/creditcard/credit_cards.parquet', aws_conn_id)

# TRANSACTION
def generate_transactions_to_s3(start_date_str, end_date_str, n_transactions, aws_conn_id):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = 'fraud-miner'

    buffer_cards = BytesIO(s3.get_key('bronze/fraud_raw/creditcard/credit_cards.parquet', bucket).get()['Body'].read())
    df_cards = pd.read_parquet(buffer_cards)

    buffer_merchants = BytesIO(s3.get_key('bronze/fraud_raw/merchant/merchants.parquet', bucket).get()['Body'].read())
    df_merchant = pd.read_parquet(buffer_merchants)

    merchant_ids = df_merchant['merchant_id'].tolist()
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    transactions_by_day = {}

    for _ in range(n_transactions):
        card = df_cards.sample(1).iloc[0]
        merchant_id = random.choice(merchant_ids)
        transaction_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        date_str = transaction_date.strftime('%Y-%m-%d')
        transactions_by_day.setdefault(date_str, []).append({
            'transaction_id': str(uuid.uuid4()),
            'card_id': card['card_id'],
            'customer_id': card['customer_id'],
            'merchant_id': merchant_id,
            'amount': round(random.uniform(1.00, 1000.00), 2),
            'currency': 'USD',
            'timestamp': transaction_date,
            'ip_address': fake.ipv4_public(),
            'is_fraud': np.random.choice([0, 1], p=[0.985, 0.015])
        })

    for date_str, records in transactions_by_day.items():
        df_day = pd.DataFrame(records)
        for col in df_day.columns:
            if pd.api.types.is_datetime64_any_dtype(df_day[col]):
                df_day[col] = df_day[col].astype('datetime64[ms]')
        key = f"bronze/fraud_raw/transaction/{date_str}/fraud_data.parquet"
        upload_df_to_s3(df_day, bucket, key, aws_conn_id)

# DAG
default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('full_fraud_pipeline_to_s3',
         schedule_interval=None,
         default_args=default_args,
         description='Generate full fake fraud dataset and upload to S3',
         tags=['fraud', 's3', 'etl']) as dag:

    aws_conn = 'aws_s3_conn_fraud'

    t1 = PythonOperator(
        task_id='generate_customers',
        python_callable=generate_customers_to_s3,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    t2 = PythonOperator(
        task_id='generate_credit_cards',
        python_callable=generate_credit_cards_to_s3,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    t3 = PythonOperator(
        task_id='generate_merchants',
        python_callable=generate_merchants_to_s3,
        op_kwargs={'aws_conn_id': aws_conn}
    )

    t4a = PythonOperator(
        task_id='generate_transactions_jan',
        python_callable=generate_transactions_to_s3,
        op_kwargs={
            'start_date_str': '2025-01-01',
            'end_date_str': '2025-01-31',
            'n_transactions': 2000000,
            'aws_conn_id': aws_conn
        }
    )

    t4b = PythonOperator(
        task_id='generate_transactions_feb',
        python_callable=generate_transactions_to_s3,
        op_kwargs={
            'start_date_str': '2025-02-01',
            'end_date_str': '2025-02-28',
            'n_transactions': 2000000,
            'aws_conn_id': aws_conn
        }
    )

    t4c = PythonOperator(
        task_id='generate_transactions_mar',
        python_callable=generate_transactions_to_s3,
        op_kwargs={
            'start_date_str': '2025-03-01',
            'end_date_str': '2025-03-31',
            'n_transactions': 2000000,
            'aws_conn_id': aws_conn
        }
    )

    # Dependencies
    chain(t1,t2,t3,[t4a,t4b,t4c])
    #t1 >> t2
    #t1 >> t3
    #t2 >> [t4a, t4b, t4c]
    #t3 >> [t4a, t4b, t4c]
