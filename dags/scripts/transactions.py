import numpy as np
from datetime import datetime
import pandas as pd
import random
import uuid
from faker import Faker
import os

fake = Faker()

df_cards = pd.read_parquet('/data/credit_cards.parquet')
df_merchant = pd.read_parquet('/data/merchants.parquet')
merchant_ids = df_merchant['merchant_id'].tolist()

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 5, 31)

transactions_by_day = {}

for _ in range(3000000):
    card = df_cards.sample(1).iloc[0]
    merchant_id = random.choice(merchant_ids)
    transaction_date = fake.date_time_between(start_date=start_date, end_date=end_date)
    date_str = transaction_date.strftime('%Y-%m-%d')

    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'card_id': card['card_id'],
        'customer_id': card['customer_id'],
        'merchant_id': merchant_id,
        'amount': round(random.uniform(1.00, 1000.00), 2),
        'currency': 'USD',
        'timestamp': transaction_date.isoformat(),
        'ip_address': fake.ipv4_public(),
        'is_fraud': np.random.choice([0, 1], p=[0.985, 0.015])
    }

    # Add transaction to the corresponding date
    transactions_by_day.setdefault(date_str, []).append(transaction)

# Save each day's transactions into a separate Parquet file
for date_str, transactions in transactions_by_day.items():
    df_day = pd.DataFrame(transactions)
    output_dir = f'/data/{date_str}'
    os.makedirs(output_dir, exist_ok=True)
    df_day.to_parquet(f'{output_dir}/fraud_data.parquet', index=False)
