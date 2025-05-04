import numpy as np
import pandas as pd
import random
import uuid
from faker import Faker
import os
import argparse
from datetime import datetime

fake = Faker()

# Load once globally
df_cards = pd.read_parquet('/data/creditcard/credit_cards.parquet')
df_merchant = pd.read_parquet('/data/merchant/merchants.parquet')
merchant_ids = df_merchant['merchant_id'].tolist()

def generate_transactions(start_date, end_date, n_transactions=300000, output_base='/data'):
    """
    Generates fake credit card transactions between start_date and end_date.

    Args:
        start_date (datetime): Start of transaction window.
        end_date (datetime): End of transaction window.
        n_transactions (int): Total number of transactions to generate.
        output_base (str): Base output folder.
    """
    transactions_by_day = {}

    for _ in range(n_transactions):
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

        transactions_by_day.setdefault(date_str, []).append(transaction)

    # Write to daily Parquet files
    for date_str, transactions in transactions_by_day.items():
        df_day = pd.DataFrame(transactions)
        output_dir = os.path.join(output_base, date_str)
        os.makedirs(output_dir, exist_ok=True)
        df_day.to_parquet(os.path.join(output_dir, 'fraud_data.parquet'), index=False)

    print(f"✅ Generated {n_transactions} transactions between {start_date.date()} and {end_date.date()}.")


# 🧠 Now parse args and run the function
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--n', type=int, default=500000, help='Number of transactions')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')

    generate_transactions(start_date, end_date, n_transactions=args.n)
