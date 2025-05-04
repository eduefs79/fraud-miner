from datetime import datetime, timedelta
import pandas as pd
import random
import uuid
from faker import Faker
import os

fake = Faker()

df_customers = pd.read_parquet('/data/customer/customers.parquet')

cards = []
for customer in df_customers['customer_id']:
    for _ in range(random.randint(1, 3)):
        issued_date = fake.date_between(start_date='-3y', end_date='-1d')
        last_update = issued_date + timedelta(days=random.randint(30, 900))
        credit_limit = round(random.uniform(500, 20000), 2)

        cards.append({
            'card_id': str(uuid.uuid4()),
            'customer_id': customer,
            'card_number': fake.credit_card_number(),
            'expiry_date': fake.credit_card_expire(),
            'card_provider': fake.credit_card_provider(),
            'cvv': fake.credit_card_security_code(),
            'issued_date': issued_date,
            'last_update': min(last_update, datetime.today().date()), 
            'credit_limit': credit_limit
        })

df_cards = pd.DataFrame(cards)
output_dir = f'/data/creditcard/'
os.makedirs(output_dir, exist_ok=True)
df_cards.to_parquet(f'{output_dir}/credit_cards.parquet', index=False)
