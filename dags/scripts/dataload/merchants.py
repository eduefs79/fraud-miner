from datetime import timedelta
import pandas as pd
import random
import uuid
from faker import Faker 
import os

fake = Faker()

merchants = []
merchant_ids = []

for _ in range(5000):
    merchant_id = str(uuid.uuid4())
    merchant_ids.append(merchant_id)
    
    created_at = fake.date_time_between(start_date='-10y', end_date='-6m')
    
    updated_at = created_at
    
    merchants.append({
        'merchant_id': merchant_id,
        'merchant_name': fake.company(),
        'category': fake.bs(),
        'address': fake.address().replace('\n', ', '),
        'city': fake.city(),
        'country': fake.country(),
        'created_at': created_at,
        'updated_at': updated_at
    })

df_merchants = pd.DataFrame(merchants)
output_dir = f'/data/merchant/'
os.makedirs(output_dir, exist_ok=True)
df_merchants.to_parquet(f'{output_dir}/merchants.parquet', index=False)
