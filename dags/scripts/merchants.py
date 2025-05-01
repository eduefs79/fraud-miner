from datetime import timedelta
import pandas as pd
import random
import uuid
from faker import Faker 

fake = Faker()  


merchants = []
merchant_ids = []
for _ in range(5000):
    merchant_id = str(uuid.uuid4())
    merchant_ids.append(merchant_id)
    merchants.append({
        'merchant_id': merchant_id,
        'merchant_name': fake.company(),
        'category': fake.bs(),
        'address': fake.address().replace('\n', ', '),
        'city': fake.city(),
        'country': fake.country()
    })

df_merchants = pd.DataFrame(merchants)
df_merchants.to_parquet('/data/merchants.parquet', index=False)
