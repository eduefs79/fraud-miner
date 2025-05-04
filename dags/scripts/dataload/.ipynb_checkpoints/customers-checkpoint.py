from faker import Faker
import pandas as pd
import uuid
from datetime import timedelta
import os

fake = Faker()
Faker.seed(42)

customers = []
for _ in range(20000):  
    customer_id = str(uuid.uuid4())
    created_at = fake.date_time_between(start_date='-3y', end_date='-6m')
    
    # In this version, assume no changes occurred (yet), so updated_at == created_at
    updated_at = created_at  

    customers.append({
        'customer_id': customer_id,
        'name': fake.name(),
        'email': fake.email(),
        'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        'address': fake.address().replace('\n', ', '),
        'created_at': created_at,
        'updated_at': updated_at
    })

df_customers = pd.DataFrame(customers)
output_dir = f'/data/customer/'
os.makedirs(output_dir, exist_ok=True)
df_customers.to_parquet(f'{output_dir}/customers.parquet', index=False, engine='pyarrow')
