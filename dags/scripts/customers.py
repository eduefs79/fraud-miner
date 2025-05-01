from faker import Faker
import pandas as pd
import uuid

fake = Faker()
Faker.seed(42)

customers = []
for _ in range(20000):  
    customer_id = str(uuid.uuid4())
    customers.append({
        'customer_id': customer_id,
        'name': fake.name(),
        'email': fake.email(),
        'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        'address': fake.address().replace('\n', ', '),
        'created_at': fake.date_time_between(start_date='-3y', end_date='-6m')
    })

df_customers = pd.DataFrame(customers)
df_customers.to_parquet('/data/customers.parquet', index=False, engine='pyarrow')
