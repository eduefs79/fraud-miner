from datetime import timedelta
import pandas as pd
import random
import uuid
from faker import Faker 

fake = Faker()  # ✅ this creates the instance

df_customers = pd.read_parquet('/data/customers.parquet')

cards = []
for customer in df_customers['customer_id']:
    for _ in range(random.randint(1, 3)):
        issued_date = fake.date_between(start_date='-3y', end_date='today')
        cards.append({
            'card_id': str(uuid.uuid4()),
            'customer_id': customer,
            'card_number': fake.credit_card_number(),
            'expiry_date': fake.credit_card_expire(),
            'card_provider': fake.credit_card_provider(),
            'cvv': fake.credit_card_security_code(),
            'issued_date': issued_date
        })

df_cards = pd.DataFrame(cards)
df_cards.to_parquet('/data/credit_cards.parquet', index=False)
