from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('generate_fake_data',
         start_date=datetime(2025, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    task_customers = BashOperator(
        task_id='generate_customers',
        bash_command='python3 /opt/airflow/dags/scripts/customers.py'
    )

    task_credit_cards = BashOperator(
        task_id='generate_credit_cards',
        bash_command='python3 /opt/airflow/dags/scripts/credit_cards.py'
    )

    task_merchants = BashOperator(
        task_id='generate_merchants',
        bash_command='python3 /opt/airflow/dags/scripts/merchants.py'
    )

    task_transactions = BashOperator(
        task_id='generate_transactions',
        bash_command='python3 /opt/airflow/dags/scripts/transactions.py'
    )

    task_customers >> task_credit_cards >> task_merchants >> task_transactions
