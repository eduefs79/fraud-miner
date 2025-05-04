from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    'owner': 'eduardo',
    'start_date': datetime(2025, 1, 1),
}

with DAG('databricks_test_dag', default_args=default_args, schedule_interval=None) as dag:
    task = DatabricksSubmitRunOperator(
        task_id='run_notebook',
        databricks_conn_id='fraud_databricks',
        notebook_task={
            'notebook_path': '/Repos/your.email@databricks.com/fraud-miner/bronze/ingest'
        },
        existing_cluster_id='<your-cluster-id>'
    )
