from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from mylibs.databricks_utils import get_databricks_connection
from mylibs.databricks_utils import run_databricks_job  

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('deploy_fraud_model_dynamic',
         default_args=default_args,
         schedule_interval=None,
         description='Deploy fraud_logreg_job.py using dynamic secrets',
         tags=['deploy', 'databricks']) as dag:

    run_job = PythonOperator(
        task_id='submit_job',
        python_callable=run_databricks_job
    )

    run_job
