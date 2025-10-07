from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')
from assets.bronze import run_bronze
from assets.silver import run_load
from assets.gold import run_gold

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def bronze_task(**context):
    execution_date = context['execution_date'].date()
    date_from = execution_date.strftime("%Y-%m-%d")
    date_to = date_from
    path = run_bronze(date_from, date_to)
    context['ti'].xcom_push(key='minio_path', value=path)
    return path

with DAG(
    dag_id='football_pipeline',
    default_args=default_args,
    description='Fetch football matches to MinIO (bronze) and load to Postgres (silver)',
    start_date=datetime(2025, 9, 15),
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:

    bronze_operator = PythonOperator(
        task_id='bronze_fetch_to_minio',
        python_callable=bronze_task,
    )

    silver_task = PythonOperator(
        task_id='silver_load_to_postgres',
        python_callable=run_load,
        op_kwargs={'bucket': 'football', 'prefix': 'bronze/matches'}
    )

    gold_task = PythonOperator(
        task_id='gold_load_to_postgres',
        python_callable=run_gold,
    )

    bronze_operator >> silver_task >> gold_task
