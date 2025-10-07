from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from assets.silver import run_load
from assets.gold import run_gold

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='football_pipeline_silver_gold',
    default_args=default_args,
    description='Load silver data and compute gold standings',
    start_date=datetime(2025, 9, 15),
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:

    silver_task = PythonOperator(
        task_id='silver_load_to_postgres',
        python_callable=run_load,
        op_kwargs={'bucket': 'football', 'prefix': 'bronze/matches'}
    )

    gold_task = PythonOperator(
        task_id='gold_compute_standings',
        python_callable=run_gold,
    )

    silver_task >> gold_task
