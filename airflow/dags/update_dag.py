import sys

sys.path.append('/opt/airflow/src')

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from common.scraping_from_realtby import extract
from common.spark_delta import update_db


default_args = {
    'owner': 'default_user',
    'start_date': days_ago(2),
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
}

with DAG('Update_DB',
        default_args=default_args,
        schedule_interval=timedelta(hours=6),
        ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        provide_context=True,
        dag=dag)

    transform_update = PythonOperator(
        task_id='transform_update',
        python_callable=update_db,
        provide_context=True,
        dag=dag)

    extract_data >> transform_update
