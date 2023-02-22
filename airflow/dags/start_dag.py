import sys

sys.path.append('/opt/airflow/src')

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from common.scraping_from_realtby import extract
from common.spark_delta import upload_to_db
from common import geo_info

default_args = {
    'owner': 'default_user',
    'start_date': days_ago(2),
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
}


with DAG('Create_DB', 
        default_args=default_args,
        schedule_interval=None
        ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        provide_context=True,
        dag=dag)

    transform_upload = PythonOperator(
        task_id='transform_upload',
        python_callable=upload_to_db,
        provide_context=True,
        dag=dag)

    extract_data >> transform_upload
