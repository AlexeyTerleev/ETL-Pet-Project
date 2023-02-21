import sys

sys.path.append('/opt/airflow/src')

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import common.scraping_from_realtby

dag = DAG('Update_DB',
          schedule_interval=timedelta(hours=6),
          start_date=datetime(2020, 7, 8, 0))


def workflow(**context):
    print(context)


test = PythonOperator(
    task_id='test',
    python_callable=workflow,
    provide_context=True,
    dag=dag)

test1 = PythonOperator(
    task_id='test1',
    python_callable=workflow,
    provide_context=True,
    dag=dag)


test >> test1