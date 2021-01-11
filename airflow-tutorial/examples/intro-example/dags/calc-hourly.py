import datetime
from datetime import timedelta

import airflow
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



def addition():
    logging.info(f"2 + 2 = {2+2}")
def subtraction():
    logging.info(f"6 -2 = {6-2}")
def division():
    logging.info(f"10 / 2 = {int(10/2)}")

dag = DAG(
    "lesson1.calc-hourly",
    # it will run for every hour..
    schedule_interval='@hourly',
    # the same time but yesterday...
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

addition_task = PythonOperator(
    task_id="addition",
    python_callable=addition,
    dag=dag
)

subtraction_task = PythonOperator(
    task_id="subtraction",
    python_callable=subtraction,
    dag=dag
)