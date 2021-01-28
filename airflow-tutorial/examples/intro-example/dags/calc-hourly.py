import datetime
from datetime import timedelta

import time
import airflow
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



def addition(random_base):
    logging.info(f"10 + 2 = {10+2}")
    time.sleep(random_base)

def subtraction():
    logging.info(f"10 -2 = {10-2}")

def division():
    logging.info(f"10 / 2 = {int(10/2)}")

def multiply():
    logging.info(f"10 * 2 = {int(10*2)}")

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
   op_kwargs={'random_base': 120},
    dag=dag
)

subtraction_task = PythonOperator(
    task_id="subtraction",
    python_callable=subtraction,
    dag=dag
)

division_task = PythonOperator(
    task_id="division",
    python_callable=division,
    dag=dag
)

multiply_task = PythonOperator(
    task_id="multiply",
    python_callable=multiply,
    dag=dag
)