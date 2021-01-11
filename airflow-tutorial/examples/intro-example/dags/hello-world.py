import datetime
from datetime import timedelta

import airflow
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("Hello World")
    logging.info(f"6 -2 = {6-2}")


hello_dag = DAG(
        "lesson1.excercise1",
        start_date=datetime.datetime.now(),
        schedule_interval='@daily',
        
        description='A simple hello world DAG'

)

t1 = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=hello_dag
)