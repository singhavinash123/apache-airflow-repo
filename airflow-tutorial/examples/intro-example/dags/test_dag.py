import datetime
from datetime import timedelta


from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

dag = DAG(
    dag_id='airflow_slack_example',
    start_date=datetime.datetime.now(),
    schedule_interval='*/1 * * * *',
    default_args=default_args,
)

test = BashOperator(
    task_id='test',
    bash_command='echo "one minute test"',
    dag=dag,
)