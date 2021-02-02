import json
import datetime
from datetime import timedelta


from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def callable_func(context):
    print(context)
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id

    email_title = "Airflow Task {task_id} Success".format(task_id = task_id)
    email_body = "{task_id} in {dag_id} Success.".format(task_id = task_id, dag_id = dag_id)
    to = "avinash.kachhwaha@oracle.com"
    send_email(to, email_title, email_body)

def error_callable_func(context):
    print(context)
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id

    email_title = "Airflow Task {task_id} Failed".format(task_id = task_id)
    email_body = "{task_id} in {dag_id} Failed.".format(task_id = task_id, dag_id = dag_id)
    to = "avinash.kachhwaha@oracle.com"
    send_email(to, email_title, email_body)
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['avinash.kachhwaha@oracle.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'on_success_callback': callable_func,
    'on_failure_callback' : error_callable_func
}

dag = DAG(
    'example_http_operator_test',
    default_args=default_args,
    tags=['example'],
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
    )

dag.doc_md = __doc__

# def transform_json(**kwargs):
#     ti = kwargs['ti']
#     pulled_value_1 = ti.xcom_pull(key=None, task_ids='save_employee')
#     import_body= json.loads(pulled_value_1)
#     print(import_body)
   

from airflow.utils.email import send_email

def notify_email(context, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**context)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**context)

    send_email('you_email@address.com', title, body)

# transform the json here and save the content to a file
def save_emp_json(**kwargs):
    ti = kwargs['ti']
    employee = ti.xcom_pull(key=None, task_ids='save_employee')
    import_body= json.loads(employee)
    print(import_body)
    id =   import_body["id"]
    print(id)
    empid = id
    Variable.set("id", empid)

 # transform the json here and save the content to a file
task_save_employee = SimpleHttpOperator(
    task_id='save_employee',
    http_conn_id='rest-connection',
    endpoint='/save',
    method="POST",
    data=json.dumps({"name": "avinash singh"}),
    headers={"Content-Type": "application/json"},
    # response_check=lambda response: response.json()['json']['name'] == "avinash singh",
    xcom_push=True,
    dag=dag,
)


task_get_all_employee = SimpleHttpOperator(
    task_id='get_all_employee',
    http_conn_id='rest-connection',
    endpoint='/get-all',
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)


task_get_byid_employee = SimpleHttpOperator(
    task_id='get-by-id-employee',
    http_conn_id='rest-connection',
    endpoint="/get-by-id/{empId}/employee".format(empId = Variable.get("id")),
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)

# task_update_employee = SimpleHttpOperator(
#     task_id='update-employee',
#     http_conn_id='rest-connection',
#     endpoint="/update?id={empId}".format(empId = Variable.get("id")),
#     method="PUT",
#     headers={"Content-Type": "application/json"},
#     response_filter=lambda response: response.json(),
#     xcom_push=True,
#     dag=dag,
# )

# [END howto_operator_http_task_del_op]
# [START howto_operator_http_http_sensor_check]
task_http_sensor_check = HttpSensor(
    task_id='api_health_check',
    http_conn_id='rest-connection',
    endpoint='/',
    request_params={},
    # response_check=lambda response: "httpbin" in response.text,
    poke_interval=5,
    # on_failure_callback=notify_email,
    dag=dag,
)

# Task 3: Save JSON data locally
# save_and_transform = PythonOperator(
#     task_id="save_and_transform", 
#     python_callable=transform_json,
#     provide_context=True,
# )

save_employee = PythonOperator(
    task_id="save_employee_transform", 
    python_callable=save_emp_json,
    provide_context=True
)

task_http_sensor_check >> task_save_employee >> save_employee >> task_get_byid_employee
save_employee >> task_get_all_employee
# save_employee >> task_update_employee >> task_get_byid_employee
