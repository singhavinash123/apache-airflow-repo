import json
import datetime
from datetime import timedelta


from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_http_operator_test',
    default_args=default_args,
    tags=['example'],
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
    )

dag.doc_md = __doc__

def transform_json(**kwargs):
    ti = kwargs['ti']
    pulled_value_1 = ti.xcom_pull(key=None, task_ids='get_data')
    import_body= json.loads(pulled_value_1)
    print(import_body)
   


    # transform the json here and save the content to a file

def save_emp_json(**kwargs):
    ti = kwargs['ti']
    employee = ti.xcom_pull(key=None, task_ids='save_employee')
    import_body= json.loads(employee)
    print(import_body)
    print(import_body["id"])
    print(import_body["name"])
    id =   import_body["id"]
    print(id)
    empid = id
    print(empid)

    Variable.set("id", empid)
    print(Variable.get("id"))


    # transform the json here and save the content to a file

task_save_employee = SimpleHttpOperator(
    task_id='save_employee',
    http_conn_id='atlassian_marketplace',
    # endpoint='/rest/2',
    endpoint='/save',
    method="POST",
    data=json.dumps({"name": "avinash singh"}),
    # data="{\"name\" : \"Avinash Singh\"}",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)


task_get_all_employee = SimpleHttpOperator(
    task_id='get_all_employee',
    http_conn_id='atlassian_marketplace',
    # endpoint='/rest/2',
    endpoint='/get-all',
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)


task_get_byid_employee = SimpleHttpOperator(
    task_id='get-by-id-employee',
    http_conn_id='atlassian_marketplace',
    # endpoint='/rest/2',
    endpoint="/get-by-id/{empId}/employee".format(empId =  Variable.get("id")),
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)

# [END howto_operator_http_task_del_op]
# [START howto_operator_http_http_sensor_check]
task_http_sensor_check = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='atlassian_marketplace',
    endpoint='/',
    request_params={},
    # response_check=lambda response: "httpbin" in response.text,
    poke_interval=5,
    dag=dag,
)



# Task 3: Save JSON data locally
save_and_transform = PythonOperator(
    task_id="save_and_transform", 
    python_callable=transform_json,
    provide_context=True,
)

save_employee = PythonOperator(
    task_id="save_employee_transform", 
    python_callable=save_emp_json,
    provide_context=True,
)

task_http_sensor_check >> task_save_employee >> task_get_all_employee >> task_get_byid_employee >> save_and_transform
task_save_employee >> save_employee