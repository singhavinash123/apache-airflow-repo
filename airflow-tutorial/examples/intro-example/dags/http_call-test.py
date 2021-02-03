import json
import datetime
from datetime import timedelta


from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack-notification'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack-notification',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)

def callable_func(context):
    print(context)
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url
    log_filepath = context.get('task_instance').log_filepath
    exec_date = context.get('execution_date')

    email_title = """
    Airflow Alert-- Airflow Task {task_id} is Success
    """.format(task_id = task_id)

    email_body = """
    <table class="reportWrapperTable" cellspacing="4" cellpadding="2" width="100%" rules="rows" style="border-collapse:collapse;color:#1f2240;background-color:#ffffff">
    <caption style="background-color:#ffffff;color:#1f2240;margin-bottom:.5em;font-size:18pt;width:100%;border:0">Airflow Alert</caption>
    <thead style="100%;color:#ffffff;background-color:#1f2240;font-weight:bold">
    <tr>
    <th scope="col" style="background-color:#1f2240">Name</th>
    <th scope="col" style="background-color:#1f2240">Status</th>
    </tr>
    </thead>
    <tbody>

    <tr>
    <td>The Task: </td>
    <td>{task_id}</td>
    </tr>
    
    <tr>
    <td>The Dag: </td>
    <td>{dag_id}</td>
    </tr>

    <tr>
    <td>Execution Time: </td>
    <td>{exec_date}</td>
    </tr>

    <tr>
    <td>Status: </td>
    <td> <span style="font-size: 20px; color: #008000;">
    Success.
    </span>
    </td>
    </tr>

    <tr>
    <td>Log Url: </td>
    <td><a href="{log_url}">Link</a></td>
    </tr>
    </tbody></table>
    """.format(task_id = task_id, dag_id = dag_id,log_url=log_url,exec_date=exec_date)
    to = "avinash.kachhwaha@oracle.com"
    send_email(to, email_title, email_body)

def error_callable_func(context):
    print(context)
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url

    email_title = "Airflow Alert-- Airflow Task {task_id} Failed".format(task_id = task_id)
    email_body = """
    The Task: {task_id} <br>
    The Dag: {dag_id} <br>
    Status: 
    <span style="font-size: 20px; color: #FF0000;">
    Failed.
    </span>
    <br>Log Url: <a href="{log_url}">Link</a><br>
    """.format(task_id = task_id, dag_id = dag_id,log_url=log_url)
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
    'on_failure_callback' : task_fail_slack_alert
    # 'on_failure_callback' : error_callable_func

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
