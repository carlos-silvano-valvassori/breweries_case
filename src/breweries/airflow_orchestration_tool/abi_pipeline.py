from datetime import datetime, timedelta
import os
from airflow import DAG
import boto3
from airflow.utils.email import send_email
import json
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator


airflow_home = os.environ["AIRFLOW_HOME"] + "/dags/abi_pipeline"
CLUSTER_JSON_PATH = os.environ["AIRFLOW_HOME"] + "/dags/cluster_config/abi_cluster.json"
EMR_CLUSTER_CONFIG = os.environ["AIRFLOW_HOME"] + "/dags/configs/abi_cluster_aws_config.json"

# Email list when error occurs in pipeline
EMAIL_LIST = [
    "csv00031@terra.com.br",
    "carlos.valvassori@terra.com.br",
]

DEFAULT_ARGS = {
    "owner": "abiinbev",
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2025, 5, 30),
}


# Scheduled for daily execution at 08:00 AM
dag = DAG(
    "abi_pipeline",
    description="ABIInbev services",
    schedule_interval="0 8 * * *",
    max_active_runs=1,
    catchup=False,
    default_args=DEFAULT_ARGS,
)


def iterate(obj, conf):
    if isinstance(obj, dict):
        for x in obj:
            if isinstance(obj[x], dict) or isinstance(obj[x], list):
                obj[x] = iterate(obj[x], conf)
            elif isinstance(obj[x], str) and obj[x][:2] == "<<" and obj[x][-2:] == ">>":
                obj[x] = conf[obj[x][2:-2]]
    elif isinstance(obj, list):
        index=0
        while index < len (obj):
            if isinstance(obj[index], dict) or isinstance(obj[index], list):
                obj[index] = iterate(obj[index], conf)
            elif isinstance(obj[index], str) and obj[index][:2] == "<<" and obj[index][-2:] == ">>":
                obj[index] = conf[obj[x][2:-2]]
            index=index+1
    return obj


# EMR_CLUSTER_CONFIG
def cover_open(obj):
    return iterate(obj, json.load(open(EMR_CLUSTER_CONFIG)))


def send_alert(message):
    send_email(DEFAULT_ARGS["email"], "Alert", message)


def send_sns_alert(context):
    task_instance = context.get("task_instance")
    task_details = task_instance.task
    execution_date = task_instance.execution_date
    task_log_url = task_instance.log_url

    sns_topic_arn = "arn:aws:sns:us-west-2:1234567890:my-sns-topic"

    sns_client = boto3.client('sns')

    state_change_time = execution_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    message = {
        "AlarmName": "Airflow Task {} has failed".format(task_details.task_id),
        "NewStateValue": "ALARM",
        "NewStateReason": "failure",
        "StateChangeTime": state_change_time,
        "AlarmDescription": "Task failed on {}. Log URL: {}".format(execution_date, task_log_url)
    }

    email_message = f"""    
        AlarmName: Airflow Task {task_details.task_id} has failed.
        NewStateValue: ALARM,
        NewStateReason: FAILURE,
        StateChangeTime: {state_change_time},
        AlarmDescription: Task failed on {execution_date}. Log URL: {task_log_url}    
    """

    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(message),
        Subject="Airflow Task Failure Alert"
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("Alert sent successfully via SNS.")
    else:
        print("Failed to send the alert via SNS.")
        print(response)

    send_alert(email_message)


# CLUSTER_JSON_PATH: EMR CLUSTER CONFIG
cluster_spec_job_info = cover_open(json.load(open(CLUSTER_JSON_PATH)))

# Cluster for ABI Inbev Services
create_abi_service_cluster = EmrCreateJobFlowOperator(
    task_id="create_abi_services",
    job_flow_overrides=cluster_spec_job_info["Job"],
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    email_on_failure=True,
    on_failure_callback=send_sns_alert,
    poke_interval=100,
    email=EMAIL_LIST,
    dag=dag,
)

create_abi_service_cluster
