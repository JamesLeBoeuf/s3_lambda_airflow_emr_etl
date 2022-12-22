import json
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)

from airflow.providers.amazon.aws.sensors.emr import (
    EmrStepSensor
)

SPARK_EMR_NAME = 'wcd-de-midterm'
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = Variable.get("BUCKET_NAME")
PARTITION_COLUMN = Variable.get("PARTITION_COLUMN") # i.e. store_id, product_id
LOG_URI = Variable.get("LOG_URI") # s3n://aws-logs-xxxxxxxxx-us-east-1/elasticmapreduce/
S3_EMR_WORKFLOW_ENTRY = Variable.get('S3_EMR_WORKFLOW_ENTRY') # s3://xxxxx/xxxx/workflow_entry.py

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False, # Dont backfill any jobs in past
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    'email': ['airflowuser@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_STEPS = [
    {
        'Name': 'sales_inv_store_wk',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode",
                "client",
                '{{ params.emr_workflow_entry }}',
                '-p', json.dumps({
                        'input_path': "{{ task_instance.xcom_pull( 'parse_request', key='s3_locations') }}",
                        'output_path': 's3://{{ params.output_path }}',
                        'partition_column': '{{ params.partition_column }}',
                        'name': '{{ params.name }}',
                    })
            ]
        },
    }
]

LAST_STEP = len(SPARK_STEPS) - 1

JOB_FLOW_OVERRIDES = {
    "Name": "auto_cluster_wcd_de_midterm",
    "ReleaseLabel": "emr-6.9.0",
    'LogUri': LOG_URI,
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

def retrieve_s3_files(**kwargs):
    s3_locations = kwargs['params']
    kwargs['ti'].xcom_push(key = 's3_locations', value = s3_locations)

dag = DAG(
    dag_id = "lambda_airflow",
    default_args = DEFAULT_ARGS,
    catchup = False,
    schedule = None,
    dagrun_timeout = timedelta(hours = 2),
    tags = ["s3_lambda_airflow_emr_etl"],
)

start_operator = DummyOperator(
    task_id = "Begin_execution",
    dag = dag
)

parse_request = PythonOperator(
    task_id = "parse_request",
    python_callable = retrieve_s3_files,
    provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
    dag = dag
)

# Auto create EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id = "create_emr_cluster",
    job_flow_overrides = JOB_FLOW_OVERRIDES,
    aws_conn_id = AWS_CONN_ID,
    dag = dag
)

# Add steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id = "add_steps",
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps = SPARK_STEPS,
    params = {
        "output_path": BUCKET_NAME,
        "partition_column": PARTITION_COLUMN,
        "name": SPARK_EMR_NAME,
        "emr_workflow_entry": S3_EMR_WORKFLOW_ENTRY
    },
    aws_conn_id = AWS_CONN_ID,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = "watch_step",
    job_flow_id = "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(LAST_STEP)
    + "] }}",
    aws_conn_id = AWS_CONN_ID,
    dag = dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id = "terminate_emr_cluster",
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id = AWS_CONN_ID,
    dag = dag
)

end_data_pipeline = DummyOperator(
    task_id = "end_data_pipeline",
    dag = dag
)

start_operator >> parse_request >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster >> end_data_pipeline

# Manual EMR cluster (cluster_id)
# CLUSTER_ID = 'xxxxxxx'
# step_adder = EmrAddStepsOperator(
#     task_id='add_steps',
#     job_flow_id = CLUSTER_ID,
#     steps=SPARK_STEPS,
#     dag = dag
# )

# step_checker = EmrStepSensor(
#     task_id='watch_step',
#     job_flow_id = CLUSTER_ID,
#     step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
#     dag = dag
# )

# start_operator >> parse_request >> step_adder >> step_checker >> end_data_pipeline
