import boto3
from datetime import datetime
from pytz import timezone
import json
import os
import requests
import logging
from send_email import send_email

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_file_list = []
required_file_list_with_bucket_name = []
s3_client=boto3.client('s3')
s3_bucket_name = os.environ['s3_bucket_name']
ec2_port = os.environ['airflow_ec2_port']
ec2_url = os.environ['airflow_ec2_url']
airflow_ec2_url = f'{ec2_url}:{ec2_port}'

af_username = os.environ['airflow_username']
af_password = os.environ['airflow_password']
af_dag_name = os.environ['airflow_dag_name']

time_format = "%Y-%m-%d"
now_eastern = timezone('US/Eastern')
time_now_eastern = datetime.now(now_eastern)
datestr = time_now_eastern.strftime(time_format)

for object in s3_client.list_objects_v2(Bucket=f'{s3_bucket_name}')['Contents']:
    s3_file_list.append(object['Key'])

required_file_list = [
    f'calendar_{datestr}.csv',
    f'inventory_{datestr}.csv',
    f'product_{datestr}.csv',
    f'sales_{datestr}.csv',
    f'store_{datestr}.csv'
]

for filename in required_file_list:
  full_filename = 's3://' + f'{s3_bucket_name}/{filename}'
  required_file_list_with_bucket_name.append(full_filename)

# This checks if all of the required files are in the s3 bucket. This allows
# for not having to strictly have the exact files in s3 == to the required files.
s3_file_list_contains_required_files = all(some_file in s3_file_list for some_file in required_file_list)

table_name = [a[:-15] for a in required_file_list]

def lambda_handler(event, context):

    if s3_file_list_contains_required_files:
        print('s3_file_list_contains_required_files')
        data = {'conf': {}}

        for i in range(len(table_name)):
          keys = table_name[i]
          values = required_file_list_with_bucket_name[i]
          data['conf'][keys] = values

        print('data is', data)

        # send signal to Airflow    
        endpoint= f'http://{airflow_ec2_url}/api/experimental/dags/{af_dag_name}/dag_runs'
    
        # Have to use requests library since CURL process is not available in Python 3.9 via Lambda
        req = requests.post(
            endpoint,
            data=json.dumps(data),
            auth=(af_username, af_password)
        )
    else:
        send_email()
