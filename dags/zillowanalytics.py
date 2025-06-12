import json
import requests
from airflow import DAG
from datetime import timedelta, datetime
#from airflow.timetable.interval import CronDataIntervalTimetable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# load JSON config file
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")
s3_bucket = 'data-pipeline-transformed-zillow-data'

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']
    #return headers
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    #specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    #write the json response to a file
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)#indent for format
    output_list = [output_file_path,file_str]
    return output_list


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 5),
    'email': ['deepeka.learnings@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

#op_kwargs is the dictionary that gets unpacked in the function
with DAG(
    dag_id='zillow_analytics_dag',
    default_args=default_args,
    #schedule_interval='@daily',
    #timetable=CronDataIntervalTimetable(
    #    cron="0 0 * * *",
    #    timezone="UTC"
    #),
    catchup=False
    ) as dag:
    
    extract_zillow_data_var = PythonOperator(
        task_id = 'task_extract_zillow_data_var',
        python_callable=extract_zillow_data,
        op_kwargs={'url':'https://zillow-com4.p.rapidapi.com/properties/search','querystring' : {"location":"Houston, TX","status":"forSale","sort":"relevance","sortType":"asc","priceType":"listPrice","listingType":"agent"}, 'headers': api_host_key, 'date_string': dt_now_string}
        )

    # ti - task instance
    # xcom- cross communication
    load_to_s3_var = BashOperator(
        task_id = 'task_load_to_S3',
        bash_command = 'aws s3 mv {{ ti.xcom_pull("task_extract_zillow_data_var")[0]}} s3://data-pipeline-airflow-zillow-data/'
    ) 
    
    #Adding S3 sensor
    is_file_in_S3_bucket = S3KeySensor(
        task_id='task_is_file_available_in_S3',
        bucket_key='{{ti.xcom_pull("task_extract_zillow_data_var")[1]}}',
        bucket_name=s3_bucket,
        aws_conn_id='aws_s3_conn',
        wildcard_match=False,
        timeout=120, #Optional: timeout in seconds
        poke_interval=5, #Optional: time interval in seconds
    )

    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id = "task_s3_to_redshift",
        aws_conn_id = 'aws_s3_conn',
        redshift_conn_id = 'redshift_conn',
        s3_bucket = s3_bucket,
        s3_key = '{{ti.xcom_pull("task_extract_zillow_data_var")[1]}}',
        schema = 'public',
        table = 'zillow_data',
        copy_options = ['CSV IGNOREHEADER 1'],
    )

    #creating the precedence/dependency
    extract_zillow_data_var >> load_to_s3_var >> is_file_in_S3_bucket >> transfer_s3_to_redshift