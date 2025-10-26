from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import sys

# --- Include your local data_generator.py path ---
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_DIR)

try:
    from data_generator import main as generate_data
except ImportError:
    def generate_data():
        print("Running fallback data generator...")
        pass

# --- DAG Configuration ---
S3_BUCKET = 'your-real-s3-bucket-name'   # Replace with your S3 bucket
S3_CONN_ID = 'your-aws-connection-id'    # Replace with your Airflow AWS connection

USE_S3 = True  # Set to False to force local testing

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Python callable functions ---
def _run_data_generator(**context):
    exec_date = context['ds']
    file_date = datetime.strptime(exec_date, '%Y-%m-%d').strftime('%Y%m%d')

    base_path = '/tmp/gen_data'
    os.makedirs(base_path, exist_ok=True)

    files_to_upload = [
        f'sensor_readings_{file_date}.csv',
        f'yield_results_{file_date}.csv',
        f'wafer_logs_{file_date}.json',
        'dim_equipment.csv',
        'dim_process_steps.csv'
    ]

    for f in files_to_upload:
        with open(os.path.join(base_path, f), 'w') as temp_f:
            temp_f.write("mock data")  # Replace with actual generate_data() output

    context['ti'].xcom_push(key='generated_files', value=files_to_upload)
    context['ti'].xcom_push(key='base_path', value=base_path)
    context['ti'].xcom_push(key='file_date', value=file_date)

def _upload_to_s3_or_local(**context):
    generated_files = context['ti'].xcom_pull(key='generated_files', task_ids='run_data_generator')
    base_path = context['ti'].xcom_pull(key='base_path', task_ids='run_data_generator')
    exec_date_str = context['ds']

    # Use a local variable for S3 mode to avoid scope issues
    use_s3_mode = USE_S3
    s3_hook = None
    if use_s3_mode:
        try:
            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        except Exception as e:
            print(f"Cannot connect to S3: {e}. Falling back to local storage...")
            use_s3_mode = False

    for file_name in generated_files:
        local_path = os.path.join(base_path, file_name)

        if use_s3_mode:
            if 'dim_' in file_name:
                s3_key = f'dimensions/{file_name}'
                replace = True
            else:
                table_name = file_name.split('_')[0]
                s3_key = f'transactional/{table_name}/dt={exec_date_str}/{file_name}'
                replace = False

            print(f'Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}')
            s3_hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=replace
            )
            os.remove(local_path)
        else:
            # Local fallback
            local_dest_dir = os.path.join(base_path, 'local_output')
            os.makedirs(local_dest_dir, exist_ok=True)
            local_dest = os.path.join(local_dest_dir, file_name)
            os.rename(local_path, local_dest)
            print(f"Saved {file_name} locally at {local_dest}")

# --- DAG definition ---
with DAG(
    dag_id='semiconductor_1_raw_data_ingestion_testable',
    default_args=default_args,
    description='Generates synthetic semiconductor data and uploads to S3 or local folder.',
    schedule='@daily',
    catchup=False,
    tags=['semiconductor', 'ingestion', 's3'],
) as dag:

    run_data_generator_task = PythonOperator(
        task_id='run_data_generator',
        python_callable=_run_data_generator,
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3_or_local',
        python_callable=_upload_to_s3_or_local,
    )

    run_data_generator_task >> upload_task
