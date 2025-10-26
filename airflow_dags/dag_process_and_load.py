from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# --- Configuration ---
USE_S3 = False  # Toggle between S3 or local mode
S3_RAW_BUCKET = 'your-semiconductor-raw-data-lake'
S3_PROCESSED_BUCKET = 'your-semiconductor-processed-zone'
SNOWFLAKE_CONN_ID = 'snowflake_default'

# --- Dynamic Paths ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
SPARK_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'src', 'transform_spark_job.py')
LOCAL_RAW_DIR = os.path.join(PROJECT_ROOT, 'raw')
LOCAL_PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'processed')

os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
os.makedirs(LOCAL_PROCESSED_DIR, exist_ok=True)

# --- SQL commands ---
COPY_INTO_FACT_PROCESS_SQL = f"""
COPY INTO fact_process_measurements
FROM 'file://{LOCAL_PROCESSED_DIR}/fact_process_measurements/dt={{ ds }}/'
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ON_ERROR = 'SKIP_FILE';
"""

COPY_INTO_FACT_YIELD_SQL = f"""
COPY INTO fact_wafer_yield
FROM 'file://{LOCAL_PROCESSED_DIR}/fact_wafer_yield/dt={{ ds }}/'
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ON_ERROR = 'SKIP_FILE';
"""

COPY_INTO_DIM_SQL = f"""
COPY INTO dim_equipment
FROM 'file://{LOCAL_RAW_DIR}/dimensions/dim_equipment.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'SKIP_FILE';

COPY INTO dim_process_steps
FROM 'file://{LOCAL_RAW_DIR}/dimensions/dim_process_steps.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'SKIP_FILE';
"""

# --- DAG Default Args ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': True,
    'start_date': datetime(2025, 10, 27),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Python callable to run Spark script locally ---
def run_spark(**context):
    exec_date = context['ds']
    try:
        raw_bucket = S3_RAW_BUCKET if USE_S3 else LOCAL_RAW_DIR
        processed_bucket = S3_PROCESSED_DIR if USE_S3 else LOCAL_PROCESSED_DIR

        cmd = [
            'python', SPARK_SCRIPT_PATH,
            '--execution_date', exec_date,
            '--raw_bucket', raw_bucket,
            '--processed_bucket', processed_bucket
        ]
        print("Running Spark script:", " ".join(cmd))
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print("Error running Spark script:", e.stderr)
        raise

# --- DAG Definition ---
with DAG(
    dag_id='semiconductor_2_process_and_load_dual',
    default_args=default_args,
    description='Transforms raw data (S3/local) and loads into Snowflake.',
    schedule='@daily',
    catchup=False,
    tags=['semiconductor', 'transformation', 'spark', 'dual-mode'],
) as dag:

    # --- Sensors for S3 (soft_fail in local mode) ---
    wait_for_sensor_data = S3KeySensor(
        task_id='wait_for_sensor_data',
        bucket_name=S3_RAW_BUCKET if USE_S3 else None,
        bucket_key=f'transactional/sensor/dt={{ ds }}/sensor_readings_{{ ds_nodash }}.csv' if USE_S3 else None,
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=1800,
        soft_fail=not USE_S3,
        mode='reschedule'
    )

    wait_for_yield_data = S3KeySensor(
        task_id='wait_for_yield_data',
        bucket_name=S3_RAW_BUCKET if USE_S3 else None,
        bucket_key=f'transactional/yield/dt={{ ds }}/yield_results_{{ ds_nodash }}.csv' if USE_S3 else None,
        aws_conn_id='aws_default',
        soft_fail=not USE_S3
    )

    wait_for_log_data = S3KeySensor(
        task_id='wait_for_log_data',
        bucket_name=S3_RAW_BUCKET if USE_S3 else None,
        bucket_key=f'transactional/wafer/dt={{ ds }}/wafer_logs_{{ ds_nodash }}.json' if USE_S3 else None,
        aws_conn_id='aws_default',
        soft_fail=not USE_S3
    )

    # --- Run Spark transformation ---
    run_spark_task = PythonOperator(
        task_id='run_spark_transformation',
        python_callable=run_spark
    )

    # --- Load dimension tables into Snowflake ---
    load_dimensions_to_snowflake = SQLExecuteQueryOperator(
        task_id='load_dimensions_to_snowflake',
        sql=COPY_INTO_DIM_SQL,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # --- Load fact tables into Snowflake ---
    load_facts_to_snowflake = SQLExecuteQueryOperator(
        task_id='load_facts_to_snowflake',
        sql=f"{COPY_INTO_FACT_PROCESS_SQL}\n{COPY_INTO_FACT_YIELD_SQL}",
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # --- Task dependencies ---
    [wait_for_sensor_data, wait_for_yield_data, wait_for_log_data] >> run_spark_task >> load_dimensions_to_snowflake >> load_facts_to_snowflake














# from airflow import DAG
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from datetime import datetime, timedelta

# # --- Configuration ---
# S3_RAW_BUCKET = 'your-semiconductor-raw-data-lake'
# S3_PROCESSED_BUCKET = 'your-semiconductor-processed-zone'
# SNOWFLAKE_CONN_ID = 'snowflake_default'  # Ensure this connection is set up in Airflow

# # SQL commands
# COPY_INTO_FACT_PROCESS_SQL = f"""
# COPY INTO fact_process_measurements
# FROM 's3://{S3_PROCESSED_BUCKET}/fact_process_measurements/dt={{ ds }}/'
# FILE_FORMAT = (TYPE = 'PARQUET')
# MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
# ON_ERROR = 'SKIP_FILE';
# """

# COPY_INTO_FACT_YIELD_SQL = f"""
# COPY INTO fact_wafer_yield
# FROM 's3://{S3_PROCESSED_BUCKET}/fact_wafer_yield/dt={{ ds }}/'
# FILE_FORMAT = (TYPE = 'PARQUET')
# MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
# ON_ERROR = 'SKIP_FILE';
# """

# COPY_INTO_DIM_SQL = """
# COPY INTO dim_equipment
# FROM 's3://{S3_RAW_BUCKET}/dimensions/dim_equipment.csv'
# FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
# ON_ERROR = 'SKIP_FILE';

# COPY INTO dim_process_steps
# FROM 's3://{S3_RAW_BUCKET}/dimensions/dim_process_steps.csv'
# FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
# ON_ERROR = 'SKIP_FILE';
# """

# # --- DAG Definition ---
# default_args = {
#     'owner': 'data_engineer',
#     'depends_on_past': True,  # This DAG depends on the ingestion DAG
#     'start_date': datetime(2025, 10, 27),
#     'email_on_failure': False,
#     'retries': 1,
# }

# with DAG(
#     dag_id='semiconductor_2_process_and_load',
#     default_args=default_args,
#     description='Runs Spark job to transform raw data and loads it into Snowflake.',
#     schedule='@daily',
#     catchup=False,
#     tags=['semiconductor', 'transformation', 'spark', 'snowflake'],
# ) as dag:

#     # 1. Wait for all raw data files for the day to arrive in S3
#     wait_for_sensor_data = S3KeySensor(
#         task_id='wait_for_sensor_data',
#         bucket_name=S3_RAW_BUCKET,
#         bucket_key=f'transactional/sensor/dt={{ ds }}/sensor_readings_{{ ds_nodash }}.csv',
#         aws_conn_id='aws_default',
#         poke_interval=60,  # Check every 60 seconds
#         timeout=1800,  # Timeout after 30 mins
#     )
    
#     wait_for_yield_data = S3KeySensor(
#         task_id='wait_for_yield_data',
#         bucket_name=S3_RAW_BUCKET,
#         bucket_key=f'transactional/yield/dt={{ ds }}/yield_results_{{ ds_nodash }}.csv',
#         aws_conn_id='aws_default',
#     )
    
#     wait_for_log_data = S3KeySensor(
#         task_id='wait_for_log_data',
#         bucket_name=S3_RAW_BUCKET,
#         bucket_key=f'transactional/wafer/dt={{ ds }}/wafer_logs_{{ ds_nodash }}.json',
#         aws_conn_id='aws_default',
#     )

#     # 2. Load dimension tables into Snowflake
#     load_dimensions_to_snowflake = SQLExecuteQueryOperator(
#         task_id='load_dimensions_to_snowflake',
#         sql=COPY_INTO_DIM_SQL,
#         conn_id=SNOWFLAKE_CONN_ID,
#     )
    
#     # 3. Load fact tables into Snowflake
#     load_facts_to_snowflake = SQLExecuteQueryOperator(
#         task_id='load_facts_to_snowflake',
#         sql=f"{COPY_INTO_FACT_PROCESS_SQL}\n{COPY_INTO_FACT_YIELD_SQL}",
#         conn_id=SNOWFLAKE_CONN_ID,
#     )

#     # --- Define Task Dependencies ---
#     [wait_for_sensor_data, wait_for_yield_data, wait_for_log_data] \
#         >> load_dimensions_to_snowflake \
#         >> load_facts_to_snowflake
