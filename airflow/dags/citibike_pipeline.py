import os
import requests
import zipfile
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from google.cloud import storage, bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv

load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

BASE_DIR = "/tmp/citibike_data"

default_args = {
    'owner': 'airflow',
}

def get_file_paths(execution_date: datetime):
    file_id = get_file_id(execution_date)
    return {
        "zip": f"{BASE_DIR}/citibike_{file_id}.zip",
        "csv": f"{BASE_DIR}/citibike_{file_id}.csv",
        "parquet": f"{BASE_DIR}/citibike_{file_id}.parquet"
    }

def get_file_id(execution_date_str):
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    return execution_date.strftime("%Y%m")

def should_process_data(execution_date_str):
    file_id = get_file_id(execution_date_str)
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"citibike/{file_id}.parquet")

    exists = blob.exists(client)
    print(f"[CHECK] File exists in GCS: {exists}")
    return not exists

def download_csv(execution_date_str):
    file_id = get_file_id(execution_date_str)
    os.makedirs(BASE_DIR, exist_ok=True)

    url_candidates = [
        f"https://s3.amazonaws.com/tripdata/{file_id}-citibike-tripdata.csv.zip",
        f"https://s3.amazonaws.com/tripdata/{file_id}-citibike-tripdata.zip",
    ]

    paths = get_file_paths(execution_date_str)

    for url in url_candidates:
        print(f"Trying URL: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            with open(paths["zip"], "wb") as f:
                f.write(response.content)
            print(f"Downloaded ZIP: {paths["zip"]} ({os.path.getsize(paths["zip"])} bytes)")
            return True

    print(f"No available file for file_id: {file_id}. Skipping this run.")
    return False


def extract_csv_files(zip_file_path, unzip_dir, file_id):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    files = os.listdir(unzip_dir)
    print('Files:', files)

    csv_files = []
    for root, dirs, files in os.walk(unzip_dir):
        for file in files:
            if file.endswith(".csv") and file_id in file:
                csv_files.append(os.path.join(root, file))

    if not csv_files:
        raise FileNotFoundError("No CSV file found after extraction.")
    return csv_files

def load_csv_files(csv_files, unzip_dir):
    def read_csv_with_fallback(csv_path):
        try:
            return pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            print(f"UTF-8 decode failed for {csv_path}, trying ISO-8859-1...")
            return pd.read_csv(csv_path, encoding='ISO-8859-1')  # or 'latin1'

    if len(csv_files) == 1:
        csv_path = os.path.join(unzip_dir, csv_files[0])
        df = read_csv_with_fallback(csv_path)
    else:
        df_list = []
        for csv_file in csv_files:
            csv_path = os.path.join(unzip_dir, csv_file)
            df_tmp = read_csv_with_fallback(csv_path)
            df_list.append(df_tmp)
        df = pd.concat(df_list, ignore_index=True)
    return df


def preprocess_df(df):
# Fix mixed-type columns, e.g., 'start_station_id' and 'end_station_id'
    for col in ['start_station_id', 'end_station_id']:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('')

    expected_columns = [
        'ride_id',
        'rideable_type',
        'started_at',
        'ended_at',
        'start_station_name',
        'start_station_id',
        'end_station_name',
        'end_station_id',
        'start_lat',
        'start_lng',
        'end_lat',
        'end_lng',
        'member_casual'
    ]

    df = df[[col for col in expected_columns if col in df.columns]]

    for date_col in ['started_at', 'ended_at']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], utc=True)
            # Convert to microsecond precision (BigQuery compatible)
            df[date_col] = df[date_col].dt.floor('us')

    return df

def convert_to_parquet(df, parquet_path):
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    new_fields = []
    for field in table.schema:
        if field.name in ['started_at', 'ended_at']:
            new_fields.append(pa.field(field.name, pa.timestamp('us', tz='UTC')))
        else:
            new_fields.append(field)
    
    new_schema = pa.schema(new_fields)
    
    table = table.cast(new_schema)

    pq.write_table(
        table, 
        parquet_path,
        use_deprecated_int96_timestamps=False,
        coerce_timestamps='us',
        allow_truncated_timestamps=True
    )
    print(f"Parquet saved at: {parquet_path}")
    print(f"Schema: {table.schema}")  # Debug info

def cleanup_zip_dir(unzip_dir):
    shutil.rmtree(unzip_dir)
    print(f"Cleaned up extracted files from {unzip_dir}")

def cleanup_zip_dir(unzip_dir):
    shutil.rmtree(unzip_dir)
    print(f"Cleaned up extracted files from {unzip_dir}")

def unzip_and_convert_to_parquet(execution_date_str):
    file_id = get_file_id(execution_date_str)
    paths = get_file_paths(execution_date_str)
    unzip_dir = f'{BASE_DIR}/csv'

    csv_files = extract_csv_files(paths["zip"], unzip_dir, file_id)
    df = load_csv_files(csv_files, unzip_dir)
    df = preprocess_df(df)
    convert_to_parquet(df, paths["parquet"])
    cleanup_zip_dir(unzip_dir)

def upload_to_gcs(execution_date_str):
    file_id = get_file_id(execution_date_str)
    paths = get_file_paths(execution_date_str)

    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = storage.Client(credentials=credentials)

    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"citibike/{file_id}.parquet")
    blob.upload_from_filename(paths["parquet"])

    if os.path.exists(paths['parquet']):
        os.remove(paths['parquet'])
        print(f"Deleted local parquet file {paths['parquet']}")
    if os.path.exists(paths['zip']):
        os.remove(paths['zip'])
        print(f"Deleted local zip file {paths['zip']}")

def create_partitioned_table_if_not_exists():
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.Client(credentials=credentials)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    schema = [
        bigquery.SchemaField("ride_id", "STRING"),
        bigquery.SchemaField("rideable_type", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("ended_at", "TIMESTAMP"),
        bigquery.SchemaField("start_station_name", "STRING"),
        bigquery.SchemaField("start_station_id", "STRING"),
        bigquery.SchemaField("end_station_name", "STRING"),
        bigquery.SchemaField("end_station_id", "STRING"),
        bigquery.SchemaField("start_lat", "FLOAT"),
        bigquery.SchemaField("start_lng", "FLOAT"),
        bigquery.SchemaField("end_lat", "FLOAT"),
        bigquery.SchemaField("end_lng", "FLOAT"),
        bigquery.SchemaField("member_casual", "STRING"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Configure partitioning by started_at (monthly partitions)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="started_at"
    )
    
    table.clustering_fields = ["member_casual", "rideable_type"]
    
    try:
        table = client.create_table(table, exists_ok=True)
        print(f"Created partitioned table {table_id}")
    except Exception as e:
        print(f"Table creation failed or already exists: {e}")


def load_into_bigquery(execution_date_str):
    file_id = get_file_id(execution_date_str)
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.Client(credentials=credentials)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    year = execution_date.year
    month = execution_date.month
    
    # Delete existing data for this month (if any) to avoid duplicates
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE DATE_TRUNC(started_at, MONTH) = DATE('{year}-{month:02d}-01')
    """
    
    try:
        delete_job = client.query(delete_query)
        delete_job.result()
        print(f"Deleted existing data for {year}-{month:02d}")
    except Exception as e:
        print(f"Delete operation failed (table might not exist): {e}")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    uri = f"gs://{GCS_BUCKET_NAME}/citibike/{file_id}.parquet"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    
    print(f"Loaded data for {file_id} into partitioned table {table_id}")

with DAG(
    dag_id="citibike_etl_dag",
    default_args=default_args,
    schedule_interval="0 0 1/7 * *",
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=True,
) as dag:
    
    check_if_needed = ShortCircuitOperator(
        task_id="check_if_needed",
        python_callable=should_process_data,
        provide_context=True,
        op_kwargs={"execution_date_str": "{{ ds }}"},
        dag=dag,
    )

    check_file_available = ShortCircuitOperator(
        task_id="check_file_available",
        python_callable=download_csv,
        op_kwargs={"execution_date_str": "{{ ds }}"},
    )

    convert_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=unzip_and_convert_to_parquet,
        op_kwargs={"execution_date_str": "{{ ds }}"},
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"execution_date_str": "{{ ds }}"},
    )

    create_table_task = PythonOperator(
        task_id="create_partitioned_table",
        python_callable=create_partitioned_table_if_not_exists,
    )

    load_bq_task = PythonOperator(
        task_id="load_into_bigquery",
        python_callable=load_into_bigquery,
        op_kwargs={"execution_date_str": "{{ ds }}"}
    )

    check_if_needed >> check_file_available >> convert_task >> upload_task >> create_table_task >> load_bq_task