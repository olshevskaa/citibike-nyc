import os
import requests
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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

LOCAL_ZIP_PATH = os.path.join(BASE_DIR, "citibike.zip")
LOCAL_CSV_PATH = os.path.join(BASE_DIR, "citibike.csv")
LOCAL_PARQUET_PATH = os.path.join(BASE_DIR, "citibike.parquet")
GCS_PARQUET_PATH = f"citibike/202401-citibike.parquet" 

default_args = {
    'owner': 'airflow',
}

def print_base_dir():
    print("BASE_DIR:", BASE_DIR)

def download_csv():
    os.makedirs(BASE_DIR, exist_ok=True)

    url = "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip"
    response = requests.get(url)
    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)


def unzip_and_convert_to_parquet():
    os.makedirs(BASE_DIR, exist_ok=True)

    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(BASE_DIR)

    files = os.listdir(BASE_DIR)
    csv_files = [f for f in files if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("No CSV file found after extraction.")
    actual_csv_path = os.path.join(BASE_DIR, csv_files[0])

    df = pd.read_csv(actual_csv_path)

    # Fix mixed-type columns, e.g., 'start_station_id' and 'end_station_id'
    for col in ['start_station_id', 'end_station_id']:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('')

    table = pa.Table.from_pandas(df)
    pq.write_table(table, LOCAL_PARQUET_PATH)

def upload_to_gcs():
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_PARQUET_PATH)
    blob.upload_from_filename(LOCAL_PARQUET_PATH)

def load_into_bigquery():
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.Client(credentials=credentials)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{GCS_BUCKET_NAME}/{GCS_PARQUET_PATH}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

with DAG(
    dag_id="citibike_etl_dag",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv
    )

    convert_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=unzip_and_convert_to_parquet
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs
    )

    load_bq_task = PythonOperator(
        task_id="load_into_bigquery",
        python_callable=load_into_bigquery
    )

    download_task >> convert_task >> upload_task >> load_bq_task