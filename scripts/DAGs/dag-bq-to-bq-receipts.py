from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocSubmitJobOperator)

# Param initializations
DAG_ID = "bq_raw_to_bq_normalized"
PROJECT_ID = "scenic-flux-389905"
BUCKET_NAME = "composer-staging"
CLUSTER_NAME = "newcluster"
REGION = "europe-central2"
ZONE = "europe-central2-b"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "scenic-flux-src/scripts"
# GCS -> BQ
SCRIPT_NAME = "bq-receipts-normalized.py"

# PySpark job configs
PYSPARK_JOB = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME}"
    }
}

# DAG definition is here
with models.DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime.now(),
        catchup=False,
        tags=["bq-receipts-normalized.py"],
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)

    # PySpark task to read data from Bigquery , perform agrregate on data and write data into GCS
    pyspark_task_bq_normalization = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_normalization",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID)

    end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the task dependencies
start_task >> pyspark_task_bq_normalization >> end_task
