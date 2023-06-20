from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocSubmitJobOperator)

# Param initializations
DAG_ID = "csv_to_bigquery_generations"
PROJECT_ID = "scenic-flux-389905"
BUCKET_NAME = "composer-staging"
CLUSTER_NAME = "newcluster"
REGION = "europe-central2"
ZONE = "europe-central2-b"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "scenic-flux-src/scripts"
SCRIPT_NAME = "csv-to-bigquery-generations.py"

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

# DAG definition
with models.DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime.now(),
        catchup=False,
        tags=["csv-to-bigquery-generations"],
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)

    pyspark_task_csv_to_bq = DataprocSubmitJobOperator(
        task_id="pyspark_task_csv_to_bq",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID)

    end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> pyspark_task_csv_to_bq >> end_task
