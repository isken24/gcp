from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocSubmitJobOperator)

# Param initializations
DAG_ID = "csv_to_bigquery_as_is"
PROJECT_ID = "scenic-flux-389905"
BUCKET_NAME = "composer-staging"
CLUSTER_NAME = "newcluster"
REGION = "europe-central2"
ZONE = "europe-central2-b"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "scenic-flux-src/scripts"
SCRIPT_NAME_01 = "csv-to-bigquery-pastry-inventory.py"
SCRIPT_NAME_02 = "csv-to-bigquery-sales-targets.py"
SCRIPT_NAME_03 = "csv-to-bigquery-sales-outlet.py"

# PySpark job configs
PYSPARK_JOB_01 = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_01}"
    }
}

PYSPARK_JOB_02 = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_02}"
    }
}

PYSPARK_JOB_03 = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_03}"
    }
}

# DAG definition
with models.DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime.now(),
        catchup=False,
        tags=["csv-to-bigquery-pastry-inventory"],
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)

    pyspark_task_csv_to_bq_pastry_inventory = DataprocSubmitJobOperator(
        task_id="pyspark_task_csv_to_bq_pastry_inventory",
        job=PYSPARK_JOB_01,
        region=REGION,
        project_id=PROJECT_ID)

    pyspark_task_csv_to_bq_sales_targets = DataprocSubmitJobOperator(
        task_id="pyspark_task_csv_to_bq_sales_targets",
        job=PYSPARK_JOB_02,
        region=REGION,
        project_id=PROJECT_ID)

    pyspark_task_csv_to_bq_sales_outlet = DataprocSubmitJobOperator(
        task_id="pyspark_task_csv_to_bq_sales_outlet",
        job=PYSPARK_JOB_03,
        region=REGION,
        project_id=PROJECT_ID)

    end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> [
    pyspark_task_csv_to_bq_pastry_inventory,
    pyspark_task_csv_to_bq_sales_targets, pyspark_task_csv_to_bq_sales_outlet
] >> end_task
