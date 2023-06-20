from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocSubmitJobOperator)

# Param initializations
DAG_ID = "bq_raw_to_bq_norm_multiple_tables"
PROJECT_ID = "scenic-flux-389905"
BUCKET_NAME = "composer-staging"
CLUSTER_NAME = "newcluster"
REGION = "europe-central2"
ZONE = "europe-central2-b"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "scenic-flux-src/scripts"

SCRIPT_NAME_01 = "bq-customer-normalized.py"
SCRIPT_NAME_02 = "bq-dates-normalized.py"
SCRIPT_NAME_03 = "bq-product-normalized.py"
SCRIPT_NAME_04 = "bq-staff-normalized.py"

# PySpark jobs configs
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

PYSPARK_JOB_04 = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_04}"
    }
}

# DAG definition is here
with models.DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime.now(),
        catchup=False,
        tags=["normalization of input data"],
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)

    pyspark_task_bq_customer_normalized = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_customer_normalized",
        job=PYSPARK_JOB_01,
        region=REGION,
        project_id=PROJECT_ID)

    pyspark_task_bq_dates_normalized = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_dates_normalized",
        job=PYSPARK_JOB_02,
        region=REGION,
        project_id=PROJECT_ID)

    pyspark_task_bq_product_normalized = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_product_normalized",
        job=PYSPARK_JOB_03,
        region=REGION,
        project_id=PROJECT_ID)

    pyspark_task_bq_staff_normalized = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_staff_normalized",
        job=PYSPARK_JOB_04,
        region=REGION,
        project_id=PROJECT_ID)

    end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the task dependencies
start_task >> [
    pyspark_task_bq_customer_normalized, pyspark_task_bq_dates_normalized,
    pyspark_task_bq_product_normalized, pyspark_task_bq_staff_normalized
] >> end_task
