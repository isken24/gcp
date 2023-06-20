#!/usr/bin/env python

import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from google.cloud import storage
import json

BUCKET_NAME = "scenic-flux-src"
BLOB_NAME = "vars.json"


def read_json_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = json.loads(blob.download_as_string(client=None))
    return data


project_vars = read_json_from_gcs(BUCKET_NAME, BLOB_NAME)

temporaryGcsBucket = project_vars['temporaryGcsBucket']
project_id = project_vars['project_id']
dataset_name = project_vars['dataset_name']
table_name = "generations"

# Create a SparkSession
spark = SparkSession.builder \
    .appName('CSV to BigQuery generations') \
    .getOrCreate()

# Read the CSV file into a DataFrame
input_path = 'gs://scenic-flux-import/generations.csv'
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("escapeQuotes", "true") \
    .load(input_path)


df.write.format("bigquery") \
    .mode("overwrite")\
    .option("temporaryGcsBucket", temporaryGcsBucket) \
    .option("project", project_id) \
    .option("dataset", dataset_name) \
    .option("table", table_name) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .save()

# Stop Spark session
spark.stop()
