from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.utils import AnalysisException
from google.cloud import bigquery
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFS to GCS to BQ Data Migration") \
    .getOrCreate()

# Configuration for parallelism
spark.conf.set("spark.sql.shuffle.partitions", "15")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define paths
gcs_path = "gs://<destination-gcs-path>"
bq_dataset = "<your-bigquery-dataset>"

# Load Data
def load_data_from_gcs(gcs_path):
    try:
        df = spark.read.parquet(gcs_path)
        logger.info(f"Loaded data from {gcs_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from GCS: {str(e)}")
        raise

# Data Quality Checks
def data_quality_checks(df):
    if df.count() == 0:
        raise ValueError("DataFrame is empty!")
    if not all(column in df.columns for column in ['required_column1', 'required_column2']):
        raise ValueError("Required columns are missing!")

# Write to BigQuery (Non-Partitioned)
def write_to_bq_non_partitioned(df, table_name):
    try:
        df.write \
            .format("bigquery") \
            .option("table", f"{bq_dataset}.{table_name}") \
            .mode("overwrite") \
            .save()
        logger.info(f"Data written to BigQuery table {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to BigQuery: {str(e)}")
        raise

# Write to BigQuery (Partitioned)
def write_to_bq_partitioned(df, table_name, partition_col):
    try:
        df.write \
            .format("bigquery") \
            .option("table", f"{bq_dataset}.{table_name}") \
            .option("partitionField", partition_col) \
            .mode("overwrite") \
            .save()
        logger.info(f"Data written to partitioned BigQuery table {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to BigQuery: {str(e)}")
        raise

# Create External Table in BigQuery
def create_external_bq_table(gcs_path, table_name):
    try:
        client = bigquery.Client()
        table = bigquery.Table(f"{bq_dataset}.{table_name}")
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [f"{gcs_path}/*.parquet"]
        table.external_data_configuration = external_config
        table = client.create_table(table)
        logger.info(f"External BigQuery table {table_name} created")
    except Exception as e:
        logger.error(f"Failed to create external BigQuery table: {str(e)}")
        raise

# Main process
def main():
    try:
        # Load data
        df = load_data_from_gcs(gcs_path)
        
        # Data quality checks
        data_quality_checks(df)
        
        # Write to BQ non-partitioned
        write_to_bq_non_partitioned(df, "my_non_partitioned_table")
        
        # Write to BQ partitioned
        write_to_bq_partitioned(df, "my_partitioned_table", "partition_column")
        
        # Create external table
        create_external_bq_table(gcs_path, "my_external_table")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        spark.stop()
        raise
    else:
        logger.info("Job completed successfully")
        spark.stop()

if __name__ == "__main__":
    main()
