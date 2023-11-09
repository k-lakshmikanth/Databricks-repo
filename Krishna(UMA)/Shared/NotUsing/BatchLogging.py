# Databricks notebook source
dbutils.widgets.text("BatchName","Test")
dbutils.widgets.text("LoadType","F")


# COMMAND ----------

V_BatchName = dbutils.widgets.get("BatchName")
V_LoadType = dbutils.widgets.get("LoadType")

# COMMAND ----------

df=spark.sql("select * from kpi_etl_analytics_conf.etlbatchhistory")
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC update kpi_etl_analytics_conf.etlbatchhistory set Status='SUCCESS' where BatchId=5

# COMMAND ----------

etl_batch_history_df = spark.table("kpi_etl_analytics_conf.etlbatchhistory")

# COMMAND ----------

current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate
print(current_system_date)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, expr
from pyspark.sql.types import TimestampType

def create_batch(batch_name, load_type):
    # Create DataFrame to simulate the [KPI_ETL_PROCESS].[ETLBatchHistory] table
    # Replace this with the actual DataFrame representing your ETL batch history table.
    # Ensure that the DataFrame contains columns: BatchID, BatchName, Status, 
    # ExtractWindowBeginTS, ExtractWindowEndTS, LoadType
    etl_batch_history_df = spark.table("kpi_etl_analytics_conf.etlbatchhistory")

    # Get the current system date and time
    current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate

    # Define the default window start date and earliest full load date
    default_window_start_date = '2000-01-01 00:00:00'
    earliest_full_load_date = '1753-01-01 00:00:00'

    # Convert the load_type to uppercase
    load_type = load_type.upper()

    # Get the latest batch information based on the provided batch_name
    latest_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).orderBy(col("BatchID").desc()).first()

    if latest_batch:
        etl_batch_id = latest_batch.BatchID
        etl_batch_status = latest_batch.Status
        extract_window_start_date = latest_batch.ExtractWindowBeginTS
        extract_window_end_date = latest_batch.ExtractWindowEndTS
        batch_load_type = latest_batch.LoadType

        if batch_load_type == 'F' and load_type == 'I' and etl_batch_status not in ('SUCCESS', 'RUNNING'):
            raise Exception("Incremental Batch cannot be started without full load completion")

        if etl_batch_status == 'SUCCESS':
            extract_window_end_date = extract_window_end_date - expr("INTERVAL 10 HOURS")

            # Set the extract_window_end_date to earliest_full_load_date for full loads
            extract_window_end_date = when(load_type == 'F', earliest_full_load_date).otherwise(extract_window_end_date)

            etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
            etl_batch_history_df = etl_batch_history_df.withColumn("ExecuteEndTS", current_system_date)
            etl_batch_history_df = etl_batch_history_df.withColumn("Status", when(col("BatchID") == etl_batch_id, "CANCELLED").otherwise(col("Status")))
            etl_batch_history_df = etl_batch_history_df.withColumn("ExecutionStatus", when(col("BatchID") == etl_batch_id, 0).otherwise(1))
            etl_batch_history_df = etl_batch_history_df.union(
                spark.createDataFrame([(etl_batch_id_new, batch_name, current_system_date, None, extract_window_end_date, current_system_date, "RUNNING", load_type)]))
            etl_batch_id = etl_batch_id_new
        else:
            if not etl_batch_id:  # If there is no record in ETLBatchHistory for BatchName
                default_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(default_window_start_date)
                etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
                etl_batch_history_df = etl_batch_history_df.union(
                    spark.createDataFrame([(etl_batch_id_new, batch_name, current_system_date, None, default_window_start_date, current_system_date, "RUNNING", load_type)]))
                etl_batch_id = etl_batch_id_new
            elif etl_batch_status == 'RUNNING':
                etl_batch_history_df = etl_batch_history_df.withColumn("ExecutionStatus", when(col("BatchID") == etl_batch_id, 0).otherwise(1))
            elif etl_batch_status == 'CANCELLED':
                extract_window_start_date = extract_window_start_date - expr("INTERVAL 10 MINUTES")
                extract_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(extract_window_start_date)
                etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
                etl_batch_history_df = etl_batch_history_df.union(
                    spark.createDataFrame([(etl_batch_id_new, batch_name, current_system_date, None, extract_window_start_date, current_system_date, "RUNNING", load_type)]))
                etl_batch_id = etl_batch_id_new
            else:
                extract_window_start_date = extract_window_end_date - expr("INTERVAL 10 MINUTES")
                extract_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(extract_window_start_date)
                etl_batch_history_df = etl_batch_history_df.withColumn("ExecuteBeginTS", current_system_date)
                etl_batch_history_df = etl_batch_history_df.withColumn("Status", "RUNNING")
                etl_batch_history_df = etl_batch_history_df.withColumn("ExecutionStatus", when(col("BatchID") == etl_batch_id, 0).otherwise(1))
                etl_batch_id = etl_batch_id

    else:
        # No batch information found for the provided batch_name, creating a new batch record.
        default_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(default_window_start_date)
        etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
        etl_batch_history_df = etl_batch_history_df.union(
            spark.createDataFrame([(etl_batch_id_new, batch_name, current_system_date, None, default_window_start_date, current_system_date, "RUNNING", load_type)]))
        etl_batch_id = etl_batch_id_new

    return etl_batch_id, extract_window_start_date, extract_window_end_date, load_type, 1
display(etl_batch_history_df)
# Initialize SparkSession
spark = SparkSession.builder.appName("ETLProcess").getOrCreate()

# Call the function with the desired parameters
batch_name = "example_batch"
load_type = "F"
etl_batch_id, extract_window_start_date, extract_window_end_date, load_type, execution_status = create_batch(batch_name, load_type)

# Print the results
print(f"ETL Batch ID: {etl_batch_id}")
print(f"Extract Window Start Date: {extract_window_start_date}")
print(f"Extract Window End Date: {extract_window_end_date}")
print(f"Load Type: {load_type}")
print(f"Execution Status: {execution_status}")

# Stop the SparkS


# COMMAND ----------

# Call the function with the desired parameters
batch_name = "example_batch"
load_type = "F"
etl_batch_id, extract_window_start_date, extract_window_end_date, load_type, execution_status = create_batch(batch_name, load_type)

# Print the results
print(f"ETL Batch ID: {etl_batch_id}")
print(f"Extract Window Start Date: {extract_window_start_date}")
print(f"Extract Window End Date: {extract_window_end_date}")
print(f"Load Type: {load_type}")
print(f"Execution Status: {execution_status}")

# Stop the SparkSession
spark.stop()
