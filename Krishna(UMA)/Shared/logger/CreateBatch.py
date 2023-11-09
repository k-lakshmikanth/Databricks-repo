# Databricks notebook source
dbutils.widgets.text("BatchName","")
dbutils.widgets.text("LoadType","")
dbutils.widgets.text("BatchId","")
dbutils.widgets.text("Ind","Process")

# COMMAND ----------

BatchName=dbutils.widgets.get("BatchName")
LoadType=dbutils.widgets.get("LoadType")
V_BatchId = dbutils.widgets.get("BatchId").strip()
V_Ind = dbutils.widgets.get("Ind")
display(V_BatchId)
display(BatchName)
display(LoadType)
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

# COMMAND ----------

# %sql
# select distinct  Status from (
# select  distinct BatchID,BatchName,Status,RetryAttempt ,
# rank()over(partition by BatchName order  by RetryAttempt  desc ) rnk
# from kpi_etl_analytics_conf.etl_batch_history_test where BatchID in (
# select max(BatchID) from kpi_etl_analytics_conf.etl_batch_history_test where BatchName like  '%BronzeExtraction%' group by BatchName
# )) where rnk=1 and Status!="SUCCESS"


same_level_status=spark.sql(f"""
select distinct  Status from (
    select  distinct BatchID,BatchName,Status,RetryAttempt ,
            rank()over(partition by BatchName order  by RetryAttempt  desc ) rnk
from kpi_etl_analytics_conf.etl_batch_history_test where BatchID in (
    select max(BatchID) from kpi_etl_analytics_conf.etl_batch_history_test 
    where BatchName like  '%BronzeExtraction%' group by BatchName
)) where rnk=1 """).collect()

print(same_level_status)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, expr
from pyspark.sql.types import TimestampType
import sys
V_error_message = ""
V_error_line=""
V_NewJobId=0
result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM {V_JobLogTable}""")
V_NewJobId = result_df.collect()[0]['JobId_NEW']
print(V_NewJobId)
def create_batch(batch_name, load_type):
    try:
        if load_type is None or load_type =='':
            df = spark.sql(f"""SELECT LoadType FROM {V_BatchLogTable} ORDER BY batchId DESC LIMIT 1""")
            if df.count()>0:
                load_type = df.select("LoadType").collect()[0][0]
            display(load_type)

        etl_batch_history_df = spark.table("V_BatchLogTable")

        # Get the current system date and time
        current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate

        # Define the default window start date and earliest full load date
        earliest_full_load_date = '1900-01-01 00:00:00'

        # Convert the load_type to uppercase
        load_type = load_type.upper()

        # Get the latest batch information based on the provided batch_name
        latest_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).orderBy(col("BatchID").desc()).first()

        new_source_name=batch_name.spilt("_")[0]

        level_name=batch_name.spilt("_")[1]
        
        same_level_status=spark.sql(f"""
select distinct  Status from (
    select  distinct BatchID,BatchName,Status,RetryAttempt ,
            rank()over(partition by BatchName order  by RetryAttempt  desc ) rnk
from kpi_etl_analytics_conf.etl_batch_history_test where BatchID in (
    select max(BatchID) from kpi_etl_analytics_conf.etl_batch_history_test 
    where BatchName like  '%{level_name}%' group by BatchName
)) where rnk=1 and Status!='SUCCESS' """).collect()[0][0]

        
        prev_silver_batch = etl_batch_history_df.filter(col("BatchName")==f"{new_source_name}_silverupsert").orderBy(col("BatchID").desc()).first()
        prev_silver_batch = etl_batch_history_df.filter(col("BatchName")==f"{new_source_name}_silverupsert").orderBy(col("BatchID").desc()).first()
        prev_dimfact_batch = etl_batch_history_df.filter(col("BatchName").contains("dimfact_laod")).orderBy(col("BatchID").desc()).first()
        pre_silver_batch_status = prev_silver_batch.Status
        pre_dimfact_batch_status = prev_dimfact_batch.Status

      

        display(latest_batch)
        
        if not latest_batch and load_type=='I':
            raise Exception("Incremental Batch cannot be started without full load completion")
        
        if latest_batch:
            print("Latest batch block is executed....")
            etl_batch_id = latest_batch.BatchID
            etl_batch_status = latest_batch.Status
            extract_window_start_date = latest_batch.ExtractWindowBeginTS
            extract_window_end_date = latest_batch.ExtractWindowEndTS
            batch_load_type = latest_batch.LoadType
        
            if batch_load_type == 'F' and load_type == 'I' and etl_batch_status not in ('SUCCESS'):
                raise Exception("Incremental Batch cannot be started without full load completion")

            if etl_batch_status == 'SUCCESS':
                if same_level_status=='SUCCESS':
                    pass
                if BatchName.lower().endswith("bronzeextraction"):
                    if pre_silver_batch_status=="Success" and pre_dimfact_batch_status=="Success":
                        print("Latest batch block with SUCCESS is executed....")
                        extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0] #minutes
                        if load_type == 'F':
                            extract_window_start_date=earliest_full_load_date
                        else:
                            extract_window_start_date=extract_window_end_date
                        etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
                        spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
                    VALUES ('{etl_batch_id_new}','{V_NewJobId}','{batch_name}','{current_system_date}',null,'{extract_window_start_date}','{current_system_date}','RUNNING','{load_type}','0')""")
                        etl_batch_id=etl_batch_id_new
                
                if BatchName.lower().endswith("silverupsert"):
                    if prev_dimfact_load_status=="Success":
                        print("Latest batch block with SUCCESS is executed....")
                        extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0] #minutes
                        if load_type == 'F':
                            extract_window_start_date=earliest_full_load_date
                        else:
                            extract_window_start_date=extract_window_end_date
                        etl_batch_id_new = etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0] + 1
                        spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
                    VALUES ('{etl_batch_id_new}','{V_NewJobId}','{batch_name}','{current_system_date}',null,'{extract_window_start_date}','{current_system_date}','RUNNING','{load_type}','0')""")
                        etl_batch_id=etl_batch_id_new
                
            else:     
                if etl_batch_status == 'RUNNING':
                    print("Latest batch block with RUNNING is executed....")
                    running_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).orderBy(col("BatchID").desc()).first()
                    etl_batch_id = running_batch.BatchID
                    etl_batch_status = running_batch.Status
                    extract_window_start_date = running_batch.ExtractWindowBeginTS
                    extract_window_end_date = running_batch.ExtractWindowEndTS
                    batch_load_type = running_batch.LoadType
                    extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0]
                    if load_type == 'F':
                        extract_window_start_date=earliest_full_load_date
                    else:
                        extract_window_start_date=extract_window_end_date
                    # extract_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(extract_window_start_date)
                    latest_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).filter(col("BatchId") == etl_batch_id)
                    etl_RetryAttempt_new = latest_batch.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
                            VALUES ('{etl_batch_id}','{V_NewJobId}','{batch_name}','{current_system_date}',null,'{extract_window_end_date}','{current_system_date}','RUNNING','{load_type}','{etl_RetryAttempt_new}')""")#use Bronze data extrcation start date of each source

                    etl_batch_id = etl_batch_id
                    etl_batch_status='RUNNING'
            
                elif etl_batch_status == 'CANCELLED':
                    print("Latest batch block with CANCELLED is executed....")
                    extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0]
                    if load_type == 'F':
                        extract_window_start_date=earliest_full_load_date
                    else:
                        extract_window_start_date=extract_window_end_date
                    # extract_window_start_date = when(load_type == 'F', earliest_full_load_date).otherwise(extract_window_start_date)
                    latest_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).filter(col("BatchId") == etl_batch_id)
                    etl_RetryAttempt_new = latest_batch.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
                            VALUES ('{etl_batch_id}','{batch_name}','{current_system_date}',null,'{extract_window_end_date}','{current_system_date}','RUNNING','{load_type}','{etl_RetryAttempt_new}')""")#use Bronze data extrcation start date of each source

                    etl_batch_id = etl_batch_id
                    etl_batch_status='RUNNING'

                elif etl_batch_status == 'FAILED':
                    print("Latest batch block with FAILED is executed....")
                    extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0]
                    if load_type == 'F':
                        extract_window_start_date=earliest_full_load_date
                    else:
                        extract_window_start_date=extract_window_end_date
                    latest_batch = etl_batch_history_df.filter(col("BatchName") == batch_name).filter(col("BatchId") == etl_batch_id)
                    print(latest_batch)
                    etl_RetryAttempt_new = latest_batch.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    print(etl_RetryAttempt_new)
                    spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
                    VALUES ('{etl_batch_id}','{V_NewJobId}','{batch_name}','{current_system_date}',null,'{extract_window_end_date}','{current_system_date}','RUNNING','{load_type}','{etl_RetryAttempt_new}')""")

                    etl_batch_status='RUNNING'
                    etl_batch_id = etl_batch_id

                else:
                    print("Latest batch block with else part is executed....")
                    extract_window_end_date=spark.sql(f"select DATEADD(Hour,-10,'{extract_window_end_date}') ").collect()[0][0]
                    if load_type == 'F':
                        extract_window_start_date=earliest_full_load_date
                    else:
                        extract_window_start_date=extract_window_end_date
                    etl_batch_history_df = etl_batch_history_df.withColumn("ExecuteBeginTS", current_system_date)
                    etl_batch_history_df = etl_batch_history_df.withColumn("Status", "RUNNING")
                    etl_batch_history_df = etl_batch_history_df.withColumn("ExecutionStatus", when(col("BatchID") == etl_batch_id, 0).otherwise(1))
                    etl_batch_id = etl_batch_id

        else:
            print("Final else block is executed....")
            display(etl_batch_history_df)
            etl_batch_id_new =etl_batch_history_df.agg({"BatchID": "max"}).collect()[0][0]
            if not etl_batch_id_new:
                etl_batch_id_new=1
            else:
                etl_batch_id_new+=1
            print(etl_batch_id_new)
            spark.sql(f"""INSERT INTO {V_BatchLogTable} (BatchID,JobId,BatchName,ExecuteBeginTS,ExecuteEndTS,ExtractWindowBeginTS,ExtractWindowEndTS,Status,LoadType,RetryAttempt)
            VALUES ('{etl_batch_id_new}','{V_NewJobId}','{batch_name}','{current_system_date}',null,'{earliest_full_load_date}','{current_system_date}','RUNNING','{load_type}','0')""")
            etl_batch_id=etl_batch_id_new
            extract_window_start_date=earliest_full_load_date
            extract_window_end_date=current_system_date
            etl_batch_status="RUNNING"

        return {"NewBatchId":etl_batch_id, "ExtractWindowsStartDate":str(extract_window_start_date), "ExtractWindowsEndDate":str(extract_window_end_date),"LoadType":load_type, "Status":etl_batch_status}
    except Exception as e:
        print(e)
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno


# COMMAND ----------

result=create_batch(BatchName,LoadType)
display(result["NewBatchId"])

# COMMAND ----------

import json
result["ErrorMessage"] = V_error_message
result["ErrorLine"] = V_error_line
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)
