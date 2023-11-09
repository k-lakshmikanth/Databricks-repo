# Databricks notebook source
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

# COMMAND ----------

from pyspark.sql.functions import col
import sys
V_error_message=''
V_error_line=''
def create_job():
    try:
        result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM {V_JobLogTable}""")
        V_JobId = result_df.collect()[0]['JobId_NEW']
        print(V_JobId)
        if V_JobId is None:
            V_NewJobId = 1
            current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate
            insert_query = f"""
                    INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                    VALUES ('{V_NewJobId}', '{current_system_date}','RUNNING', '0')
                """
            print(insert_query)
            spark.sql(insert_query)
            etl_job_id = V_NewJobId #Convert to string before printing
        else:
            # Get the current system date and time
            current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate
            etl_job_history_df = spark.table(V_JobLogTable)
            latest_Job = etl_job_history_df.filter(col("JobId") == V_JobId).orderBy(col("JobId").desc()).first()
            display(latest_Job)
                
            if latest_Job:
                print("Latest job block is executed....")
                etl_job_id = latest_Job.JobID
                etl_job_status = latest_Job.Status
                print(etl_job_status)

                if etl_job_status == 'SUCCESS':
                    print("Latest job block with SUCCESS is executed....")
                    etl_job_id_new = etl_job_history_df.agg({"JobId": "max"}).collect()[0][0] + 1
                    spark.sql(f"""INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                            VALUES ('{etl_job_id_new}','{current_system_date}','RUNNING','0')""")
                    etl_batch_id=etl_job_id_new

                if etl_job_status == 'RUNNING':
                    print("Latest job block with RUNNING is executed....")
                    running_job = etl_job_history_df.filter(col("JobId") == etl_job_id)
                    print(running_job)
                    etl_RetryAttempt_new = running_job.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    print(etl_RetryAttempt_new)
                    spark.sql(f"""INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                        VALUES ('{etl_job_id}','{current_system_date}','RUNNING','{etl_RetryAttempt_new}')""")

                    etl_job_status='RUNNING'
                    etl_job_id = etl_job_id

                elif etl_job_status == 'FAILED':
                    print("Latest job block with FAILED is executed....")
                    latest_job = etl_job_history_df.filter(col("JobId") == etl_job_id)
                    print(latest_job)
                    etl_RetryAttempt_new = latest_job.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    print(etl_RetryAttempt_new)
                    spark.sql(f"""INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                        VALUES ('{etl_job_id}','{current_system_date}','RUNNING','{etl_RetryAttempt_new}')""")

                    etl_job_status='RUNNING'
                    etl_job_id = etl_job_id

                elif etl_job_status == 'CANCELLED':
                    print("Latest job block with CANCELLED is executed....")
                    latest_job = etl_job_history_df.filter(col("JobId") == etl_job_id)
                    etl_RetryAttempt_new = latest_job.agg({"RetryAttempt": "max"}).collect()[0][0] + 1
                    spark.sql(f"""INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                                        VALUES ('{etl_job_id}','{current_system_date}','RUNNING','{etl_RetryAttempt_new}')""")

                    etl_job_id = etl_job_id
                    etl_job_status='RUNNING'
            else:
                print("Final else job block is executed....")
                etl_job_id_new = etl_job_history_df.agg({"JobId": "max"}).collect()[0][0] + 1
                spark.sql(f"""INSERT INTO {V_JobLogTable} (JobID,JobStartTime,Status,RetryAttempt)
                VALUES ('{etl_job_id_new}','{current_system_date}','RUNNING','0')""")
                etl_batch_id=etl_job_id_new
        return {"JobId":etl_job_id}
    except Exception as e:
        print(e)
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno
        


# COMMAND ----------

import json
# Call the function
created_job_id = create_job()
print("Created Job ID:", created_job_id)

# COMMAND ----------

created_job_id["ErrorMessage"] = V_error_message
created_job_id["ErrorLine"] = V_error_line
result_json = json.dumps(created_job_id)
dbutils.notebook.exit(result_json)
