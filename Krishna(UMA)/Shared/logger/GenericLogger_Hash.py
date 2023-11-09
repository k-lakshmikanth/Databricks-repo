# Databricks notebook source
######################################################################################################################################################
'''
Author:           KPI Partners
Purpose:          The notebooks to write logs to log target 
Description:
The notebooks to write logs to log target 
'''
######################################################################################################################################################
import json
import sys
dbutils.widgets.text("BatchId","")
dbutils.widgets.text("ExecutionId","")
dbutils.widgets.text("JobId","")
dbutils.widgets.text("ErrorMessage","")
dbutils.widgets.text("ErrorLine","")
dbutils.widgets.text("InsertedCount","")
dbutils.widgets.text("UpdatedCount","")
dbutils.widgets.text("DestTableName","")
dbutils.widgets.text("DestSchema","")
dbutils.widgets.text("TimeStamp","")
dbutils.widgets.text("TimetakenInSecs","")
dbutils.widgets.text("Ind","Batch")
dbutils.widgets.text("BatchName","")
dbutils.widgets.text("LoadType","")

#Deriving the Variables
V_BatchId = dbutils.widgets.get("BatchId").strip()
V_JobId = dbutils.widgets.get("JobId").strip()
V_BatchName = dbutils.widgets.get("BatchName")
V_BatchName = V_BatchName.replace("''", '')
V_LoadType = dbutils.widgets.get("LoadType")
V_LoadType = V_LoadType.replace("''", '')
V_Ind = dbutils.widgets.get("Ind")
V_BatchId = V_BatchId.replace("''", '')
V_ExecutionId = dbutils.widgets.get("ExecutionId").strip()
V_ExecutionId = V_ExecutionId.replace("''", '')
V_ErrorMessage = dbutils.widgets.get("ErrorMessage").strip()
V_ErrorMessage = V_ErrorMessage.replace("''", '')
V_TimeStamp = dbutils.widgets.get("TimeStamp").strip()
V_TimeStamp = V_TimeStamp.replace("''", '')
V_TableName = dbutils.widgets.get("DestTableName").strip()
V_TableName = V_TableName.replace("''", '')
V_DestName = dbutils.widgets.get("DestSchema").strip()
V_DestName = V_DestName.replace("''", '')
V_TimetakenInSecs = dbutils.widgets.get("TimetakenInSecs")
V_ErrorLine = dbutils.widgets.get("ErrorLine")
display(V_BatchName)
V_ErrorLine = V_ErrorLine.replace("''", '')
V_InsertedCount = dbutils.widgets.get("InsertedCount")
V_InsertedCount = V_InsertedCount.replace("''", '')
V_UpdatedCount = dbutils.widgets.get("UpdatedCount")
V_UpdatedCount = V_UpdatedCount.replace("''", '')
display(V_BatchId)
V_error_message = ""
V_error_line=""
V_NewExecutionId = 0
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib
def get_md5_hash(*args):
    concatenated_str = "".join(str(arg) for arg in args)
    md5_hash = hashlib.md5(concatenated_str.encode()).hexdigest()
    return md5_hash

V_ErrorMessage = V_ErrorMessage.replace("'","''")
V_NewJobId=0
result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM {V_JobLogTable}""")
V_NewJobId = result_df.collect()[0]['JobId_NEW']
print(V_NewJobId)
print(V_Ind)
print(V_ExecutionId)
print(V_ErrorMessage)
try:
    if V_Ind=='Process':
        if V_ExecutionId:
            if V_ErrorMessage:
                V_InsertedCount = 0
                V_UpdatedCount = 0
                # Failure case: Set STATUS to 'FAILED' 
                # Failure case: Set STATUS to 'FAILED'
                UpdateStmt = "UPDATE " + V_ProcessLogTable + " SET ExecutionEndTime='" + str(V_TimeStamp) + "', TargetInsertRecCount='" + str(V_InsertedCount) + "', TargetUpdateRecCount='" + str(V_UpdatedCount) + "', TimeTakenInSecs='" + str(V_TimetakenInSecs) + "', STATUS='FAILED', ErrorDescription='" + str(V_ErrorMessage) + "', ErrorLine='" + str(V_ErrorLine) + "' WHERE ExecutionId='" + str(V_ExecutionId) + "' and TableName='" + str(V_DestName) + "." + str(V_TableName) + "'"
            else:
                # Success case: Set STATUS to 'SUCCESS'
                FinalCountdf = spark.sql(f"""select count(*) as count from {V_DestName}.{V_TableName}""")
                V_FinalTargetCount = FinalCountdf.collect()[0]['count']
                display(V_FinalTargetCount)
                UpdateStmt = "UPDATE " + V_ProcessLogTable + " SET ExecutionEndTime='" + str(V_TimeStamp) + "', TargetInsertRecCount='" + str(V_InsertedCount) + "', TargetUpdateRecCount='" + str(V_UpdatedCount) + "', TimeTakenInSecs='" + str(V_TimetakenInSecs) + "', FinalTargetRecCount = '"+str(V_FinalTargetCount)+"', STATUS='SUCCESS' WHERE ExecutionId='" + str(V_ExecutionId) + "' and TableName='" + str(V_DestName) + "." + str(V_TableName) + "'"
            display(UpdateStmt)
            spark.sql(UpdateStmt)  
        else:
            print("Creating new process!")
            if V_LoadType=='F':
                V_InitialTargetCount=0
            else:
                InitialCountdf = spark.sql(f"""select count(*) as count from {V_DestName}.{V_TableName}""")
                V_InitialTargetCount = InitialCountdf.collect()[0]['count']
            V_DestTableName = V_DestName+'.'+V_TableName
            
            # Execute the SQL query to get the new ExecutionId
            V_NewExecutionId = get_md5_hash(V_TimeStamp,V_DestTableName)
            display(V_NewExecutionId)
            status = spark.sql(f"""select status from kpi_etl_analytics_conf.etl_batch_history_test where BatchId ='{V_BatchId}' order by ExecuteBeginTS desc limit 1""").collect()[0][0]
            print(status)
            if status == 'RUNNING':
                #Make an entry in ProcessHistory table
                InsertStmt = f"INSERT INTO {V_ProcessLogTable} VALUES ('{V_NewExecutionId}', '{V_BatchId}',{V_NewJobId},'{str(V_TimeStamp)}', NULL, '{V_DestTableName}', 'RUNNING', 0, 0, {V_InitialTargetCount}, 0, 0, NULL, NULL)"
                display(InsertStmt)
                spark.sql(InsertStmt)
    else:
        if V_BatchId:
            if len(V_ErrorMessage)>0:
                # Error case: Set STATUS to 'FAILED' and include the error message in the log table
                error = spark.sql(f"""select ErrorDescription as error from {V_BatchLogTable} where BatchId ='{V_BatchId}' and ErrorDescription is not null order by RetryAttempt """)
                if error.count()>0:
                    display(error)
                else:
                    dml = (
                    "UPDATE " + str(V_BatchLogTable) + " SET ExecuteEndTS='" + str(V_TimeStamp) + "', STATUS='FAILED', ErrorDescription='" + str(V_ErrorMessage) + "' WHERE BatchID='"
                    + str(V_BatchId) + "' AND RetryAttempt = (" + "SELECT MAX(RetryAttempt) FROM " + str(V_BatchLogTable) + " WHERE BatchId = '" + V_BatchId + "') AND BatchName = '" + str(V_BatchName) + "'")
            else:
                dml = f"""
                UPDATE {V_BatchLogTable} SET ExecuteEndTS='{V_TimeStamp}',
                STATUS='SUCCESS'
                WHERE BatchID='{V_BatchId}' AND ErrorDescription IS NULL AND Batchname = '{V_BatchName}' AND RetryAttempt = (select max(RetryAttempt) from {V_BatchLogTable} where BatchId='{V_BatchId}' AND Batchname = '{V_BatchName}' )"""
            display(dml)
            spark.sql(dml) 
except Exception as e:
        print(e)
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno


# COMMAND ----------

import json
result={"ExecutionId": V_NewExecutionId, "ErrorLine": V_error_line, "ErrorMessage":V_error_message}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)

# COMMAND ----------

df=spark.sql("select * from kpi_etl_analytics_conf.etl_batch_history_test")
#.filter("BatchName='source2_SilverUpsert'")
display(df)

# COMMAND ----------

df=spark.sql("select * from kpi_etl_analytics_conf.etl_process_history_test")
#.filter("TableName='kpi_cloud_sl.Hed_Contact' and BatchID IN(2,5)")
display(df)


# COMMAND ----------

df=spark.sql("select * from kpi_etl_analytics_conf.etl_job_history")
#.filter("TableName='kpi_cloud_sl.Hed_Contact' and BatchID IN(2,5)")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ErrorDescription AS Error
# MAGIC         FROM kpi_etl_analytics_conf.etl_batch_history_test
# MAGIC         WHERE JobId = 3 
# MAGIC         ORDER BY ExecuteBeginTS DESC LIMIT 1
