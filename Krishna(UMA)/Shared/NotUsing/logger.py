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
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SQLContext

log_table = "KPI_ETL_PROCESS.ETL_BATCH_HISTORY"
upd_ins_flag = "I"
parent_batch_name = ""
parent_session_id = ""
session_id = "Run000"
task_name = "" 
execution_status = ""
start_time = str(datetime.now())
end_time = ""
batch_session_id=""
dml=""
batch_name=""
try:
  log_table = getArgument("log_table")
  session_id = getArgument("session_id")
  upd_ins_flag = getArgument("upd_ins_flag")
  execution_status = getArgument("execution_status")
except Exception as e:
  pass

try:
  parent_batch_name = getArgument("parent_batch_name")
  parent_session_id = getArgument("parent_session_id")
except Exception as e:
  pass

try:
  batch_name = getArgument("batch_name")
except Exception as e:
  pass

try:
  task_name = getArgument("task_name")
except Exception as e:
  pass

try:
  batch_session_id = getArgument("batch_session_id")
except Exception as e:
  pass

if(log_table == "KPI_ETL_PROCESS.ETL_BATCH_HISTORY" and upd_ins_flag == "I"):
  start_time = str(datetime.now())
  end_time = "" 
  dml = "INSERT INTO "+log_table+" VALUES('"+batch_name+"','"+session_id+"','"+parent_batch_name+"','"+parent_session_id+"','"+execution_status+"','"+start_time+"','"+end_time+"')"
elif(log_table == "KPI_ETL_PROCESS.ETL_PROCESS_HISTORY" and upd_ins_flag == "I"):
  start_time = str(datetime.now())
  end_time = ""
  dml = "INSERT INTO "+log_table+" VALUES('"+batch_session_id+"','"+task_name+"','"+session_id+"','"+execution_status+"','"+start_time+"','"+end_time+"')"
elif(log_table == "KPI_ETL_PROCESS.ETL_BATCH_HISTORY" and upd_ins_flag == "U"):
  end_time = str(datetime.now())
  dml = "UPDATE "+log_table+" SET Batch_End_Date='"+end_time+"',BATCH_EXECUTION_STATUS='"+execution_status+"' WHERE session_id='"+session_id+"'"
elif(log_table == "KPI_ETL_PROCESS.ETL_PROCESS_HISTORY" and upd_ins_flag == "U"):
  end_time = str(datetime.now())
  dml = "UPDATE "+log_table+" SET Task_End_Date='"+end_time+"',TASK_EXECUTION_STATUS='"+execution_status+"' WHERE session_id='"+session_id+"'"  
#print(dml)

if(upd_ins_flag=='I'):
  #con.cursor().execute(dml)
  #df = spark.write.jdbc(url=jdbcUrl, table=dml, properties=connectionProperties)
  #databaseEngine.execute(dml)
  dbutils.notebook.run("../../util/logger/SQLPushdown",60,{"dml": dml})
elif(upd_ins_flag=='U'):
  dbutils.notebook.run("../../util/logger/SQLPushdown",60,{"dml": dml})
  #sqlContext.sqlDBQuery(connectionProperties)


