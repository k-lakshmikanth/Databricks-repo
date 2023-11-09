# Databricks notebook source
dbutils.widgets.text("BatchID","1")
dbutils.widgets.text("ErrorMessage","")


# COMMAND ----------

from datetime import datetime
V_BatchId = int(dbutils.widgets.get("BatchID").strip())
V_ErrorMessage = dbutils.widgets.get("ErrorMessage").strip()
V_CurrentSystemDate = datetime.now()
V_BatchLogTable="kpi_etl_analytics_conf.ETLBatchHistory"

# COMMAND ----------

#from pyspark.sql.functions import isNotNull
if len(V_ErrorMessage)>0:
    # Error case: Set STATUS to 'FAILED' and include the error message in the log table
    dml = "UPDATE " + str(V_BatchLogTable) + " SET ExecuteEndTS='" + str(V_CurrentSystemDate) + "', " + \
          "ExtractWindowEndTS='" + str(V_CurrentSystemDate) + "', STATUS='FAILED', " + \
          "ErrorDescription='" + str(V_ErrorMessage) + "' WHERE BatchID='" + str(V_BatchId) + "'"
else:
    # Success case: Set STATUS to 'SUCCESS'
    dml = "UPDATE " + str(V_BatchLogTable) + " SET ExecuteEndTS='" + str(V_CurrentSystemDate) + "', " + \
          "ExtractWindowEndTS='" + str(V_CurrentSystemDate) + "', STATUS='SUCCESS' " + \
          "WHERE BatchID='" + str(V_BatchId) + "'"

print(dml)  # Print the SQL statement for debugging
spark.sql(dml)  # Execute the SQL statement


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_etl_analytics_conf.ETLBatchHistory
