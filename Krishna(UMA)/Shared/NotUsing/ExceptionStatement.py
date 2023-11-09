# Databricks notebook source
dbutils.widgets.text("BatchId","")
dbutils.widgets.text("ExecutionId","")
dbutils.widgets.text("Error_Message","")
dbutils.widgets.text("Error_LineNo","")
dbutils.widgets.text("EndTime","")
dbutils.widgets.text("TimeTaken","")

# COMMAND ----------

V_BatchId = dbutils.widgets.get("BatchId")
V_ExecutionId = dbutils.widgets.get("ExecutionId")
V_Error_Message = dbutils.widgets.get("Error_Message")
V_Error_LineNo = dbutils.widgets.get("Error_LineNo")
V_EndTime = dbutils.widgets.get("EndTime")
V_TimeTaken = dbutils.widgets.get("TimeTaken")
