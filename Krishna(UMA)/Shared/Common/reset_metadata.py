# Databricks notebook source
dbutils.widgets.text("loadType","F")
# Define input widgets
dbutils.widgets.text("TableName", "")
dbutils.widgets.text("SourceName", "")


# COMMAND ----------

load_type=dbutils.widgets.get("loadType")
Src_nm = dbutils.widgets.get("SourceName")
tbl_nm = dbutils.widgets.get("TableName")
print(load_type)

# COMMAND ----------

if load_type== 'F':
        #Update the WatermarkValue in the metadata table for the next incremental load
        spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue='1900-01-01T00:00:00.000Z' , EffectiveDateColumnName='null' where TableName='{tbl_nm}' and SourceName='{Src_nm}' """)
