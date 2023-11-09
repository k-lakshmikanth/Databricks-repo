# Databricks notebook source
# Define input widgets
dbutils.widgets.text("TableName", "")
dbutils.widgets.text("SourceName", "")

Src_nm = dbutils.widgets.get("SourceName")
tbl_nm = dbutils.widgets.get("TableName")


# COMMAND ----------


#Update the WatermarkValue in the metadata table for the next incremental load
spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue=current_timestamp() where TableName='{tbl_nm}' and SourceName='{Src_nm}' """)

#Update the EffectiveDateColumnName in the metadata table for the next incremental load
spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set EffectiveDateColumnName='last_update_date' where TableName='{tbl_nm}' and SourceName='{Src_nm}'""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kpi_etl_analytics_conf.ctl_table_sync where TableName="Contact"
