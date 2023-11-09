# Databricks notebook source
query = f"""
        SELECT MAX(BatchID) as ETLBatchID, 
               COALESCE(MAX(Status), 'FAILED') as ETLBatchStatus,
               MAX(ExtractWindowBeginTS) as ExtractWindowStartDate, 
               MAX(ExtractWindowEndTS) as ExtractWindowEndDate,
               LoadType
        FROM kpi_etl_analytics_conf.etlbatchhistory
        WHERE BatchName = 'dsd'
        GROUP BY LoadType
    """
result = spark.sql(query).collect()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kpi_etl_analytics_conf.etlbatchhistory

# COMMAND ----------

display(spark.table("kpi_etl_analytics_conf.etlbatchhistory"))

# COMMAND ----------

# %sql

# delete from  kpi_cloud_sl.hed_contact where Hed_contactID_c='1TR001'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kpi_cloud_bl.source1_contact
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kpi_cloud_bl.source1_contact;
# MAGIC
# MAGIC -- select * from kpi_cloud_bl.source2_contact;
# MAGIC
# MAGIC -- select * from kpi_cloud_sl.hed_contact;
# MAGIC
# MAGIC select * from kpi_cloud_gl.dim_contact;
# MAGIC
# MAGIC -- select * from kpi_etl_analytics_conf.etlbatchhistory;
# MAGIC
# MAGIC -- select * from kpi_etl_analytics_conf.etlprocesshistory;
# MAGIC
# MAGIC
# MAGIC -- select * from kpi_etl_analytics_conf.ctl_table_sync;
# MAGIC
# MAGIC -- select * from kpi_etl_analytics_conf.ctl_bl_sl_mapping;
# MAGIC
# MAGIC -- select * from kpi_etl_analytics_conf.ctl_table_dims_facts;
