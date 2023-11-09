# Databricks notebook source
dim_fact_table=spark.sql("select distinct Source_table,Source_Schema,Target_Table,Target_Schema from kpi_etl_analytics_conf.ctl_table_dims_facts order by Target_Table").toJSON().collect()


# COMMAND ----------

dim_fact_table


# COMMAND ----------


dbutils.notebook.exit(dim_fact_table)
