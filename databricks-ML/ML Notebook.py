# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/credit-card-fraud/

# COMMAND ----------

ccf_df = spark.read.format("parquet").load("dbfs:/databricks-datasets/credit-card-fraud/data")
ccf_df.display()
