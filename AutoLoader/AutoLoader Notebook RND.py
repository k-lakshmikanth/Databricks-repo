# Databricks notebook source
spark.read.text("/mnt/auto-test/Source/user_json").display()

# COMMAND ----------

# MAGIC %fs ls /mnt/auto-test/Source/

# COMMAND ----------

raw_data_location = "/mnt/auto-test/Source"

# COMMAND ----------

spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "json")\
    .option("cloudFiles.schemaLocation", f"{raw_data_location}/inferred_schema")\
    .option("cloudFiles.inferColumnTypes", "true")\
    .load(raw_data_location+"/user_json")\
    .writeStream\
    .format("delta")\
    .option("checkpointLocation", raw_data_location+"/checkpoint")\
    .option("mergeSchema", "true")\
    .option("path","/mnt/auto-test/Target/")\
    .table("autoloader_demo_output")

# COMMAND ----------

spark.table("autoloader_demo_output").display()
