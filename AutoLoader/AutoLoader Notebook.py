# Databricks notebook source
# MAGIC %md
# MAGIC # Onboard data from ADLS
# MAGIC *https://docs.databricks.com/en/ingestion/onboard-data.html#language-python*

# COMMAND ----------

# MAGIC %fs ls /mnt/auto-test/

# COMMAND ----------

@dlt.table(table_properties={'quality': 'bronze'})
def Temp():
  return (
     spark.readStream.format('cloudFiles')
     .option('cloudFiles.format', 'json')
     .load(f'/mnt/auto-test/Source/user_json/')
 )
