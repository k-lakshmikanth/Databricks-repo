# Databricks notebook source
# MAGIC %md
# MAGIC # Onboard data from ADLS
# MAGIC *https://docs.databricks.com/en/ingestion/onboard-data.html#language-python*

# COMMAND ----------

# MAGIC %fs ls /mnt/auto-test/

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table(table_properties={'quality': 'bronze'})
def Temp():
  return (
     spark.readStream.format('cloudFiles')
     .option('cloudFiles.format', 'json')
     .load(f'/mnt/auto-test/Source/user_json/')
 )

# COMMAND ----------

# Add
#     "node_type_id": "Standard_DS3_v2",
#     "driver_node_type_id": "Standard_DS3_v2",
# And remove
# any other Attributes  otherthan num_workers
