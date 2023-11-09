# Databricks notebook source
try:
    result = dbutils.notebook.run("/Shared/logger/CreateJob",1000)
    display(result)
    import json
    result_dict = json.loads(result)
    V_NewJobId = result_dict.get("JobId")
    print(V_NewJobId)
except Exception as e:
    print(e)

# COMMAND ----------

import json
from pyspark.sql import Row

rows = spark.table("kpi_etl_analytics_conf.ctl_table_sync").rdd.collect()
# Convert Rows to dictionaries
dict_list = [row.asDict() for row in rows]

# Convert the list of dictionaries to JSON format
json_data = json.dumps(dict_list)

# Print the JSON data
print(json_data)

# COMMAND ----------

from datetime import datetime
result={"table_mapping": json_data,"current_datetime":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"JobId":V_NewJobId}
# Send the JSON string as output
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)
