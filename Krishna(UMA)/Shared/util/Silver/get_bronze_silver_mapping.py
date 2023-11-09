# Databricks notebook source
import json
from pyspark.sql import Row
rows = spark.sql(f"""select distinct Src_nm,Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm from kpi_etl_analytics_conf.ctl_bl_sl_mapping""").rdd.collect()

# Convert Rows to dictionaries
dict_list = [row.asDict() for row in rows]

# Convert the list of dictionaries to JSON format
json_data = json.dumps(dict_list)

# Print the JSON data
print(json_data)

# COMMAND ----------

json_data

# COMMAND ----------

from datetime import datetime
result={"table_mapping": json_data,"current_datetime":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# Send the JSON string as output
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)
