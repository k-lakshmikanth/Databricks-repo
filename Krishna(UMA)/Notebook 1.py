# Databricks notebook source
spark.sql("SELECT current_timestamp() AS CURRENTSYSTEMDATE").first().CURRENTSYSTEMDATE

# COMMAND ----------

from datetime import datetime
spark.table(f"{DestinationSchema}.{src_name}_{table_name}").write.json(f"{abfss_path}/{src_name}/in/{LoadType}/{datetime.now().strftime(f'%Y/%m/%d/{table_name}_%m%d%Y_%H%M%S.json')}")
