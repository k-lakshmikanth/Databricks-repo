# Databricks notebook source
src_name = "dbo"
table_name = "class"

# COMMAND ----------

# MAGIC %run /Shared/Common/src_connection

# COMMAND ----------

V_Query = spark.sql(f"""select ColumnList from kpi_etl_analytics_conf.ctl_table_sync where TableName='{table_name}' and SourceName='{src_name}' """).collect()[0][0]
print(V_Query)

# COMMAND ----------

query = f"({V_Query}) AS custom_query" #  where lastLoadDate >= '{V_WatermarkVlaue}' 
source_df = spark.read.jdbc(url=jdbcurl, table=query, properties=connectionProperties)

# COMMAND ----------

source_df.columns

# COMMAND ----------

print(f"/mnt/adls_landing/bronze/{src_name}_{table_name}")

# COMMAND ----------

spark.read.json(f"/mnt/adls_landing/bronze/{src_name}_{table_name}").columns

# COMMAND ----------

dest_schema = spark.read.json(f"/mnt/adls_landing/bronze/{src_name}_{table_name}").select(*source_df.columns).schema

# COMMAND ----------

spark.createDataFrame(source_df.collect(), schema=dest_schema).display()

# COMMAND ----------

[for i in ]

# COMMAND ----------

dest_schema

# COMMAND ----------

source_df.createOrReplaceGlobalTempView("df")

# COMMAND ----------

dbutils.notebook.run("/Repos/lkoct2.2022@gmail.com/Databricks-repo/Krishna(UMA)/Shared/util/Bronze/Notebook 2",60,{"df":"df","dest_path":f"/mnt/adls_landing/bronze/{src_name}_{table_name}"})

# COMMAND ----------


