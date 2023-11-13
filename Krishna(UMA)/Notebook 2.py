# Databricks notebook source
df = spark.createDataFrame([{"Name":"LK","IS_LATEST":"1"},{"Name":"LUCKY","IS_LATEST":"1"}])
df1 = spark.createDataFrame([{"Name":"LUCKY"}])

# COMMAND ----------

df.display()
df1.display()

# COMMAND ----------

df.createOrReplaceTempView("a.df")
df1.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO DF 
# MAGIC USING DF1
# MAGIC ON DF.NAME = DF1.NAME
# MAGIC WHEN MATCHED THEN
# MAGIC   BEGIN
# MAGIC   UPDATE SET IS_LATEST = 0
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

df.repartition(1).write.parquet("dbfs:/FileStore/df1.parquet",compression="uncompressed")

# COMMAND ----------

# MAGIC %fs ls /FileStore/df1.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.kpi_etl_analytics_conf.etl_batch_history_test WHERE STATUS='SUCCESS'

# COMMAND ----------

df.repartition(1).write.json("bronze/")
dbutils.fs.ls("bronze")

# COMMAND ----------

# MAGIC %fs ls /df.json

# COMMAND ----------

spark.sql('SELECT * FROM kpi_etl_analytics_conf.ctl_bl_sl_mapping').columns
