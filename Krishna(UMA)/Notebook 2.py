# Databricks notebook source
df = spark.createDataFrame([{"Name":"LK"}])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

df.repartition(1).write.parquet("dbfs:/FileStore/df1.parquet",compression="uncompressed")

# COMMAND ----------

# MAGIC %fs ls /FileStore/df1.parquet
