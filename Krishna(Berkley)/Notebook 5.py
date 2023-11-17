# Databricks notebook source
data = [(1,'venkat'),(2,'chary')]
cols = ("sno","sname")

df = spark.createDataFrame(data,cols)

# COMMAND ----------

df.write.format("parquet").save("/mnt/adls_landing/testing/df")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %fs ls /mnt/adls_landing

# COMMAND ----------

df.toPandas().to_json("/dbfs/mnt/adls_landing/bronze/df_json.json",orient="records")

# COMMAND ----------

# MAGIC %fs head /mnt/adls_landing/bronze/df_json.json

# COMMAND ----------

#dbutils.fs.mkdirs("/mnt/adls_landing/testing")

# COMMAND ----------

# MAGIC %fs ls /mnt/adls_landing/testing/df
