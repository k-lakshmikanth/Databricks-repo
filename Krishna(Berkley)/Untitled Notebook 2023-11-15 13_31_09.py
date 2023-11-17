# Databricks notebook source
data = [(1,'venkat'),(2,'chary')]
cols = ("sno","sname")

df = spark.createDataFrame(data,cols)

# COMMAND ----------

df.write.format("parquet").save("/mnt/adls_landing/testing/df")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

#dbutils.fs.mkdirs("/mnt/adls_landing/testing")

# COMMAND ----------

# MAGIC %fs ls /mnt/adls_landing/testing/df
