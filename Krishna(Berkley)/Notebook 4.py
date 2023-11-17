# Databricks notebook source
from pyspark.sql.functions import when

# COMMAND ----------

# src_df = spark.read.csv("dbfs:/FileStore/Source.csv", inferSchema = True,header=True)
import pandas as pd

src_df = pd.read_csv("/dbfs/FileStore/Source.csv", header=0)
trg_df = spark.read.csv("dbfs:/FileStore/target.csv", inferSchema = True,header=True)

# COMMAND ----------

# src_df = src_df.union(spark.createDataFrame([{"id":1, "name":"Ammu"}]))
src_df.display()

# COMMAND ----------

src_df.to_dict(orient="list")

# COMMAND ----------

for i in src_df.collect():
    trg_df = trg_df.withColumn("IsLatest",when(trg_df.id == i.id,0).otherwise(trg_df.IsLatest))
    i = i.asDict()
    i["IsLatest"] = 1
    trg_df = trg_df.union(spark.createDataFrame([i]).select(trg_df.columns))
trg_df.display()

# COMMAND ----------

# MAGIC %fs ls /mnt/source/df/

# COMMAND ----------

trg_df.coalesce(1).write.csv("/mnt/source/df",header=True, mode="overwrite")
