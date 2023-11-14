# Databricks notebook source
src_df = spark.read.csv("dbfs:/FileStore/Source.csv", inferSchema = True,header=True)
trg_df = spark.read.csv("dbfs:/FileStore/target.csv", inferSchema = True,header=True)

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

for i in src_df.collect():
    trg_df = trg_df.withColumn("IsLatest",when(trg_df.id == i.id,0).otherwise(trg_df.IsLatest))
    i = i.asDict()
    i["IsLatest"] = 1
    trg_df = trg_df.union(spark.createDataFrame([i]).select(trg_df.columns))
trg_df.display()

# COMMAND ----------


