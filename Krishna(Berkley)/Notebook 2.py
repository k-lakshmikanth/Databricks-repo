# Databricks notebook source
layer = "raw"
mount_point = f"/mnt/adlskrishna2023/bdw/{layer}"
Last_modified_time = 1698225978000

# COMMAND ----------

from pyspark.sql.functions import lit
data_dict1 = {}

print(f"Layer : {layer}")
for BU in dbutils.fs.ls(mount_point):
    print(f"\tBusiness Unit: {BU.name[:-1]}")
    data_dict1[BU.name[:-1]] = {}
    for table in dbutils.fs.ls(BU.path):
        print(f"\t\tTable : {table.name[:-1]}")
        # read the dataframe from path and add required columns and replace spark.createDataFrame with the dataframe

        data_dict1[BU.name[:-1]][table.name[:-1]] = spark.read.parquet(*[f.path for f in dbutils.fs.ls(table.path) if f.modificationTime > Last_modified_time]).withColumn("BU",lit(BU.name[:-1]))

# COMMAND ----------

dbutils.fs.ls(BU.path)

# COMMAND ----------

data_dict1

# COMMAND ----------

data_dict2 = {}
for BU in data_dict1:
    for table in data_dict1[BU]:
        try:
            data_dict2[table] = data_dict2[table].union(data_dict1[BU][table])
        except:
            data_dict2[table] = data_dict1[BU][table]

# COMMAND ----------

data_dict2

# COMMAND ----------

data_dict2["contracts_h"].display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlskrishna2023/bdw/raw/bcm/
