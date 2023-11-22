# Databricks notebook source
dbutils.widgets.text("df","")
dbutils.widgets.text("dest_path","")

# COMMAND ----------

temp_view = dbutils.widgets.get("df")
dest_path = dbutils.widgets.get("dest_path")

# COMMAND ----------

src = spark.sql(f"select * from global_temp.{temp_view}")
src.display()

# COMMAND ----------

dest = spark.read.json(dest_path)
dest.display()

# COMMAND ----------

dest_schema = dest.select(*set(dest.columns).intersection(set(src.columns))).schema
dest_schema

# COMMAND ----------

src.schema

# COMMAND ----------

set(dest.columns).union(set(src.columns))

# COMMAND ----------

from pyspark.sql.functions import lit
# from pyspark.sql.type import s
src.withColumns({i:lit(None) for i in set(dest.columns) - set(src.columns)}).display()

# COMMAND ----------

schema = list(dest.schema)
for i in src.schema:
    if i.name not in [i.name for i in schema]:
        schema.append(i)
schema

# COMMAND ----------

spark.createDataFrame(src.withColumns({i:lit(None) for i in set(dest.columns) - set(src.columns)}).collect(), schema=schema).schema
