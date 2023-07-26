# Databricks notebook source
base_url = dbutils.widgets.get("Base_url")
relative_url = "/api/2.1/jobs/list"

# COMMAND ----------

from requests import get

request = get(base_url+relative_url,\
    headers = {"Authorization":f"Bearer {dbutils.widgets.get('Access_token')}"
})

# COMMAND ----------

df = spark.createDataFrame([[i["job_id"],i["settings"]["name"],i["created_time"]] for i in request.json()["jobs"]],schema=["job_id","name","created_time"])

# COMMAND ----------

from pyspark.sql.functions import max
dbutils.notebook.exit(df.filter(df.created_time == df.select(max("created_time").alias("latest_created_time")).collect()[0].latest_created_time).collect()[0].asDict())
