# Databricks notebook source
try:
  {
    dbutils.fs.unmount("/mnt/adls_landing/metadata")
  }
except:
  pass

# COMMAND ----------

try:
  {
    dbutils.fs.unmount("/mnt/adls_landing/bronze")
  }
except:
  pass

# COMMAND ----------

try:
  {
    dbutils.fs.unmount("/mnt/adls_landing/silver")
  }
except:
  pass

# COMMAND ----------

try:
  {
    dbutils.fs.unmount("/mnt/adls_landing/gold")
  }
except:
  pass

# COMMAND ----------

display(dbutils.fs.mounts())
