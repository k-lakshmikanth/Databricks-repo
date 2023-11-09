# Databricks notebook source
# Getting access key of ADLS using scope and storing it in variable
ACCESS_KEY = "MLO0vbHDaHRV9QrCj/J9X5iskna1GCe/ro6HF/lk/ZIh7lpAryaKwx71UYa5PgZCegLU2uxL2mn++AStgL9pGw=="# dbutils.secrets.get(scope = "keyvaultscope" , key = "kpidatalakestoreAcesskey")

# COMMAND ----------

try:
    {
        #dbutils.fs.unmount("/mnt/adls_landing/metadata")
        dbutils.fs.mount(
        source="wasbs://metadata@adlskrishna1.blob.core.windows.net",
        mount_point="/mnt/adls_landing/metadata",
        extra_configs={"fs.azure.account.key.adlskrishna1.blob.core.windows.net":ACCESS_KEY}
        )
    }
except:
  pass

# COMMAND ----------


# Mount ADLS Container to local mount point using access key ID and secret access key
try:
    {
        #dbutils.fs.unmount("/mnt/adls_landing/bronze")
        dbutils.fs.mount(
        source="wasbs://bronze@adlskrishna1.blob.core.windows.net",
        mount_point="/mnt/adls_landing/bronze",
        extra_configs={"fs.azure.account.key.adlskrishna1.blob.core.windows.net":ACCESS_KEY}
        )
    }
except:
  pass

# COMMAND ----------

# Mount ADLS Silver Container to local mount point using access key ID and secret access key
try:
    {
        #dbutils.fs.unmount("/mnt/adls_landing/silver")
        dbutils.fs.mount(
        source="wasbs://silver@adlskrishna1.blob.core.windows.net",
        mount_point="/mnt/adls_landing/silver",
        extra_configs={"fs.azure.account.key.adlskrishna1.blob.core.windows.net":ACCESS_KEY}
        )
    }
except:
  pass

# COMMAND ----------

# Mount ADLS Gold Container to local mount point using access key ID and secret access key
try:
    {
        #dbutils.fs.unmount("/mnt/adls_landing/gold")
        dbutils.fs.mount(
        source="wasbs://gold@adlskrishna1.blob.core.windows.net",
        mount_point="/mnt/adls_landing/gold",
        extra_configs={"fs.azure.account.key.adlskrishna1.blob.core.windows.net":ACCESS_KEY}
        )
    }
except:
  pass

# COMMAND ----------

# MAGIC %fs ls /mnt/adls_landing/
