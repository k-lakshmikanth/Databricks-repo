# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes<br>
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/security/secrets/example-secret-workflow

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

secret = dbutils.secrets.get(scope = "SatheeshKumar-KV", key = "az-adls-mnt-sec")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "2cfd03fd-2155-49c0-bcca-5b4c7c52c313",
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/841d5291-c0fb-4516-8fca-f429875be39d/oauth2/token"}

# COMMAND ----------

 dbutils.fs.mount(
  source = "abfss://lucky@satheeshthatoju.dfs.core.windows.net/",
  mount_point = "/mnt/lucky",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/lucky

# COMMAND ----------

# dbutils.fs.unmount("/mnt/lucky")
