# Databricks notebook source
pip install azure-identity azure-ai-ml mltable -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azure.ai.ml import MLClient

# COMMAND ----------

try:
    credential = DefaultAzureCredential()
    # Check if given credential can get token successfully.
    credential.get_token("https://management.azure.com/.default")
except Exception as ex:
    # Fall back to InteractiveBrowserCredential in case DefaultAzureCredential not work
    credential = InteractiveBrowserCredential()

# COMMAND ----------

ml_client = MLClient(credential=credential,subscription_id='2bcf5e1a-2a28-4ce2-8082-e04bec437ea0',resource_group_name="lk-az-ml-ws-rg",workspace_name="lk-az-mlstudio")
ml_client

# COMMAND ----------

print('subscription_id="2bcf5e1a-2a28-4ce2-8082-e04bec437ea0"\nworkspace_name="lk-az-mlstudio"\nresource_group="lk-az-ml-ws-rg"')

# COMMAND ----------

datastores = [i for i in ml_client.datastores.list()]
for datastore in datastores:
    print(datastore)

# COMMAND ----------

from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes

my_path = 'https://lkmlwsadls.blob.core.windows.net/traindatasets?sp=r&st=2023-08-23T04:50:41Z&se=2023-08-23T12:50:41Z&spr=https&sv=2022-11-02&sr=c&sig=pNeivABbRhlDC0w6T8AAS3fKxpPHWds4Cb1ttrpznE8%3D'

my_data = Data(
    path=my_path,
    type=AssetTypes.URI_FOLDER,
    description="Adls sas generated folder",
    name="traindatasets"
)

# ml_client.data.create_or_update(my_data)


# COMMAND ----------

print("data of aml studio:\n")
print(*[i for i in ml_client.data.list()],sep="\n---------------------------------------------------\n")

# COMMAND ----------

print(dbutils.fs.ls("/mnt/traindatasets/")[0].path)

# COMMAND ----------

path ="dbfs:/mnt/traindatasets/diabetes_prediction_dataset.csv"

my_data = Data(
    path=my_path,
    type=AssetTypes.URI_FILE,
    description="file uploaded from DBFS",
    name="diabetes_prediction_dataset"
)

if my_data.name not in [i.name for i in ml_client.data.list()]:
    ml_client.data.create_or_update(my_data)
else:
    print(f"{my_data.name} already exists")

# COMMAND ----------

my_data.name

# COMMAND ----------

print(ml_client.data.get(my_data.name,version=1))

# COMMAND ----------

spark.read.csv(path,header=True,inferSchema=True).display()

# COMMAND ----------

import mltable

# COMMAND ----------

path

# COMMAND ----------

import Ao
