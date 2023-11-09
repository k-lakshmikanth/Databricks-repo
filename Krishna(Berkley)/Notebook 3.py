# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.adlskrishna1.dfs.core.windows.net",
  "MLO0vbHDaHRV9QrCj/J9X5iskna1GCe/ro6HF/lk/ZIh7lpAryaKwx71UYa5PgZCegLU2uxL2mn++AStgL9pGw==")

# COMMAND ----------

sqldwUsername = "sqladmin"
sqldwPassword = "Azure$2023"
sqldwHostname = "kr-az-sql-pool2.database.windows.net"
sqldwDatabase = "krishna-sqlpool"
sqldwPort = 1433
sqldwUrl = f"jdbc:sqlserver://{sqldwHostname}:{sqldwPort};database={sqldwDatabase};user={sqldwUsername};password={sqldwPassword}"

tempDir = "abfss://source@adlskrishna1.dfs.core.windows.net/"



def write_to_sqldw(table):
  """This code writes dataframe to sqldw. Using pre action which delete previous BU records ."""
  global sqldwUrl,tempDir,data_dict2

  preActionsQuery = f"""IF (EXISTS (SELECT * FROM sys.tables WHERE [name] = '{table}'))
    BEGIN
      DELETE FROM DBO.{table} WHERE BU in {tuple(i.BU for i in data_dict2[table].select('BU').distinct().collect())}
    END"""
  try:
    data_dict2[table]\
      .write\
      .format("com.databricks.spark.sqldw")\
      .option("url", sqldwUrl)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable", table)\
      .option("tempDir", tempDir)\
      .option("preActions", preActionsQuery if re.findall('_stg_h',table) != [] else "")\
      .mode("append")\
      .save()
    
    print(f"\t{table} : success")
  except Exception as e:
    print(f"\t{table} : failed\n\t\tError:{e}")

# COMMAND ----------

import pandas as pd
df = spark.read.jdbc(sqldwUrl,table="dbo.table")

# COMMAND ----------

temp = {"ID":[1,2,3],"NAME":["A","B","C"],"ISLATEST":[1,1,1]}

# COMMAND ----------

for i in range(3):
    print(f"Insert into dbo.TARGET({','.join(temp.keys())}) values {temp['ID'][i],temp['NAME'][i],temp['ISLATEST'][i]};")

# COMMAND ----------

print(f"Insert into dbo.table({','.join(temp.keys())}) values ()")

# COMMAND ----------

df.schema

# COMMAND ----------

postAction = """MERGE INTO [dbo].[TARGET] AS Target
USING [dbo].[SOURCE] AS Source
ON Source.ID = Target.ID
WHEN MATCHED THEN 
UPDATE SET
    Target.NAME = Source.NAME,
    Target.ISLATEST = 0;"""

# COMMAND ----------

df.write\
      .format("com.databricks.spark.sqldw")\
      .option("url", sqldwUrl)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable", "dbo.TARGET")\
      .option("tempDir", tempDir)\
      .option("postActions", postAction)\
      .mode("append")\
      .save()
