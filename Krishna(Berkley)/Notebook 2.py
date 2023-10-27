# Databricks notebook source
layer = "raw"
mount_point = f"/mnt/adlskrishna23/bdw/{layer}"

Last_modified_time = spark.sql("SELECT MAX(MODIFICATIONTIME) as Last_modified_time FROM FILE_LOAD_LOG").collect()[0].Last_modified_time
Last_modified_time = 0 if Last_modified_time == None else Last_modified_time

print(f"Last Modified Time : {Last_modified_time}")

# COMMAND ----------

import re
path_to_id = lambda path: re.sub("/|_","-",path[len(mount_point)+6:-8]) # this function is used in cmd 3 used to record log

# COMMAND ----------

from pyspark.sql.functions import lit
data_dict1 = {}
logs = []

print(f"Layer : {layer}")
for BU in dbutils.fs.ls(mount_point):
    print(f"\tBusiness Unit: {BU.name[:-1]}")
    data_dict1[BU.name[:-1]] = {}
    for table in dbutils.fs.ls(BU.path):
        print(f"\t\tTable : {table.name[:-1]}")
        try:
            data_dict1[BU.name[:-1]][table.name[:-1]] = spark.read.parquet(*[f.path for f in dbutils.fs.ls(table.path) if f.modificationTime > Last_modified_time]).withColumn("BU",lit(BU.name[:-1]))

            logs.extend([(path_to_id(f.path),f.path,f.modificationTime,"PASSED") for f in dbutils.fs.ls(table.path) if f.modificationTime > Last_modified_time])
            
        except Exception as e:
            logs.extend([(path_to_id(f.path),f.path,f.modificationTime,e.args[0]) for f in dbutils.fs.ls(table.path) if f.modificationTime > Last_modified_time])


# COMMAND ----------

data_dict2 = {}
for BU in data_dict1:
    for table in data_dict1[BU]:
        try:
            data_dict2[table] = data_dict2[table].union(data_dict1[BU][table])
        except:
            data_dict2[table] = data_dict1[BU][table]

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.adlskrishna23.dfs.core.windows.net",
  "1GrP5UwK+b9YIgiokMZti64d3buVAPcYaH41Kmx7uVF0+mctWZaH5xR7StWnFUukDleUvDZ9uFLd+AStlYtOUg==")

sqldwUsername = "sqladmin"
sqldwPassword = "Azure$2023"
sqldwHostname = "kr-az-sql-pool.database.windows.net"
sqldwDatabase = "krishna-sqlpool"
sqldwPort = 1433
sqldwUrl = f"jdbc:sqlserver://{sqldwHostname}:{sqldwPort};database={sqldwDatabase};user={sqldwUsername};password={sqldwPassword}"

tempDir = "abfss://source@adlskrishna23.dfs.core.windows.net/"



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

#Load df to Azure Dedicated SQL pool
print("Status :")
for table in data_dict2:
    write_to_sqldw(table)

# COMMAND ----------

spark.sql(f"INSERT INTO FILE_LOAD_LOG VALUES {str(logs)[1:-1]}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # end of notebook2
# MAGIC ----
# MAGIC update of notebook2

# COMMAND ----------

table[:-2]

# COMMAND ----------

re.findall('_stg_h',table[:-2])
