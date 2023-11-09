# Databricks notebook source
# Define input widgets
dbutils.widgets.text("BatchId", "")
dbutils.widgets.text("DestinationSchema", "")
dbutils.widgets.text("TableName", "")
dbutils.widgets.text("SourceName", "")
dbutils.widgets.text("ColumnList", "")
dbutils.widgets.text("LoadType", "")


# COMMAND ----------

# Get the values of parameters using dbutils.widgets module
DestinationSchema = dbutils.widgets.get("DestinationSchema")
table_name = dbutils.widgets.get("TableName")
src_name = dbutils.widgets.get("SourceName")
ColumnList = dbutils.widgets.get("ColumnList")
LoadType = dbutils.widgets.get("LoadType")
V_BatchId = dbutils.widgets.get("BatchId")

# Print the parameter values (optional)
print("DestinationSchema:", DestinationSchema)
print("TableName:", table_name)
print("SourceName:", src_name)
print("ColumnList:", ColumnList)
print("LoadType:", LoadType)


# COMMAND ----------

# MAGIC %run /Shared/Common/src_connection

# COMMAND ----------

if LoadType== 'F':
        #Update the WatermarkValue in the metadata table for the next incremental load
        spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue='1900-01-01T00:00:00.000Z' , EffectiveDateColumnName='null' where TableName='{table_name}' and SourceName='{src_name}' """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kpi_etl_analytics_conf.ctl_source_bl_mapping

# COMMAND ----------

dst=f"""SELECT Dst_Schema_nm,Dst_tbl_nm FROM kpi_etl_analytics_conf.ctl_source_bl_mapping WHERE Src_nm = '{src_name}' AND Src_tbl_nm = '{table_name}'"""
display(dst)
dstdf= spark.sql(dst)
V_DestName = dstdf.select("Dst_Schema_nm").collect()[0][0]
V_TableName= dstdf.select("Dst_tbl_nm").collect()[0][0]
V_Status=''
st=f"""SELECT status FROM kpi_etl_analytics_conf.etl_process_history WHERE BatchID = {V_BatchId} AND TableName = '{V_DestName}.{V_TableName}'"""
display(st)
df= spark.sql(st)
if(df.count()>0):
    V_Status = df.select("status").collect()[0][0]
display(V_Status)

# COMMAND ----------

mapping_df = spark.sql(f"Select * from kpi_etl_analytics_conf.ctl_source_bl_mapping where Src_tbl_nm ='{table_name}' and Src_nm='{src_name}' order by Mapping_Id ")
display(mapping_df)

# COMMAND ----------


from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
import datetime
try:
    if V_Status=='FAILED' or V_Status=='' or V_Status=='RUNNING' or V_Status=='':
        V_WatermarkVlaue = spark.sql(f"""select watermarkValue from kpi_etl_analytics_conf.ctl_table_sync where TableName='{table_name}' and SourceName='{src_name}' """).collect()[0][0]
        V_EffectiveDateColumnName = spark.sql(f"""select EffectiveDateColumnName from kpi_etl_analytics_conf.ctl_table_sync where TableName='{table_name}' and SourceName='{src_name}' """).collect()[0][0]

        V_Query = spark.sql(f"""select ColumnList from kpi_etl_analytics_conf.ctl_table_sync where TableName='{table_name}' and SourceName='{src_name}' """).collect()[0][0]
        display(V_Query)

        # Extract metadata values
        query = f"({V_Query} where lastLoadDate >= '{V_WatermarkVlaue}' ) AS custom_query"

        display(query)
        # Read the source table from the SQL Server
        try:
            source_df = spark.read.format("jdbc").option("url", jdbcurl).option("dbtable", query).load()
        except Exception as e:
            print(f"Error reading data from the source table: {e}")
            raise e

        # Check if the destination table exists
        try:
            destination_df = spark.sql(f"select * from {DestinationSchema}.{src_name}_{table_name} limit 1")
        except Exception as e:
            print(f"Error accessing destination table: {e}")
            destination_df = None


        if destination_df:
            # Get the column names from the source and destination dataframes
            source_column_names = source_df.columns
            destination_column_names = destination_df.columns

            # Calculate the added columns (columns present in the source but not in the destination)
            added_columns = set(source_column_names) - set(destination_column_names)

            # Calculate the removed columns (columns present in the destination but not in the source)
            removed_columns = set(destination_column_names) - set(source_column_names)

            # Obtain the maximum Mapping_Id from the destination table
            mapping_id = spark.sql("SELECT max(Mapping_Id) FROM kpi_etl_analytics_conf.ctl_bl_sl_mapping").collect()[0][0]
            if mapping_id is None:
                mapping_id = 0

            # Add new columns to the destination table mapping
            if added_columns:
                for column_name in added_columns:
                    # Increment mapping_id for each new column
                    mapping_id += 1
                    
                    # Create the destination column name in the format 'Hed_{column_name}_c'
                    dst_col_nm = f'Hed_{column_name}_c'
                    
                    try:
                        # Insert a new mapping record into the ctl_bl_sl_mapping table
                        spark.sql(f"""INSERT INTO kpi_etl_analytics_conf.ctl_bl_sl_mapping 
                                    VALUES ({mapping_id}, '{src_name}', '{table_name}', '{column_name}', 
                                    '{src_name}', '{DestinationSchema}', 'Hed_{table_name}', '{dst_col_nm}', 'N')""")
                    except Exception as e:
                        # Print error message and raise exception if the insert fails
                        print(f"Error inserting mapping record: {e}")
                        raise e

        # Hardcoded the DataSourceID column
        source_df2 = source_df.withColumn("DataSourceID", lit(f"{src_name}"))
        source_df2 = source_df2.withColumn("LastmodifiedDate", current_timestamp())#need to update lastmodifieddate row by row

        if V_EffectiveDateColumnName == 'last_update_date':
            print("Incremental load is running")
            try:
                Write source data to ADLS table
                source_df2.write.format("delta").mode("append") \
                    .option("path", f"/mnt/adls_landing/bronze/{src_name}_{table_name}") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(f"{DestinationSchema}.{src_name}_{table_name}")
            except Exception as e:
                print(e)
        else:
            print("Full load is running")
            # Truncate the bronze layer to load the full load
            if destination_df:
                try:
                    spark.sql(f"truncate table {DestinationSchema}.{src_name}_{table_name}")
                except Exception as e:
                    print(f"Error truncating the destination table: {e}")
                    raise e

            # Write dataframe to Delta table 
            try:
                source_df2.write.format("delta").mode("overwrite") \
                    .option("path", f"/mnt/adls_landing/bronze/{src_name}_{table_name}") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(f"{DestinationSchema}.{src_name}_{table_name}")
            except Exception as e:
                print(e)
                raise e
    else:
        print("Destination table already loaded!")
except Exception as e:
    print(e)
    raise e


# COMMAND ----------

#Update the WatermarkValue in the metadata table for the next incremental load
spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue=current_timestamp() where TableName='{table_name}' and SourceName='{src_name}' """)

#Update the EffectiveDateColumnName in the metadata table for the next incremental load
spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set EffectiveDateColumnName='last_update_date' where TableName='{table_name}' and SourceName='{src_name}'""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_bl.source2_contact
# MAGIC --update kpi_cloud_bl.source1_contact set ContactPhone='2222222222' where ContactID='CUST003'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_bl.source1_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_bl.source3_ext_contact

# COMMAND ----------

import json
result={"ErrorMessage": V_error_message, "ErrorLine": V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)
