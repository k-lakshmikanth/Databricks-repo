# Databricks notebook source
# MAGIC %run /Shared/Common/src_connection

# COMMAND ----------

metaData=spark.table("kpi_etl_analytics_conf.ctl_table_sync").where(" SourceName='source1' ").rdd.collect()

# COMMAND ----------

display(spark.table("kpi_etl_analytics_conf.ctl_table_sync").where(" SourceName='source1' "))

# COMMAND ----------


# Module=metaData['Module']
from pyspark.sql.functions import when, col ,lit

# Iterate over metadata DataFrame
for row in metaData:
    DestinationSchema=row["DestinationSchema"]
    table_name= row["TableName"]
    src_name=row["SourceName"]
    EffectiveDateColumnName=row['EffectiveDateColumnName']
    WatermarkValue=row['WatermarkValue']
    ColumnList=row["ColumnList"]

      # Extract metadata values
    query = f"( {ColumnList} where lastLoadDate >= '{WatermarkValue}' ) AS custom_query"
    
    #Read the source table from the sql server
    source_df = spark.read.format("jdbc").option("url", jdbcurl).option("dbtable",query).load()

    try:
        destination_df = spark.sql(f"select * from {DestinationSchema}.{src_name}_{table_name} limit 1")
    except:
        destination_df=None
        pass

    if destination_df:
        source_column_names = source_df.columns
        destination_column_names = destination_df.columns
        added_columns = set(source_column_names) - set(destination_column_names)
        removed_columns = set(destination_column_names) - set(source_column_names)
        # print(f"Added columns: {added_columns}")
        # print(f"Removed columns: {removed_columns}")

        # Obtain the maximum Mapping_Id from the destination table
        mapping_id = spark.sql("SELECT max(Mapping_Id) FROM kpi_etl_analytics_conf.ctl_bl_sl_mapping").collect()[0][0]
        if mapping_id is None:
            mapping_id = 0
        # Add new columns to the destination table mapping
        if added_columns:
            for column_name in added_columns:
                mapping_id += 1
                dst_col_nm=f'Hed_{column_name}_c'
                spark.sql(f"""INSERT INTO kpi_etl_analytics_conf.ctl_bl_sl_mapping VALUES ({mapping_id}, '{src_name}', '{table_name}', '{column_name}', '{src_name}', '{DestinationSchema}', 'Hed_{table_name}','{dst_col_nm}' , 'N')""")

    #Hardcoded the DataSourceID column
    source_df2=source_df.withColumn("DataSourceID",lit(f"{src_name}"))
    # source_df2.show()
    # print(f'EffectiveDateColumnName : {EffectiveDateColumnName}')
    #Check the EffectiveDateColumnName is null or not
    if EffectiveDateColumnName =='last_update_date':
        print("Incremantal load is Running")
        # Write source data to ADLS table
        source_df2.write.format("delta").mode("append")\
    .option("path", f"/mnt/adls_landing/bronze/{src_name}_{table_name}") \
    .option("mergeSchema", "true")\
    .saveAsTable(f"{DestinationSchema}.{src_name}_{table_name}")
    else:
        print("Full load is Running")
        #Truncating the bronze layer to load the full load
        if destination_df:
            spark.sql(f"truncate table {DestinationSchema}.{src_name}_{table_name}")

            # relative_path="/mnt/adls_landing/bronze/"
            # path=relative_path+src_name+'_'+table_name
            # for i in dbutils.fs.ls(path):
            #     dbutils.fs.rm(i[0],True)

        # Write source data to ADLS table
        source_df2.write.format("delta").mode("overwrite")\
    .option("path", f"/mnt/adls_landing/bronze/{src_name}_{table_name}") \
    .option("mergeSchema", "true")\
    .saveAsTable(f"{DestinationSchema}.{src_name}_{table_name}")
     
# ====================================================================================================        

# COMMAND ----------

# # Remove columns from the destination table mapping
# for column_name in removed_columns:
#     spark.sql(f"""DELETE FROM kpi_etl_analytics_conf.ctl_bl_sl_mapping WHERE Src_tbl_nm='contact' AND Src_Schema_nm='source1' AND Src_cl_nm='{column_name}'""")


# from pyspark.sql.functions import when, col ,lit

# for column_name in added_columns:
#     column_data_type = source_schema[column_name].dataType
#     destination_df = destination_df.withColumn(column_name, lit(None).cast(column_data_type))

# print(f"Old schema : {destination_df.columns}")
# final_df = destination_df.drop(*removed_columns)
# print(f"Current schema  : {final_df.columns}")
