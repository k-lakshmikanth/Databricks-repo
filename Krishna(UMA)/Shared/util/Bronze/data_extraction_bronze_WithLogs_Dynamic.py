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
import sys
# Print the parameter values (optional)
print("DestinationSchema:", DestinationSchema)
print("TableName:", table_name)
print("SourceName:", src_name)
print("ColumnList:", ColumnList)
print("LoadType:", LoadType)


# COMMAND ----------

# MAGIC %run /Shared/Common/src_connection

# COMMAND ----------

wquery = f"""SELECT ExecuteBeginTS FROM kpi_etl_analytics_conf.etl_batch_history WHERE BatchName LIKE '{src_name}%' AND BatchName LIKE '%BronzeExtraction' ORDER BY batchId DESC LIMIT 1"""
display(wquery)
df = spark.sql(wquery)
if df.count() > 0:
    ExtractWindowsStartDate = df.select("ExecuteBeginTS").collect()[0][0]
    print(ExtractWindowsStartDate)

# COMMAND ----------


V_Load = spark.sql(f"""select IsAlwaysFullLoad from kpi_etl_analytics_conf.ctl_table_sync where TableName='{table_name}' and SourceName='{src_name}' """).collect()[0][0]
print(V_Load)
if LoadType== 'F' or V_Load == 'Y':
        #Update the WatermarkValue in the metadata table for the next incremental load
        spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue='1900-01-01T00:00:00.000Z' , EffectiveDateColumnName='null' where TableName='{table_name}' and SourceName='{src_name}' """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kpi_etl_analytics_conf.ctl_source_bl_mapping

# COMMAND ----------

try:
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
except Exception as e:
    print(e)
    V_error_message = str(e)
    V_error_message = V_error_message[0:300]
    display(V_error_message)
    _, _, tb = sys.exc_info()
    V_error_line = tb.tb_lineno

# COMMAND ----------

mapping_df = spark.sql(f"Select * from kpi_etl_analytics_conf.ctl_source_bl_mapping where Src_tbl_nm ='{table_name}' and Src_nm='{src_name}' order by Mapping_Id ")
display(mapping_df)

# COMMAND ----------

try:
    V_error_message = ""
    V_error_line=""
    num_inserted_rows=0
    num_updated_rows=0
    V_Status =''
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

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        import sys
        from pyspark.sql.functions import col,when,desc

        print(f"Table {src_name}-{table_name} is started loading into bronze.....")

        table_mapping=spark.sql(f"""select * from kpi_etl_analytics_conf.ctl_source_bl_mapping
        where Src_nm='{src_name}' and Src_tbl_nm = '{table_name}' order by Mapping_Id """).collect()
        display(table_mapping)

        def source_logic(mapping):
            col_list1=[]
            Src_Schema_nm=''
            Src_tbl_nm =''
            for row in mapping:
                Src_Schema_nm=row["Src_Schema_nm"]
                Src_tbl_nm=row["Src_tbl_nm"]
                Src_cl_nm=row["Src_cl_nm"]
                Dst_cl_nm=row["Dst_cl_nm"]
                Transformation=row["Transformation"]
                if Transformation=="direct":
                    col_list1.append(Src_cl_nm)
                else:
                    if f'{Transformation} as {Dst_cl_nm}' not in col_list1:
                        col_list1.append(f'{Transformation} as {Dst_cl_nm}')

            col_str1 = f"select \n{','.join(map(str, col_list1))}\n from {src_name}.{Src_tbl_nm} where LastmodifiedDate >= '{ExtractWindowsStartDate}'"
            print(col_list1)
            return col_str1
        source_stmt=source_logic(table_mapping)     

        #bronze_df=spark.sql(source_stmt)
        print(source_stmt)
        
        # display(bronze_df)
        # bronze_df.createOrReplaceTempView(f"{table_name}")

        def return_merge_Statmet(mapping):
            col_list1=[]
            col_list2=[]
            col_list3=[]
            col_list4=[]
            col_list5=[]
            
            Src_Schema_nm=''
            Src_tbl_nm =''
            Dst_Schema_nm=''
            Dst_tbl_nm=''
            src_join_key=''
            tgt_join_key=''
            
            for row in mapping:
                Src_Schema_nm=row["Src_Schema_nm"]
                Src_tbl_nm=row["Src_tbl_nm"]
                Src_cl_nm=row["Src_cl_nm"]

                Dst_Schema_nm=row["Dst_Schema_nm"]
                Dst_tbl_nm=row["Dst_tbl_nm"]
                Dst_cl_nm=row["Dst_cl_nm"]
            
                Is_join_cl=row["Is_join_cl"]
                Transformation=row["Transformation"]

                if Is_join_cl:
                    # print(Is_join_cl)
                    src_join_key=Src_cl_nm
                    tgt_join_key=Dst_cl_nm

                if Transformation=="direct":
                    col_list1.append(Src_cl_nm)
                    col_list2.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}')
                    col_list3.append('MERGE_SUBQUERY.'+Src_cl_nm)
                    col_list4.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}=MERGE_SUBQUERY.{Src_cl_nm}')
                    col_list5.append(f'{Src_cl_nm} as {Dst_cl_nm}')
                else:
                    if f'{Dst_cl_nm}' not in col_list1:
                        col_list1.append(f'{Dst_cl_nm}')
                    if f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}' not in col_list2:
                        col_list2.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}')
                    if 'MERGE_SUBQUERY.'+Dst_cl_nm not in col_list3:
                        col_list3.append('MERGE_SUBQUERY.'+Dst_cl_nm)
                    if f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}=MERGE_SUBQUERY.{Dst_cl_nm}' not in col_list4:
                        col_list4.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}=MERGE_SUBQUERY.{Dst_cl_nm}')
                    if f'{Transformation} as {Dst_cl_nm}' not in col_list5:
                        col_list5.append(f'{Transformation} as {Dst_cl_nm}')
                
            col_str1='select \n'+'\n,'.join(map(str,col_list1))+f'\nfrom {Src_Schema_nm}.{Src_tbl_nm}'
            col_str2='\n,'.join(map(str,col_list2))
            col_str3='\n,'.join(map(str,col_list3))
            col_str4='\n,'.join(map(str,col_list4))
            col_str5='select \n'+'\n,'.join(map(str,col_list5))+f'\n from {Src_tbl_nm}'
            print(col_str1)
            join_cond_str=f"{Dst_Schema_nm}.{Dst_tbl_nm}.{tgt_join_key} = MERGE_SUBQUERY.{src_join_key}"

            merge_str=f"""
            MERGE
            INTO {Dst_Schema_nm}.{Dst_tbl_nm}
            USING
            (
            {col_str1}
            ) MERGE_SUBQUERY
            ON
            ({join_cond_str})
            WHEN MATCHED 
            THEN
            UPDATE SET {col_str4},LastLoadDate = current_timestamp()
            WHEN NOT MATCHED THEN
            INSERT
            (
            {col_str2},LastLoadDate
            ) values (
            {col_str3}, current_timestamp()
            );
            """
                
            return (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,merge_str)

        (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,merge_str)=return_merge_Statmet(mapping=table_mapping)
        

        print(merge_str)

        #insert_merge_df = spark.sql(insert_merge_str)
        print("\nMerge sql executed......")

        # # Access the values of the "num_inserted_rows" and "num_updated_rows" columns
        # num_inserted_rows=insert_merge_df.select("num_inserted_rows").collect()[0][0]
        # num_updated_rows=insert_merge_df.select("num_updated_rows").collect()[0][0]
        # print(num_inserted_rows,num_updated_rows)
        print(f"Table {src_name}.{table_name} is succesfully loaded into silver.....")
    else:
        print("Destination table already loaded!")
except Exception as e:
    print(e)
    V_error_message = str(e)
    V_error_message = V_error_message[0:300]
    display(V_error_message)
    _, _, tb = sys.exc_info()
    V_error_line = tb.tb_lineno

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC contactID,contactFirstName,contactLastName,contactGender,contactEmail,contactPhone,LastLoadDate
# MAGIC  from source2.Contact where LastmodifiedDate >= '2023-08-22 06:14:40.071000'
# MAGIC -- select 
# MAGIC -- contactID
# MAGIC -- ,contactFirstName
# MAGIC -- ,contactLastName
# MAGIC -- ,contactGender
# MAGIC -- ,contactEmail
# MAGIC -- ,contactPhone
# MAGIC -- ,LastLoadDate
# MAGIC -- from Source2.Contact
# MAGIC
# MAGIC --             MERGE
# MAGIC --             INTO kpi_cloud_bl.source2_contact
# MAGIC --             USING
# MAGIC --             (
# MAGIC --             select 
# MAGIC -- contactID
# MAGIC -- ,contactFirstName
# MAGIC -- ,contactLastName
# MAGIC -- ,contactGender
# MAGIC -- ,contactEmail
# MAGIC -- ,contactPhone
# MAGIC -- ,LastLoadDate
# MAGIC -- from Source2.Contact
# MAGIC --             ) MERGE_SUBQUERY
# MAGIC --             ON
# MAGIC --             (kpi_cloud_bl.source2_contact.contactID = MERGE_SUBQUERY.contactID)
# MAGIC --             WHEN MATCHED 
# MAGIC --             THEN
# MAGIC --             UPDATE SET kpi_cloud_bl.source2_contact.contactID=MERGE_SUBQUERY.contactID
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactFirstName=MERGE_SUBQUERY.contactFirstName
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactLastName=MERGE_SUBQUERY.contactLastName
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactGender=MERGE_SUBQUERY.contactGender
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactEmail=MERGE_SUBQUERY.contactEmail
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactPhone=MERGE_SUBQUERY.contactPhone
# MAGIC -- ,kpi_cloud_bl.source2_contact.LastLoadDate=MERGE_SUBQUERY.LastLoadDate,LastLoadDate = current_timestamp()
# MAGIC --             WHEN NOT MATCHED THEN
# MAGIC --             INSERT
# MAGIC --             (
# MAGIC --             kpi_cloud_bl.source2_contact.contactID
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactFirstName
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactLastName
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactGender
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactEmail
# MAGIC -- ,kpi_cloud_bl.source2_contact.contactPhone
# MAGIC -- ,kpi_cloud_bl.source2_contact.LastLoadDate,LastLoadDate
# MAGIC --             ) values (
# MAGIC --             MERGE_SUBQUERY.contactID
# MAGIC -- ,MERGE_SUBQUERY.contactFirstName
# MAGIC -- ,MERGE_SUBQUERY.contactLastName
# MAGIC -- ,MERGE_SUBQUERY.contactGender
# MAGIC -- ,MERGE_SUBQUERY.contactEmail
# MAGIC -- ,MERGE_SUBQUERY.contactPhone
# MAGIC -- ,MERGE_SUBQUERY.LastLoadDate, current_timestamp()
# MAGIC --             );

# COMMAND ----------

try:
    #Update the WatermarkValue in the metadata table for the next incremental load
    spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set WatermarkValue=current_timestamp() where TableName='{table_name}' and SourceName='{src_name}' """)

    #Update the EffectiveDateColumnName in the metadata table for the next incremental load
    spark.sql(f"""update kpi_etl_analytics_conf.ctl_table_sync set EffectiveDateColumnName='last_update_date' where TableName='{table_name}' and SourceName='{src_name}'""")
except Exception as e:
    print(e)
    V_error_message = str(e)
    V_error_message = V_error_message[0:300]
    display(V_error_message)
    _, _, tb = sys.exc_info()
    V_error_line = tb.tb_lineno

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_bl.source2_contact

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
