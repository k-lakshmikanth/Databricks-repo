# Databricks notebook source
# Define input widgets
dbutils.widgets.text("SourceTableName", "")
dbutils.widgets.text("SourceName", "")
dbutils.widgets.text("SourceSchemaName", "")
dbutils.widgets.text("TargetSchemaName", "")
dbutils.widgets.text("TargetTableName", "")
dbutils.widgets.text("BatchId", "")
dbutils.widgets.text("ExtractWindowsStartDate", "")

# COMMAND ----------

Src_nm = dbutils.widgets.get("SourceName")
Src_Schema_nm = dbutils.widgets.get("SourceSchemaName")
tbl_nm = dbutils.widgets.get("SourceTableName")
Tgt_schema_nm = dbutils.widgets.get("TargetSchemaName")
Tgt_tbl_nm = dbutils.widgets.get("TargetTableName")
V_BatchId = dbutils.widgets.get("BatchId")
ExtractWindowsStartDate = dbutils.widgets.get("ExtractWindowsStartDate")
print(Src_nm,Src_Schema_nm,tbl_nm,Tgt_schema_nm,Tgt_tbl_nm,V_BatchId)
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

# COMMAND ----------

wquery = f"""SELECT ExecuteBeginTS FROM {V_BatchLogTable} WHERE BatchName LIKE '{Src_nm}%' AND BatchName LIKE '%BronzeExtraction' and RetryAttempt =(SELECT MIN(RetryAttempt) FROM {V_BatchLogTable} WHERE BatchName LIKE '{Src_nm}%' AND BatchName LIKE '%BronzeExtraction') ORDER BY ExecuteBeginTS DESC LIMIT 1"""
display(wquery)
df = spark.sql(wquery)
if df.count() > 0:
    ExtractWindowsStartDate = df.select("ExecuteBeginTS").collect()[0][0]
    print(ExtractWindowsStartDate)

# COMMAND ----------

V_Status=''
st=f"""SELECT status FROM {V_ProcessLogTable} WHERE BatchID = '{V_BatchId}' AND TableName = '{Tgt_schema_nm}.{Tgt_tbl_nm}'"""
display(st)
df= spark.sql(st)
if(df.count()>0):
    V_Status = df.select("status").collect()[0][0]
display(V_Status)

# COMMAND ----------

try:
            
    V_error_message = ""
    V_error_line=""
    num_inserted_rows=0
    num_updated_rows=0
    if V_Status=='FAILED' or V_Status=='' or V_Status=='RUNNING' or V_Status=='':
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        import sys
        from pyspark.sql.functions import col,when,desc

        print(f"Table {Src_nm}-{tbl_nm} is started loading into silver.....")

        table_mapping=spark.sql(f"""select * from kpi_etl_analytics_conf.ctl_bl_sl_mapping 
        where Src_nm='{Src_nm}' and Src_tbl_nm = '{tbl_nm}' order by Mapping_Id """).collect()
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

            col_str1 = f"select \n{','.join(map(str, col_list1))}\n from {Src_Schema_nm}.{Src_tbl_nm} where LastmodifiedDate >= '{ExtractWindowsStartDate}'"
            print(col_list1)
            return col_str1
        source_stmt=source_logic(table_mapping)     

        bronze_df=spark.sql(source_stmt)
        print(source_stmt)

        # Define columns to exclude from the checksum calculation
        columns_to_exclude = ['Effective_Start_Date', 'Effective_End_Date', 'Current_Record','Hed_LastLoadDate_c','LastLoadDate']

        bronze_df = bronze_df.withColumn(
            "checksum",
            F.sha2(
                F.concat(
                    *[F.coalesce(bronze_df[col], F.lit("")) for col in bronze_df.columns if col not in columns_to_exclude]
                ),
                256  # Specify the SHA-2 hash length (in this case, 256 bits)
            )
        ).dropDuplicates()
        
        display(bronze_df)
        bronze_df.createOrReplaceTempView(f"{tbl_nm}")

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
                
            col_str1='select \n'+'\n,'.join(map(str,col_list1))+f'\n,checksum \nfrom {Src_tbl_nm}'
            col_str2='\n,'.join(map(str,col_list2))+f'\n,{Dst_Schema_nm}.{Dst_tbl_nm}.checksum'
            col_str3='\n,'.join(map(str,col_list3))+f'\n,MERGE_SUBQUERY.checksum' 
            col_str4='\n,'.join(map(str,col_list4))+f'\n,{Dst_Schema_nm}.{Dst_tbl_nm}.checksum=MERGE_SUBQUERY.checksum'
            col_str5='select \n'+'\n,'.join(map(str,col_list5))+f'\n from {Src_tbl_nm}'
            print(col_str1)
            join_cond_str=f"{Dst_Schema_nm}.{Dst_tbl_nm}.{tgt_join_key} = MERGE_SUBQUERY.{src_join_key}"

            insert_merge_str=f"""
    MERGE
    INTO {Dst_Schema_nm}.{Dst_tbl_nm}
    USING
    (
    {col_str1}
    ) MERGE_SUBQUERY
    ON
    ({join_cond_str})
    WHEN MATCHED AND {Dst_Schema_nm}.{Dst_tbl_nm}.checksum != MERGE_SUBQUERY.checksum
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
                
        
    #         join_update_cond_str=f"{Dst_Schema_nm}.{Dst_tbl_nm}.checksum = MERGE_SUBQUERY.checksum"

    #         update_merge_str=f"""
    # MERGE
    # INTO {Dst_Schema_nm}.{Dst_tbl_nm}
    # USING
    # (
    # {col_str1}
    # ) MERGE_SUBQUERY
    # ON
    # ({join_update_cond_str})
    # WHEN NOT MATCHED THEN
    # INSERT
    # (
    # {col_str2}
    # ) values (
    # {col_str3}
    # );
                #"""
            return (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,insert_merge_str)

        (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,insert_merge_str)=return_merge_Statmet(mapping=table_mapping)
        

        print(insert_merge_str)

        insert_merge_df = spark.sql(insert_merge_str)
        print("\nMerge sql executed......")

        # Access the values of the "num_inserted_rows" and "num_updated_rows" columns
        num_inserted_rows=insert_merge_df.select("num_inserted_rows").collect()[0][0]
        num_updated_rows=insert_merge_df.select("num_updated_rows").collect()[0][0]
        print(num_inserted_rows,num_updated_rows)
        print(f"Table {Src_nm}.{Src_tbl_nm} is succesfully loaded into silver.....")
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

V_NewJobId=0
result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM kpi_etl_analytics_conf.etl_job_history""")
V_NewJobId = result_df.collect()[0]['JobId_NEW']
print(V_NewJobId)

# COMMAND ----------

import json
result={"ErrorMessage": V_error_message, "ErrorLine": V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows,"JobId":V_NewJobId}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_sl.hed_contact
