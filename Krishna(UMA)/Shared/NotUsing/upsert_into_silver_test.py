# Databricks notebook source
dbutils.widgets.text("BatchId","1")


# COMMAND ----------

V_BatchId = int(dbutils.widgets.get("BatchId").strip())

# COMMAND ----------

all_metadata=spark.sql("select distinct Src_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm from kpi_etl_analytics_conf.ctl_bl_sl_mapping").collect()


# COMMAND ----------

display(all_metadata)


# COMMAND ----------

V_error_message = ""
V_error_line=""
num_updated_rows=""
num_updated_rows=""

# COMMAND ----------


from pyspark.sql.functions import col,when,desc
for i in all_metadata:
    
    Src_nm=i["Src_nm"]
    Src_tbl_nm=i["Src_tbl_nm"]
    Dest_Schema_nm=i["Dst_Schema_nm"]
    Dest_table_nm=i["Dst_tbl_nm"]
    print(f"Table {Src_nm}.{Src_tbl_nm} is started loading into silver.....")
    where_condtion=f"Src_nm='{Src_nm}' and Src_tbl_nm = '{Src_tbl_nm}'"
    table_mapping=spark.table("kpi_etl_analytics_conf.ctl_bl_sl_mapping").where(where_condtion).orderBy("Mapping_Id").collect()
    
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

    V_DestTableName = Dest_Schema_nm+'.'+Dest_table_nm
    
    # Execute the SQL query to get the new ExecutionId
    result_df = spark.sql(f"""SELECT MAX(ExecutionID)+1 AS ExecutionId_NEW FROM kpi_etl_analytics_conf.ETLProcessHistory""")

    # Get the value of ETLBatchID_NEW from the DataFrame
    V_NewExecutionId = result_df.collect()[0]['ExecutionId_NEW']

    #%%run /Shared/logger/GenericLogger ExecutionId=$V_NewExecutionId BatchId=$V_BatchId DestName=$Dest_Schema_nm TableName=$Dest_table_nm LoadType="" ErrorMessage="" ErrorLineNo="" UpdatedCount=""
    #result = dbutils.notebook.run("/Shared/logger/GenericLogger", timeout_seconds=1800, arguments={"ExecutionId": V_NewExecutionId, "BatchId": V_BatchId, "DestName": Dest_Schema_nm, "TableName": Dest_table_nm, "LoadType":"", "ErrorMessage":"","ErrorLineNo":"","UpdatedCount":""})

    for row in table_mapping:
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
            if f'{Transformation} as {Dst_cl_nm}' not in col_list1:
                col_list1.append(f'{Transformation} as {Dst_cl_nm}')
            if f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}' not in col_list2:
                col_list2.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}')
            if 'MERGE_SUBQUERY.'+Dst_cl_nm not in col_list3:
                col_list3.append('MERGE_SUBQUERY.'+Dst_cl_nm)
            if f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}=MERGE_SUBQUERY.{Dst_cl_nm}' not in col_list4:
                col_list4.append(f'{Dst_Schema_nm}.{Dst_tbl_nm}.{Dst_cl_nm}=MERGE_SUBQUERY.{Dst_cl_nm}')
            if f'{Transformation} as {Dst_cl_nm}' not in col_list5:
                col_list5.append(f'{Transformation} as {Dst_cl_nm}')
                
    col_str1='select \n'+'\n,'.join(map(str,col_list1))+f'\n from {Src_Schema_nm}.{Src_tbl_nm}'
    col_str2='\n,'.join(map(str,col_list2))
    col_str3='\n,'.join(map(str,col_list3)) 
    col_str4='\n,'.join(map(str,col_list4))
    col_str5='select \n'+'\n,'.join(map(str,col_list5))+f'\n from {Src_Schema_nm}.{Src_tbl_nm}'

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
    WHEN MATCHED THEN
    UPDATE SET
        {col_str4}
    WHEN NOT MATCHED THEN
    INSERT
    (
    {col_str2}
    ) values (
    {col_str3}
    );
        """
    # print(merge_str)
    import sys
    try:
        spark.sql(f"DESCRIBE TABLE {Dst_Schema_nm}.{Dst_tbl_nm}")
        merge_df = spark.sql(merge_str)
        # Get the first row of the DataFrame
        first_row = merge_df.first()

        # Access the values of the "num_inserted_rows" and "num_updated_rows" columns
        num_inserted_rows = first_row["num_inserted_rows"]
        num_updated_rows = first_row["num_updated_rows"]
        display(num_updated_rows)
        display(num_inserted_rows)
        
        print("Merge sql executed......")

       # %run /Shared/logger/GenericLogger ExecutionId="" UpdatedCount=V_UpdatedCount DestName=Dest_Schema_nm TableName=Dest_table_nm

        # #Getting Final count of Target table
        # FinalCount = spark.sql(f"""select count(*) as count from {Dest_Schema_nm}.{Dest_table_nm}""")
        # V_FinalTargetCount = FinalCount.collect()[0]['count']
        # #display(V_FinalTargetCount)
        # V_InsertedCount = V_FinalTargetCount-V_InitialTargetCount
        # display(V_UpdatedCount)
        # V_EndTime = datetime.now()
        # V_TimetakenInSecs = (V_EndTime - V_StartTime).total_seconds()

        # # Get the value of ETLBatchID_NEW from the DataFrame
        # UpdateStmt = "UPDATE " + V_ProcessLogTable + " SET ExecutionEndTime='" + str(V_EndTime) + "', TargetInsertRecCount='" + str(V_InsertedCount) + "', TargetUpdateRecCount='" + str(V_UpdatedCount) + "', TimeTakenInSecs='" + str(V_TimetakenInSecs) + "', STATUS='SUCCESS' WHERE ExecutionId='" + str(Execution_id_new) + "'"
        # #display(UpdateStmt)
        # spark.sql(UpdateStmt)

    except Exception as e:
        #%run /Shared/Common/src_connection
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno


# COMMAND ----------

#   InsertStmt = f"INSERT INTO  {V_ProcessLogTable}  VALUES ( {str(Execution_id_new)}, {V_BatchId} , {str(V_StartTime)},NULL,  {V_DestTableName},'RUNNING',0,0,{str(V_InitialTargetCount)},0,0,NULL,NULL,NULL,NULL)"

# COMMAND ----------


 #df=spark.sql("select * from kpi_etl_analytics_conf.ETLProcessHistory")
 #display(df)


# COMMAND ----------

#df=spark.sql("select * from kpi_etl_analytics_conf.etlbatchhistory")
#display(df)

# COMMAND ----------

#dbutils.notebook.exit(V_error_message)
import json
result={"ErrorMessage": V_error_message, "ErrorLine": V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)
