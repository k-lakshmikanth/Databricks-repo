# Databricks notebook source
all_metadata=spark.sql("select distinct Src_nm,Src_tbl_nm from kpi_etl_analytics_conf.ctl_bl_sl_mapping").collect()


# COMMAND ----------

display(all_metadata)

# COMMAND ----------


from pyspark.sql.functions import col,when,desc


for i in all_metadata:
    Src_nm=i["Src_nm"]
    Src_tbl_nm=i["Src_tbl_nm"]
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

    # print(join_cond_str)
    # print(col_str1)
    # print(col_str2)
    # print(col_str3)
    # print(col_str4)
    # seq_str=f'{DATABASE}.SEQ_{TABLE_NAME}.NEXTVAL,'

    #Getting Initial count of Target table
    InitialCount = spark.sql(f"""select count(*) as count from {Dst_Schema_nm}.{Dst_tbl_nm}""")
    V_InitialTargetCount = InitialCount.collect()[0]['count']
    display(V_InitialTargetCount)

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
    print(merge_str)
    
    #Getting Initial count of Target table
    FinalCount = spark.sql(f"""select count(*) as count from {Dst_Schema_nm}.{Dst_tbl_nm}""")
    V_FinalTargetCount = FinalCount.collect()[0]['count']
    display(V_FinalTargetCount)
    V_InsertedCount = V_FinalTargetCount-V_InitialTargetCount

    try:
        spark.sql(f"DESCRIBE TABLE {Dst_Schema_nm}.{Dst_tbl_nm}")
        spark.sql(merge_str)
        print("Merge sql executed......")
    except:
        # print(col_str5)
        bronzeDF=spark.sql(col_str5)
        print("Exception block is executed......")
        bronzeDF.write.format("delta").mode("overwrite")\
        .option("path",f"/mnt/adls_landing/silver/{Dst_tbl_nm}")\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{Dst_Schema_nm}.{Dst_tbl_nm}")

    print(f"Table {Src_nm}.{Src_tbl_nm} is succesfully loaded into silver.....")
    print("\n")
        


# COMMAND ----------

# %sql

# select * from kpi_cloud_sl.hed_contact
