# Databricks notebook source
dbutils.widgets.text("ExtractWindowsStartDate","")
dbutils.widgets.text("TargetTableName", "")
dbutils.widgets.text("SourceTableName", "")
dbutils.widgets.text("SourceSchemaName", "")
dbutils.widgets.text("TargetSchemaName", "")
dbutils.widgets.text("BatchId", "")

# COMMAND ----------

V_ExtractWindowsEndDate =dbutils.widgets.get("ExtractWindowsStartDate")
TableName=dbutils.widgets.get("TargetTableName")
SourceTableName=dbutils.widgets.get("SourceTableName")
V_SourceSchemaName = dbutils.widgets.get("SourceSchemaName")
V_TargetSchemaName = dbutils.widgets.get("TargetSchemaName")
ExtractWindowsStartDate=dbutils.widgets.get("ExtractWindowsStartDate")
V_BatchId = dbutils.widgets.get("BatchId")
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"
print(V_BatchId)
print(TableName)
print(ExtractWindowsStartDate)

# COMMAND ----------

# DBTITLE 1,Pyspark Dataframe Approach
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window


# # Create a mapping between the silver and the Gold layer column names
# column_mapping = {
#     "Hed_contactID_c": "ContactID",
#     "Hed_Choosen_full_name_c": "FullName",
#     "Hed_contactGender_c": "Gender",
#     "Hed_contactEmail_c": "Email",
#     "Hed_contactPhone_c": "Phone"
# }

# # Read the silver layer DataFrame and format the date column
# silver_df = spark.table("kpi_cloud_sl.hed_contact").withColumn("Hed_LastLoadDate_c", F.date_format("Hed_LastLoadDate_c", "yyyy-MM-dd"))

# print(f"{silver_df.count()} source records are ready to compare with target table\n")


# # Rename the columns in the DataFrame based on the mapping
# for original_col, gold_col in column_mapping.items():
#     silver_df = silver_df.withColumnRenamed(original_col, gold_col)

# # Define columns to exclude from the checksum calculation
# columns_to_exclude = ['Effective_Start_Date', 'Effective_End_Date', 'DataSourceID', 'Current_Record','Hed_LastLoadDate_c']

# # Calculate the checksum for each row based on the specified columns
# silver_df = silver_df.withColumn(
#     "checksum",
#     F.sha2(
#         F.concat(
#             *[F.coalesce(silver_df[col], F.lit("")) for col in silver_df.columns if col not in columns_to_exclude]
#         ),
#         256  # Specify the SHA-2 hash length (in this case, 256 bits)
#     )
# )

# # Read the gold layer DataFrame
# full_gold_df = spark.table("kpi_cloud_gl.dim_contact")
# print(f"{full_gold_df.count()} target records are already there in the target table\n")

# gold_df = full_gold_df.filter(F.col("Current_Record")=="Y")
# print(f"Out of {full_gold_df.count()} target records , {gold_df.count()} latest records are ready compare with the source table\n")

# # Find the maximum value of the 'contact_ik' in the gold layer
# max_contact_ik = gold_df.agg(F.max("contact_ik")).collect()[0][0]

# # Set the starting value for the new surrogate key
# if not max_contact_ik:
#     max_contact_ik = 1000
# new_surrogate_key_start_value = max_contact_ik
# window_spec = Window.orderBy(F.lit(1))

# # Find new records by performing a left anti join
# new_records = silver_df.join(gold_df, ['ContactID'], "left_anti")

# # Set default values for Effective_Start_Date, Effective_End_Date, and Current_Record for new records
# new_records = new_records.withColumn("Effective_Start_Date", F.lit("1900-01-01").cast("date"))
# new_records = new_records.withColumn("Effective_End_Date", F.lit("9999-12-31").cast("date"))
# new_records = new_records.withColumn("Current_Record", F.lit("Y"))

# # Calculate the new value for the 'contact_ik' column as a unique identifier for new records
# new_records = new_records.withColumn("contact_ik", F.row_number().over(window_spec) + new_surrogate_key_start_value)


# # Find updated records by performing a left outer join and filter for non-null ContactID
# updated_records = silver_df.join(gold_df, (silver_df['ContactID'] == gold_df['ContactID']) & (silver_df['checksum'] != gold_df['checksum']), "left_outer").dropDuplicates(['ContactID'])

# # print("Updated records")
# # display(updated_records)

# # Filter out rows where ContactID is null in the gold layer
# updated_records = updated_records.filter(gold_df['ContactID'].isNotNull())

# # print("Updated records")
# # display(updated_records)

# # Drop columns from the updated_records DataFrame that are not needed in the gold layer (except for Effective_Start_Date, Contact_ik, Current_Record, and Effective_End_date)
# updated_records = updated_records.drop(*[gold_df[col] for col in gold_df.columns if col not in ["Effective_Start_Date", "Contact_ik", "Current_Record", "Effective_End_date"]])

# # print("Updated records")
# # display(updated_records)

# # Set the value of Effective_Start_Date in the updated_records DataFrame to the corresponding value in the silver_df's Hed_LastLoadDate_c column
# updated_records = updated_records.withColumn("Effective_Start_Date", silver_df["Hed_LastLoadDate_c"])

# # print("Updated records")
# # display(updated_records)


# # Define a window specification for partitioning by ContactID and ordering by Effective_Start_Date
# w = Window.partitionBy(F.col('ContactID')).orderBy(F.col("Effective_Start_Date"))


# # Combine data from the gold layer and the new records into a single DataFrame
# combined_data = full_gold_df.union(new_records.select(gold_df.columns))
# # display(combined_data)

# # Add updated records to the combined DataFrame
# combined_data = combined_data.union(updated_records.select(gold_df.columns))
# # display(combined_data)

# # Calculate the next date for each record based on the Effective_Start_Date using a window function
# combined_data = combined_data.withColumn("next_date", F.lead("Effective_Start_Date").over(w))
# # display(combined_data)

# # Calculate the new date as the current date minus one day, using the first value of the next_date column within the window
# combined_data = combined_data.withColumn("new_date", F.date_sub(F.first("next_date").over(w), 1))
# # display(combined_data)

# # Update the Effective_End_Date column with the new_date value if the next_date is not null, otherwise keep the original Effective_End_Date
# combined_data = combined_data.withColumn("Effective_End_Date", F.when(F.col("next_date").isNotNull(), F.col("new_date")).otherwise(F.col("Effective_End_Date")))
# # display(combined_data)

# # Drop the new_date and next_date columns as they are no longer needed
# combined_data = combined_data.drop("new_date").drop("next_date")
# # display(combined_data)


# # Update the Current_Record column based on Effective_End_Date
# combined_data = combined_data.withColumn('Current_Record', F.when(F.col("Effective_End_date") == F.lit('9999-12-31'), "Y").otherwise("N"))
# # display(combined_data)

# # Format Effective_Start_Date column to the desired format 'yyyy-MM-dd'
# combined_data = combined_data.withColumn("Effective_Start_Date", F.to_date("Effective_Start_Date", "yyyy-MM-dd"))
# # display(combined_data)

# print("Inserted records")
# print(f"{new_records.count()} records Inserted\n")
# display(new_records)

# print("Updated records")
# print(f"{updated_records.count()} records Updated\n")
# display(updated_records)

# print("Final Data Frame after SCD2")
# print(f"{combined_data.count()} records Loaded")
# display(combined_data)
        

# # Write the combined_data DataFrame to the Delta table in the gold layer

# # Select only the columns from the gold_df (since the combined_data includes all columns from gold_df and new_records DataFrame)
# combined_data.select(gold_df.columns).write.format("delta").mode("overwrite")\
#     .option("path", f"/mnt/adls_landing/gold/dim_contact") \
#     .option("mergeSchema", "true")\
#     .saveAsTable("kpi_cloud_gl.dim_contact")


# COMMAND ----------



# # Read the mapping table into a DataFrame
# mapping_df = spark.table("kpi_etl_analytics_conf.ctl_table_dims_facts")
# # Generate the MERGE query dynamically
# merge_query = "MERGE INTO {} AS target\n".format("kpi_cloud_gl.dim_contact")

# # Create the source select statement with transformations
# select_clause = "SELECT\n"
# for row in mapping_df.collect():
#     src_column = row["Source_Column"]
#     tgt_column = row["Target_Column"]
#     transformation = row["Transformation"]

#     source_schema = row["Source_Schema"]
#     source_table = row["Source_Table"]
   
#     target_schema = row["Target_Schema"]
#     target_table = row["Target_Table"]

#     is_join_key = row["Is_Join_Key"]

#     # Add transformation for the source column if it exists
#     if transformation!='direct':
#         select_clause += "    {} AS {},\n".format(transformation.format(src_column), tgt_column)
#     else:
#         select_clause += "    {}.{}.{},\n".format(source_schema,source_table,src_column)

# # Remove the trailing comma and add FROM clause
# select_clause = select_clause.rstrip(",\n") + "\nFROM {}.{}".format(source_schema,source_table)

# # Generate the ON clause for the join keys
# join_keys = mapping_df.filter(mapping_df["Is_Join_Key"] == "true").select("Target_Column")
# on_clause = "ON " + " AND ".join("target.{} = source.{}".format(tgt_column, tgt_column)
#                                  for row in join_keys.collect())


# # Generate the WHEN MATCHED and WHEN NOT MATCHED clauses
# when_matched_clause = "WHEN MATCHED THEN\n UPDATE SET \n"
# when_not_matched_clause = "WHEN NOT MATCHED THEN\n"

# # Add update statements for columns that need to be updated
# for row in mapping_df.collect():
#     tgt_column = row["Target_Column"]
#     src_column = row["Source_Column"]

#     # Skip join key columns and columns that are directly mapped
#     if row["Is_Join_Key"] == "true" or not row["Transformation"]:
#         continue

#     when_matched_clause += "  target.{} = source.{},\n".format(tgt_column, tgt_column)

# # Add insert statements for columns that need to be inserted
# when_not_matched_clause += "    INSERT ({})\n".format(", ".join(mapping_df.filter("Transformation=='direct' ")
#                                                                 .select("Target_Column")
#                                                                 .rdd.flatMap(lambda x: x)
#                                                                 .collect()))

# when_not_matched_clause += "    VALUES ({})".format(", ".join(mapping_df.filter("Transformation=='direct' ")
#                                                              .select("Source_Column")
#                                                              .rdd.flatMap(lambda x: x)
#                                                              .collect()))

# # Combine all clauses to form the final MERGE query
# merge_query += "{}\n{}\n{}".format(select_clause, on_clause, when_matched_clause.rstrip(",\n") + "\n" + when_not_matched_clause)

# # Print the generated MERGE query
# print(merge_query)


# COMMAND ----------

wquery = f"""SELECT ExecuteBeginTS FROM {V_BatchLogTable} WHERE BatchName LIKE '%SilverUpsert' and RetryAttempt =(SELECT MIN(RetryAttempt) FROM {V_BatchLogTable} WHERE BatchName LIKE '%SilverUpsert') ORDER BY ExecuteBeginTS LIMIT 1"""
display(wquery)
df = spark.sql(wquery)
if df.count() > 0:
    ExtractWindowsStartDate = df.select("ExecuteBeginTS").collect()[0][0]
    print(ExtractWindowsStartDate)

# COMMAND ----------

V_Status=''
st=f"""SELECT status FROM {V_ProcessLogTable} WHERE BatchID = '{V_BatchId}' AND TableName = '{V_TargetSchemaName}.{TableName}'"""
print(st)
df= spark.sql(st)
if(df.count()>0):
    V_Status = df.select("status").collect()[0][0]
display(V_Status)

# COMMAND ----------

mapping_df = spark.sql(f"Select * from kpi_etl_analytics_conf.CTL_TABLE_DIMS_FACTS where Target_Table ='{TableName}' order by id ")
display(mapping_df)
mapping=mapping_df.collect()

# COMMAND ----------

from collections import defaultdict
import sys
V_error_message=''
V_error_line=''
num_inserted_rows=0
num_updated_rows=0
if V_Status=='FAILED' or V_Status=='' or V_Status=='RUNNING' or V_Status=='':
    # def join_tables_logic(mapping):
    #     col_dict = defaultdict(list)

    #     from_list=[]
        
    #     driving_keys=[]
    #     join_keys=[]

    #     driving_table = "kpi_cloud_sl.hed_contact"  # Set the driving table name here

    #     for row in mapping:
    #         # print(row)
    #         src_schema_tbl = f"{row['Source_Schema']}.{row['Source_Table']}"
    #         src_col = f"{src_schema_tbl}.{row['Source_Column']}"
    #         transformation = row['Transformation']
    #         dst_col = row['Target_Column']

    #         Join_Table_Column=row['Join_Table_Column']
            
    #         if row["Is_Join_Key"] and src_schema_tbl==driving_table:
    #             driving_keys.append(src_col)
    #         elif row["Is_Join_Key"]:
    #             Join_Table=row["Join_Table"]
    #             join_keys.append(f'{Join_Table}.{Join_Table_Column}={src_col}')
            
    #         if transformation == "direct":
    #             col_dict[src_schema_tbl].append(src_col)
    #         else:
    #             transformation_expr = f"{transformation} as {dst_col}"
    #             if transformation_expr not in col_dict[src_schema_tbl]:
    #                 col_dict[src_schema_tbl].append(transformation_expr)

    #     final_select_list = []
        
    #     # Append the driving table's SELECT statement first
    #     if driving_table in col_dict:
    #         driving_select_clause = ',\n'.join(col_dict[driving_table])
    #         final_select_list.append(driving_select_clause)
    #         from_list.append(driving_table)

    #     # Append the rest of the tables' SELECT statements
    #     for table_name, columns in col_dict.items():
    #         if table_name != driving_table:  # Skip the driving table, as it's already added
    #             select_clause = ',\n'.join(columns)
    #             final_select_list.append(select_clause)
    #             from_list.append(table_name)

    #     select_stmt = "select\n"+",\n".join(map(str,final_select_list))
    #     from_stmt= "\nfrom "+",\n".join(map(str,from_list))
    #     if len(col_dict)>1:
    #         where_stmt="\nwhere\n"+"\n and ".join(map(str,join_keys))
    #     else:
    #         where_stmt=""

    #     return select_stmt+from_stmt+where_stmt


    # def union_tables_logic(mapping):
    #     col_dict = defaultdict(list)
    #     for row in mapping:
    #         src_schema_tbl = f"{row['Source_Schema']}.{row['Source_Table']}"
    #         src_col = f"{src_schema_tbl}.{row['Source_Column']}"
    #         transformation = row['Transformation']
    #         dst_col = row['Target_Column']
            
    #         if transformation == "direct":
    #             col_dict[src_schema_tbl].append(src_col)
    #         else:
    #             transformation_expr = f"{transformation} as {dst_col}"
    #             if transformation_expr not in col_dict[src_schema_tbl]:
    #                 col_dict[src_schema_tbl].append(transformation_expr)

    #     final_select_list = []
    #     driving_table = "kpi_cloud_sl.hed_contact"  # Set the driving table name here

    #     # Append the driving table's SELECT statement first
    #     if driving_table in col_dict:
    #         driving_select_clause = ',\n'.join(col_dict[driving_table])
    #         final_select_list.append(f"select\n{driving_select_clause}\nfrom {driving_table}")

    #     # Append the rest of the tables' SELECT statements
    #     for table_name, columns in col_dict.items():
    #         if table_name != driving_table:  # Skip the driving table, as it's already added
    #             select_clause = ',\n'.join(columns)
    #             final_select_list.append(f"select\n{select_clause}\nfrom {table_name}")

    #     select_stmt = '\nUNION ALL\n'.join(final_select_list)
    #     return select_stmt

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
            src_join_key=[]
            tgt_join_key=[]
            
            for row in mapping:
                Src_Schema_nm=row["Source_Schema"]
                Src_tbl_nm=row["Source_Table"]
                Src_cl_nm=row["Source_Column"]

                Dst_Schema_nm=row["Target_Schema"]
                Dst_tbl_nm=row["Target_Table"]
                Dst_cl_nm=row["Target_Column"]
            
                Is_join_cl=row["Is_Join_Key"]
                Transformation=row["Transformation"]

                if Is_join_cl:
                    # print(Is_join_cl)
                    src_join_key.append(f'{Src_Schema_nm}.{Src_tbl_nm}.{Src_cl_nm}')
                    tgt_join_key.append(Dst_cl_nm)
                # if Is_Surrogate_Key:
                #     sur_key = row["Source_Column"]

                if Transformation=="direct":
                    col_list1.append(f'{Src_cl_nm} as {Dst_cl_nm}' )
                    col_list2.append(f'target.{Dst_cl_nm}')
                    col_list3.append('source.'+Dst_cl_nm)
                    col_list4.append(f'target.{Dst_cl_nm}=source.{Src_cl_nm}')
                    col_list5.append(f'{Src_cl_nm} as {Dst_cl_nm}')
                else:
                    if f'{Dst_cl_nm}' not in col_list1:
                        col_list1.append(f'{Dst_cl_nm}')
                    if f'target.{Dst_cl_nm}' not in col_list2:
                        col_list2.append(f'target.{Dst_cl_nm}')
                    if 'source.'+Dst_cl_nm not in col_list3:
                        col_list3.append('source.'+Dst_cl_nm)
                    if f'target.{Dst_cl_nm}=source.{Dst_cl_nm}' not in col_list4:
                        col_list4.append(f'target.{Dst_cl_nm}=source.{Dst_cl_nm}')
                    if f'{Transformation} as {Dst_cl_nm}' not in col_list5:
                        col_list5.append(f'{Transformation} as {Dst_cl_nm}')

            business_keys=f",".join(map(str,src_join_key))      
            col_str1='\n,'.join(map(str,col_list1))
            col_str2='\n,'.join(map(str,col_list2))+f'\n,target.CONTACT_IK,target.CURRENT_RECORD'
            col_str3='\n,'.join(map(str,col_list3))+f"\n,source.CONTACT_IK,'Y'"
            col_str4='\n,'.join(map(str,col_list4))+f'target.{Dst_cl_nm}=source.{Dst_cl_nm}'
            col_str5='select \n'+'\n,'.join(map(str,col_list5))+f'\n from {Src_tbl_nm}'
            
            

            join_cond_str=f"target.HASH_JOIN_KEY = source.HASH_JOIN_KEY"

            insert_merge_str=f"""

    With MaxContactIK AS (
    SELECT MAX(contact_ik) AS max_contact_ik FROM {Dst_Schema_nm}.{Dst_tbl_nm} 
        )
    MERGE
    INTO {Dst_Schema_nm}.{Dst_tbl_nm} as target
    USING
    (
    select \nget_md5_hash({business_keys}) as HASH_JOIN_KEY
    ,{col_str1}
    ,row_number() OVER (ORDER BY {business_keys}) + nvl(MaxContactIK.max_contact_ik,1000) as contact_ik
    from\n{Src_Schema_nm}.{Src_tbl_nm},MaxContactIK where LastLoadDate >= '{ExtractWindowsStartDate}'
    ) source
    ON
    ({join_cond_str})
    WHEN MATCHED and target.checksum <> source.checksum and target.Current_Record='Y' then
        UPDATE SET 
        target.Effective_End_Date = (case when DATEADD(day, -1, source.Effective_Start_Date) <  target.Effective_Start_Date then target.Effective_Start_Date else DATEADD(day, -1, source.Effective_Start_Date) end),
        target.Current_Record='N'
    WHEN NOT MATCHED THEN
    INSERT
    (
    target.HASH_JOIN_KEY\n,{col_str2},target.LastLoadDate
    ) values (
    source.HASH_JOIN_KEY\n,{col_str3.replace("source.Effective_Start_Date","'1900-01-01'")},current_timestamp()
    );
                """
        
            join_update_cond_str=f"target.checksum = source.checksum"

            update_merge_str=f"""
    With MaxContactIK AS (
    SELECT MAX(contact_ik) AS max_contact_ik FROM {Dst_Schema_nm}.{Dst_tbl_nm} 
        )
    MERGE
    INTO {Dst_Schema_nm}.{Dst_tbl_nm} as target
    USING
    (
    select \nget_md5_hash({business_keys}) as HASH_JOIN_KEY
    ,{col_str1}
    ,row_number() OVER (ORDER BY {business_keys}) + nvl(MaxContactIK.max_contact_ik,1000) as contact_ik
    from\n{Src_Schema_nm}.{Src_tbl_nm},MaxContactIK where LastLoadDate >= '{ExtractWindowsStartDate}'
    ) source
    ON
    ({join_update_cond_str})
    WHEN NOT MATCHED THEN
    INSERT
    (
    target.HASH_JOIN_KEY\n,{col_str2},target.LastLoadDate
    ) values (
    source.HASH_JOIN_KEY\n,{col_str3},current_timestamp()
    );
                """
            return (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,insert_merge_str,update_merge_str)

    try:
        (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,insert_merge_str,update_merge_str)=return_merge_Statmet(mapping=mapping)

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        import hashlib

        def get_md5_hash(*args):
            concatenated_str = "".join(str(arg) for arg in args)
            md5_hash = hashlib.md5(concatenated_str.encode()).hexdigest()
            return md5_hash

        spark.udf.register("get_md5_hash", get_md5_hash, StringType())

        print(insert_merge_str)
        print(update_merge_str)

        insert_merge_df=spark.sql(insert_merge_str)
        update_merge_df=spark.sql(update_merge_str)

        #num_inserted_rows=insert_merge_df.select("num_inserted_rows").collect()[0][0]
        #num_updated_rows=update_merge_df.select("num_updated_rows").collect()[0][0]

        print(f'num_inserted_rows : {num_inserted_rows}')
        print(f'num_updated_rows : {num_updated_rows}')
    except Exception as e:
        print(e)
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno
else:
    print("Destination table already loaded!")


# COMMAND ----------

# def union_tables_logic(mapping):
#     col_dict = defaultdict(list)
#     max_column_count = 0  # Keep track of the maximum number of columns in any SELECT statement

#     # Collect columns and transformations for each table
#     for row in mapping:
#         src_schema_tbl = f"{row['Source_Schema']}.{row['Source_Table']}"
#         src_col = f"{src_schema_tbl}.{row['Source_Column']}"
#         transformation = row['Transformation']
#         dst_col = row['Target_Column']

#         if transformation == "direct":
#             col_dict[src_schema_tbl].append(f"{src_col} as {dst_col}")
#         else:
#             col_dict[src_schema_tbl].append(f"{transformation} as {dst_col}")

#         # Keep track of the maximum column count
#         current_column_count = len(col_dict[src_schema_tbl])
#         max_column_count = max(max_column_count, current_column_count)

#     # Ensure all SELECT statements have the same number of columns
#     for table_name, columns in col_dict.items():
#         current_column_count = len(columns)
#         if current_column_count < max_column_count:
#             # Add NULLs for the extra columns in the SELECT statement
#             for _ in range(max_column_count - current_column_count):
#                 col_dict[table_name].append("NULL")

#     # Build the UNION ALL statement
#     final_select_list = []
#     for table_name, columns in col_dict.items():
#         select_clause = ',\n'.join(columns)
#         final_select_list.append(f"SELECT\n{select_clause}\nFROM {table_name}")

#     select_stmt = '\nUNION ALL\n'.join(final_select_list)
#     return select_stmt

# union_mapping= spark.table("kpi_etl_analytics_conf.ctl_table_dims_facts").filter("JOIN_TYPE='UNION'").filter("Source_Column!='Sap_contactPhone_c'").collect()

# union_stmt=union_tables_logic(union_mapping)

# print(union_stmt)

# COMMAND ----------

# def add_missing_elements(list1, list2):
#     # Step 1: Find missing elements in list2
#     missing_elements = list(set(list1) - set(list2))

#     # Step 2: Get the index of each missing element in list1
#     index_dict = {elem: list1.index(elem) for elem in missing_elements}

#     # Step 3: Add the missing elements to list2 with the same index
#     for elem, index in index_dict.items():
#         list2.insert(index, elem)

#     return list2

# # Test the function
# list1 = [1, 2, 3, 4, 6]
# list2 = [1, 2, 4]

# result = add_missing_elements(list1, list2)
# print("Result:", result)


# COMMAND ----------


# join_mapping = spark.table("kpi_etl_analytics_conf.ctl_table_dims_facts").filter("JOIN_TYPE='JOIN'").collect()
# # union_mapping= spark.table("kpi_etl_analytics_conf.ctl_table_dims_facts").filter("JOIN_TYPE='UNION'").collect()
# join_stmt=join_tables_logic(join_mapping)
# # union_stmt=union_tables_logic(union_mapping)
# final_logic=join_stmt
# # print(final_logic)

# import sys
# V_error_message=''
# V_error_line=''
# num_inserted_rows=0
# num_updated_rows=0
# if V_Status=='FAILED' or V_Status=='' or V_Status=='RUNNING':
#     try:
#         (Src_Schema_nm,Src_tbl_nm,Dst_Schema_nm,Dst_tbl_nm,src_join_key,tgt_join_key,insert_merge_str,update_merge_str)=return_merge_Statmet(mapping=mapping)

#         from pyspark.sql import SparkSession
#         from pyspark.sql.functions import udf
#         from pyspark.sql.types import StringType
#         import hashlib

#         def get_md5_hash(*args):
#             concatenated_str = "".join(str(arg) for arg in args)
#             md5_hash = hashlib.md5(concatenated_str.encode()).hexdigest()
#             return md5_hash

#         spark.udf.register("get_md5_hash", get_md5_hash, StringType())

        # print(insert_merge_str)
        # print(update_merge_str)

    #     insert_merge_df=spark.sql(insert_merge_str)
    #     update_merge_df=spark.sql(update_merge_str)

    #     num_inserted_rows=insert_merge_df.select("num_inserted_rows").collect()[0][0]
    #     num_updated_rows=update_merge_df.select("num_inserted_rows").collect()[0][0]

    #     print(f'num_inserted_rows : {num_inserted_rows}')
    #     print(f'num_updated_rows : {num_updated_rows}')
    # except Exception as e:
    #     print(e)
    #     V_error_message = str(e)
    #     V_error_message = V_error_message[0:300]
    #     _, _, tb = sys.exc_info()
    #     V_error_line = tb.tb_lineno



# COMMAND ----------

# DBTITLE 1,Spark sql Approach

# try:
#     from pyspark.sql import SparkSession
#     from pyspark.sql import functions as F
#     from pyspark.sql.window import Window
#     import sys

#     V_error_message = ""
#     V_error_line=""
#     num_inserted_rows=0
#     num_updated_rows=0

#     # Create a mapping between the silver and the Gold layer column names
#     column_mapping = {
#         "Hed_contactID_c": "ContactID",
#         "Hed_Choosen_full_name_c": "FullName",
#         "Hed_contactGender_c": "Gender",
#         "Hed_contactEmail_c": "Email",
#         "Hed_contactPhone_c": "Phone",
#         "Hed_LastLoadDate_c":"Effective_Start_Date"
#     }
    
#     #src_tbl_nm=''
#     #tgt_tbl_nm=''

#     # Read the silver layer DataFrame and format the date column
#     silver_df = spark.table("kpi_cloud_sl.hed_contact").withColumn("Hed_LastLoadDate_c", F.date_format("Hed_LastLoadDate_c", "yyyy-MM-dd"))

#     print(f"{silver_df.count()} source records are ready to compare with target table\n")


#     # Rename the columns in the DataFrame based on the mapping
#     for original_col, gold_col in column_mapping.items():
#         silver_df = silver_df.withColumnRenamed(original_col, gold_col)

#     # Define columns to exclude from the checksum calculation
#     columns_to_exclude = ['Effective_Start_Date', 'Effective_End_Date', 'Current_Record','Hed_LastLoadDate_c']

#     # # Calculate the checksum for each row based on the specified columns
#     # silver_df = silver_df.withColumn(
#     #     "checksum",
#     #     F.sha2(
#     #         F.concat(
#     #             *[F.coalesce(silver_df[col], F.lit("")) for col in silver_df.columns if col not in columns_to_exclude]
#     #         ),
#     #         256  # Specify the SHA-2 hash length (in this case, 256 bits)
#     #     )
#     # )

#     display(silver_df)

#     silver_df.createOrReplaceTempView("hed_contact")


#     insert_df=spark.sql("""
#     With MaxContactIK AS (
#     SELECT MAX(contact_ik) AS max_contact_ik FROM kpi_cloud_gl.dim_contact
#     )
    
#     MERGE INTO kpi_cloud_gl.dim_contact AS target
#     USING (
#     SELECT 
#             ContactID,
#             FullName,
#             Gender,
#             Email,
#             Phone,
#             Effective_Start_Date,
#             DataSourceID,
#             checksum,
#             row_number() OVER (ORDER BY ContactID) + nvl(MaxContactIK.max_contact_ik,1000) as contact_ik
#     FROM hed_contact,MaxContactIK
#     ) AS source
#     ON target.ContactID = source.ContactID

#     WHEN MATCHED and target.checksum <> source.checksum and target.Current_Record='Y' then
#         UPDATE SET 
#         target.Effective_End_Date = (case when DATEADD(day, -1, source.Effective_Start_Date) <  source.Effective_Start_Date then source.Effective_Start_Date else DATEADD(day, -1, source.Effective_Start_Date) end),
#         target.Current_Record='N'

#     WHEN NOT MATCHED THEN
#     -- Insert a new record with Effective_Start_Date and Effective_End_Date set to indicate it's the current record
#     INSERT (contact_ik,ContactID, FullName, Gender, Email, Phone, Effective_Start_Date, Effective_End_Date, Current_Record,DataSourceID,checksum)
#     VALUES (source.contact_ik,source.ContactID, source.FullName, source.Gender, source.Email, source.Phone,"1900-01-01", null,'Y',source.DataSourceID,source.checksum)
#     """)

#     update_df=spark.sql("""
    
#      With MaxContactIK AS (
#     SELECT MAX(contact_ik) AS max_contact_ik FROM kpi_cloud_gl.dim_contact
#     )
    
#     MERGE INTO kpi_cloud_gl.dim_contact AS target
#     USING (
#     SELECT 
#             ContactID,
#             FullName,
#             Gender,
#             Email,
#             Phone,
#             Effective_Start_Date,
#             DataSourceID,
#             checksum,
#             row_number() OVER (ORDER BY ContactID) + MaxContactIK.max_contact_ik as contact_ik
#     FROM hed_contact,MaxContactIK
#     ) AS source
#     ON target.checksum = source.checksum
#     WHEN NOT MATCHED THEN
#     -- Insert a new record with Effective_Start_Date and Effective_End_Date set to indicate it's the current record
#     INSERT (contact_ik,ContactID, FullName, Gender, Email, Phone, Effective_Start_Date, Effective_End_Date, Current_Record,DataSourceID,checksum)
#     VALUES (source.contact_ik,source.ContactID, source.FullName, source.Gender, source.Email, source.Phone, source.Effective_Start_Date, null,'Y',source.DataSourceID,source.checksum)
#     """)

#     num_inserted_rows=insert_df.select("num_inserted_rows").collect()[0][0]
#     num_updated_rows=update_df.select("num_updated_rows").collect()[0][0]

# except Exception as e:
#     print(e)
#     V_error_message = str(e)
#     V_error_message = V_error_message[0:300]
#     _, _, tb = sys.exc_info()
#     V_error_line = tb.tb_lineno

# COMMAND ----------

V_NewJobId=0
result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM kpi_etl_analytics_conf.etl_job_history""")
V_NewJobId = result_df.collect()[0]['JobId_NEW']
print(V_NewJobId)

# COMMAND ----------

import json
result={"ErrorMessage": V_error_message,"ErrorLine":V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows,"JobId":V_NewJobId}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  kpi_cloud_gl.dim_contact 
