# Databricks notebook source
dbutils.widgets.text("TargetTableName", "")
dbutils.widgets.text("SourceTableName", "")
dbutils.widgets.text("BatchId", "")
dbutils.widgets.text("SourceSchemaName", "")
dbutils.widgets.text("TargetSchemaName", "")

# COMMAND ----------

tgt_tbl_nm = dbutils.widgets.get("TargetTableName")
SourceTableName=dbutils.widgets.get("SourceTableName")
V_BatchId = dbutils.widgets.get("BatchId")
V_SourceSchemaName = dbutils.widgets.get("SourceSchemaName")
V_TargetSchemaName = dbutils.widgets.get("TargetSchemaName")
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

# COMMAND ----------

wquery = f"""SELECT ExecuteBeginTS FROM {V_BatchLogTable} WHERE BatchName ='DIM_FACT_LOAD' and RetryAttempt =(SELECT MIN(RetryAttempt) FROM kpi_etl_analytics_conf.etl_batch_history WHERE BatchName = 'DIM_FACT_LOAD') ORDER BY ExecuteBeginTS DESC LIMIT 1"""
display(wquery)
df = spark.sql(wquery)
if df.count() > 0:
    ExtractWindowsStartDate = df.select("ExecuteBeginTS").collect()[0][0]
    print(ExtractWindowsStartDate)

# COMMAND ----------

V_Status=''
st=f"""SELECT status FROM {V_ProcessLogTable} WHERE BatchID = '{V_BatchId}' AND TableName = '{V_TargetSchemaName}.{tgt_tbl_nm}'"""
display(st)
df= spark.sql(st)
if(df.count()>0):
    V_Status = df.select("status").collect()[0][0]
display(V_Status)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import functions as F
import sys
V_error_message = ""
V_error_line=""
num_inserted_rows=0
num_updated_rows=0
if V_Status=='FAILED' or V_Status=='' or V_Status=='RUNNING' or V_Status=='':
    try:
        # Load the dimension table (dim_contact) into a DataFrame.
        dim_table_name = f"{V_SourceSchemaName}.{SourceTableName}"
        dim_df = spark.sql(f"""select * from {dim_table_name} where lastLoadDate >= '2023-08-30 12:08:36.419000'""")
        #dim_df = dim_df.filter((col("Current_Record") == "Y") | (F.isnull(col("Current_Record"))))

        def create_column_mapping(mapping_df: DataFrame):
            # Create a column mapping dictionary from the DataFrame
            column_mapping = dict((row['Source_Column'], row['Target_Column']) for row in mapping_df.collect())
            return column_mapping

        def upsert_into_fact_table(source_df: DataFrame, fact_table_name: str, column_mapping: dict):
            # Register the DataFrames as temporary views
            source_df.createOrReplaceTempView("dim_table")
            #display(source_df)
            # Filter for join condition columns
            join_condition_df = mapping_df.filter(mapping_df.Is_Join_Key == "true")
            display(join_condition_df)

            # Construct join conditions dynamically
            join_conditions = []
            for row in join_condition_df.collect():
                join_condition = f"s.{row.Source_Column} = f.{row.Target_Column}"
                join_conditions.append(join_condition)

            # Construct the final join condition string
            final_join_condition = " AND ".join(join_conditions)

            print("Final Join Condition:", final_join_condition)

            # Define the SQL MERGE INTO statement
            insert_columns = [Target_Column for _, Target_Column in column_mapping.items()]
            print(insert_columns)
            insert_values = ', '.join([f's.{col}' for col in column_mapping.keys()])
            insert_columns.append("LastLoadDate")
            insert_values = insert_values + ", current_timestamp()"
            display(insert_values)

            merge_query = f"""
                MERGE INTO {fact_table_name} AS f
                USING dim_table AS s
                ON {final_join_condition}
                WHEN MATCHED AND ({' OR '.join([f'f.{Target_Column} != s.{Source_Column}' for Source_Column, Target_Column in column_mapping.items() if Target_Column != 'Dim_Contact_sk'])})
                THEN UPDATE SET {', '.join([f'f.{Target_Column} = s.{Source_Column}' for Source_Column, Target_Column in column_mapping.items() if Target_Column != 'Dim_Contact_sk'])},f.LastLoadDate = current_timestamp()
                WHEN NOT MATCHED
                THEN INSERT ({', '.join(insert_columns)})
                VALUES ({insert_values})
            """

            display(merge_query)
            # Execute the merge query
            result_df = spark.sql(merge_query)
            display(result_df)
            # Calculate updated_count and inserted_count
            num_inserted_rows=result_df.select("num_inserted_rows").collect()[0][0]
            num_updated_rows=result_df.select("num_updated_rows").collect()[0][0]
            print(f'num_inserted_rows : {num_inserted_rows}')
            print(f'num_updated_rows : {num_updated_rows}')
            return num_inserted_rows,num_updated_rows

        mapping_df = spark.sql(f"Select * from kpi_etl_analytics_conf.CTL_TABLE_DIMS_FACTS where Source_Table ='{SourceTableName}' order by id ")

        # Create the column mapping dictionary from the DataFrame
        column_mapping = create_column_mapping(mapping_df)

        # Use the function to perform the upsert operation
        fact_table_name = f"{V_TargetSchemaName}.{tgt_tbl_nm}"
        num_inserted_rows,num_updated_rows = upsert_into_fact_table(dim_df, fact_table_name, column_mapping)

    except Exception as e:
        print(e)
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno
else:
    print("Destination table already loaded!")


# COMMAND ----------

V_NewJobId=0
result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM {V_JobLogTable}""")
V_NewJobId = result_df.collect()[0]['JobId_NEW']
print(V_NewJobId)

# COMMAND ----------

import json
result={"ErrorMessage": V_error_message, "ErrorLine": V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows,"JobId":V_NewJobId}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)

# COMMAND ----------

#Previous Approach
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col
# from pyspark.sql.functions import current_timestamp

# # Load the dimension table (dim_contact) into a DataFrame.
# dim_df = spark.table("kpi_cloud_gl.dim_contact")
# dim_df = dim_df.filter(col("Current_Record") == "Y")
# dim_df = dim_df.withColumn("LastInserted_date", current_timestamp())
# dim_df = dim_df.withColumn("LastUpdated_date", current_timestamp())
# display(dim_df)
# #dim_contact_sk
# # Define the columns to be updated/inserted in the fact table
# update_columns = ["FullName", "Gender", "Phone", "Email"]
# insert_columns = ["Dim_Contact_sk", "ContactID", "FullName", "Gender", "Phone", "Email"]

# def upsert_into_fact_table(source_df: DataFrame, fact_table_name: str, update_cols: list, insert_cols: list):
#     # Register the DataFrames as temporary views
#     source_df.createOrReplaceTempView("contact_dim")

#     # Define the SQL MERGE INTO statement
#     update_clause = ', '.join([f'f.{col} = s.{col}' for col in update_cols])
#     insert_values = ', '.join([f's.{col}' for col in insert_cols])

#     merge_query = f"""
#     MERGE INTO {fact_table_name} AS f
#     USING contact_dim AS s
#     ON f.Dim_Contact_sk = s.Contact_ik
#     WHEN MATCHED AND (f.FullName != s.FullName OR f.Gender != s.Gender OR f.Phone != s.Phone OR f.Email != s.Email) 
#     THEN UPDATE SET {update_clause}, LastUpdated_date = CURRENT_TIMESTAMP()
#     WHEN NOT MATCHED
#       THEN INSERT ({', '.join(insert_cols + ["LastInserted_date", "LastUpdated_date"])})
#       VALUES ({insert_values}, CURRENT_TIMESTAMP(), NULL)
#     """

#     # Execute the merge query
#     display(merge_query)
#     spark.sql(merge_query)

# # Use the function to perform the upsert operation
# fact_table_name = "kpi_cloud_gl.Fact_Contact"  # Replace with the actual name of your fact table
# upsert_into_fact_table(dim_df, fact_table_name, update_columns, insert_columns)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_gl.fact_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_cloud_gl.dim_contact
