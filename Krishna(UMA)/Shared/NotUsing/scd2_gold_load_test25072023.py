# Databricks notebook source
# Define input widgets
dbutils.widgets.text("TableName", "")
dbutils.widgets.text("ExtractWindowsStartDate", "")

# COMMAND ----------

tbl_nm = dbutils.widgets.get("TableName")
ExtractWindowsStartDate = dbutils.widgets.get("ExtractWindowsStartDate")

V_error_message = ""
V_error_line=""
num_inserted_rows=0
num_updated_rows=0

# COMMAND ----------

# DBTITLE 1,Spark sql Approach

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    import sys

    V_error_message = ""
    V_error_line=""

    # Create a mapping between the silver and the Gold layer column names
    column_mapping = {
        "Hed_contactID_c": "ContactID",
        "Hed_Choosen_full_name_c": "FullName",
        "Hed_contactGender_c": "Gender",
        "Hed_contactEmail_c": "Email",
        "Hed_contactPhone_c": "Phone",
        "Hed_LastLoadDate_c":"Effective_Start_Date"
    }

    # Read the silver layer DataFrame and format the date column
    silver_df = spark.table("kpi_cloud_sl.hed_contact").withColumn("Hed_LastLoadDate_c", F.date_format("Hed_LastLoadDate_c", "yyyy-MM-dd"))

    print(f"{silver_df.count()} source records are ready to compare with target table\n")


    # Rename the columns in the DataFrame based on the mapping
    for original_col, gold_col in column_mapping.items():
        silver_df = silver_df.withColumnRenamed(original_col, gold_col)

    # Define columns to exclude from the checksum calculation
    columns_to_exclude = ['Effective_Start_Date', 'Effective_End_Date', 'DataSourceID', 'Current_Record','Hed_LastLoadDate_c']

    # Calculate the checksum for each row based on the specified columns
    silver_df = silver_df.withColumn(
        "checksum",
        F.sha2(
            F.concat(
                *[F.coalesce(silver_df[col], F.lit("")) for col in silver_df.columns if col not in columns_to_exclude]
            ),
            256  # Specify the SHA-2 hash length (in this case, 256 bits)
        )
    )

    # display(silver_df)

    silver_df.createOrReplaceTempView("hed_contact")


    insert_df=spark.sql("""
    With MaxContactIK AS (
    SELECT MAX(contact_ik) AS max_contact_ik FROM kpi_cloud_gl.dim_contact
    )
    MERGE INTO kpi_cloud_gl.dim_contact AS target
    USING (
    SELECT 
            ContactID,
            FullName,
            Gender,
            Email,
            Phone,
            Effective_Start_Date,
            DataSourceID,
            checksum,
            checksum as checksum2,
            row_number() OVER (ORDER BY ContactID) + nvl(MaxContactIK.max_contact_ik,1000) as contact_ik
    FROM hed_contact,MaxContactIK
    ) AS source
    ON target.ContactID = source.ContactID

    WHEN MATCHED and target.checksum <> source.checksum and target.Current_Record='Y' then
        UPDATE SET 
        target.Effective_End_Date = (case when DATEADD(day, -1, source.Effective_Start_Date) <  source.Effective_Start_Date then source.Effective_Start_Date else DATEADD(day, -1, source.Effective_Start_Date) end),
        target.Current_Record='N'

    WHEN NOT MATCHED THEN
    -- Insert a new record with Effective_Start_Date and Effective_End_Date set to indicate it's the current record
    INSERT (contact_ik,ContactID, FullName, Gender, Email, Phone, Effective_Start_Date, Effective_End_Date, Current_Record,DataSourceID,checksum)
    VALUES (source.contact_ik,source.ContactID, source.FullName, source.Gender, source.Email, source.Phone,"1900-01-01", null,'Y',source.DataSourceID,source.checksum)
    """)

    update_df=spark.sql("""
    
     With MaxContactIK AS (
    SELECT MAX(contact_ik) AS max_contact_ik FROM kpi_cloud_gl.dim_contact
    )
    
    MERGE INTO kpi_cloud_gl.dim_contact AS target
    USING (
    SELECT 
            ContactID,
            FullName,
            Gender,
            Email,
            Phone,
            Effective_Start_Date,
            DataSourceID,
            checksum,
            row_number() OVER (ORDER BY ContactID) + MaxContactIK.max_contact_ik as contact_ik
    FROM hed_contact,MaxContactIK
    ) AS source
    ON target.checksum = source.checksum
    WHEN NOT MATCHED THEN
    -- Insert a new record with Effective_Start_Date and Effective_End_Date set to indicate it's the current record
    INSERT (contact_ik,ContactID, FullName, Gender, Email, Phone, Effective_Start_Date, Effective_End_Date, Current_Record,DataSourceID,checksum)
    VALUES (source.contact_ik,source.ContactID, source.FullName, source.Gender, source.Email, source.Phone, source.Effective_Start_Date, null,'Y',source.DataSourceID,source.checksum)
    """)

    num_inserted_rows=insert_df.select("num_inserted_rows").collect()[0][0]
    num_updated_rows=update_df.select("num_updated_rows").collect()[0][0]

    #print(f)
    # display(update_surrogate_key)
except Exception as e:
        V_error_message = str(e)
        V_error_message = V_error_message[0:300]
        display(V_error_message)
        _, _, tb = sys.exc_info()
        V_error_line = tb.tb_lineno

# COMMAND ----------

import json
result={"ErrorMessage": V_error_message, "ErrorLine": V_error_line, "InsertedRecordCount": num_inserted_rows, "UpdatedRecordCount": num_updated_rows}
result_json = json.dumps(result)
dbutils.notebook.exit(result_json)

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

