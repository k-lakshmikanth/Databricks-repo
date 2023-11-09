# Databricks notebook source
# MAGIC %run /Shared/Common/mountfs

# COMMAND ----------

for i in dbutils.fs.ls("/mnt/adls_landing/metadata/"):
    for j in dbutils.fs.ls(i.path):
        dbutils.fs.rm(j.path, recurse=True)
    dbutils.fs.rm(i.path,recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kpi_etl_analytics_conf.ctl_table_sync;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists kpi_etl_analytics_conf.ctl_table_sync;
# MAGIC CREATE TABLE kpi_etl_analytics_conf.ctl_table_sync (
# MAGIC     TableName VARCHAR(255),
# MAGIC     WatermarkValue VARCHAR(255),
# MAGIC     ActiveFlag VARCHAR(255),
# MAGIC     EffectiveDateColumnName VARCHAR(255),
# MAGIC     SchemaName VARCHAR(255),
# MAGIC     SourceName VARCHAR(255),
# MAGIC     ColumnList VARCHAR(255),
# MAGIC     DestinationSchema VARCHAR(255),
# MAGIC     DestinationTable varchar(255),
# MAGIC     IsAlwaysFullLoad VARCHAR(25),
# MAGIC     LoadType VARCHAR(25),
# MAGIC     SourceType varchar(25)
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (SourceName)
# MAGIC LOCATION 'dbfs:/mnt/adls_landing/metadata/ctl_table_sync';
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC PARTITION (SourceName='source1')
# MAGIC (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema, DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC VALUES ('Contact', '1900-01-01T00:00:00.000Z', 'Y', null, 'source1', 'select * from source1.contact', 'kpi_cloud_bl','source1_contact','N','F','NonCDC');
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC PARTITION (SourceName='source1')
# MAGIC (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema, DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC VALUES ('Contact', '1900-01-01T00:00:00.000Z', 'Y', null, 'source1', 'SELECT * FROM cdc.fn_cdc_get_all_changes_Source1_Contact_new', 'kpi_cloud_bl','source1_contact_CDC','N','F','CDC');
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC PARTITION (SourceName='source2')
# MAGIC (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema,DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC VALUES ('Contact', '1900-01-01T00:00:00.000Z', 'Y', null, 'source2', 'select * from source2.contact', 'kpi_cloud_bl','source2_contact','N','F','NonCDC');
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC PARTITION (SourceName='source3')
# MAGIC (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema,DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC VALUES ('Ext_Contact', '1900-01-01T00:00:00.000Z', 'Y', null, 'source3', 'select * from source3.Ext_contact', 'kpi_cloud_bl','source3_ext_contact','N','F','NonCDC');
# MAGIC
# MAGIC -- INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC -- PARTITION (SourceName='source4')
# MAGIC -- (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema,DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC -- VALUES ('Contact', '1900-01-01T00:00:00.000Z', 'Y', null, 'source4', 'https://testpoc28082023.azurewebsites.net/api/webapi', 'kpi_cloud_bl','source4_contact','N','F','API');
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC PARTITION (SourceName='APIData')
# MAGIC (TableName, WatermarkValue, ActiveFlag, EffectiveDateColumnName, SchemaName, ColumnList, DestinationSchema,DestinationTable, IsAlwaysFullLoad, LoadType,SourceType)
# MAGIC VALUES ('Entries', '1900-01-01T00:00:00.000Z', 'Y', null, 'APIData', 'https://api.publicapis.org/entries', 'kpi_cloud_bl','APIData_Entries','N','F','Y');
# MAGIC
# MAGIC

# COMMAND ----------

# spark.sql("TRUNCATE TABLE kpi_etl_analytics_conf.ctl_table_sync")

# COMMAND ----------

# MAGIC %sql
# MAGIC --delete from kpi_etl_analytics_conf.ctl_table_sync where SourceName='APIData'
# MAGIC select * from kpi_etl_analytics_conf.ctl_table_sync 
# MAGIC --update kpi_etl_analytics_conf.ctl_table_sync set LoadType='F' where IsAlwaysFullLoad='N'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP table if exists kpi_etl_analytics_conf.ctl_source_bl_mapping ;
# MAGIC
# MAGIC CREATE TABLE kpi_etl_analytics_conf.ctl_source_bl_mapping 
# MAGIC (
# MAGIC     Mapping_Id INT,
# MAGIC     Src_Schema_nm VARCHAR(50),
# MAGIC     Src_tbl_nm VARCHAR(50),
# MAGIC     Src_cl_nm VARCHAR(50),
# MAGIC     Src_nm VARCHAR(50),
# MAGIC     Dst_Schema_nm VARCHAR(50),
# MAGIC     Dst_tbl_nm VARCHAR(50),
# MAGIC     Dst_cl_nm VARCHAR(50),
# MAGIC     Is_join_cl BOOLEAN,
# MAGIC     Transformation VARCHAR(250)
# MAGIC ) USING DELTA LOCATION 'dbfs:/mnt/adls_landing/metadata/ctl_source_bl_mapping';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from kpi_etl_analytics_conf.ctl_source_bl_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_source_bl_mapping (Mapping_ID, Src_Schema_nm, Src_tbl_nm, Src_cl_nm, Src_nm, Dst_Schema_nm, Dst_tbl_nm, Dst_cl_nm, Is_join_cl,Transformation)
# MAGIC VALUES
# MAGIC   (1, 'Source1', 'Contact', 'contactID', 'source1', 'kpi_cloud_bl', 'source1_contact', 'contactID', 'Y','direct'),
# MAGIC   (2, 'Source1', 'Contact', 'contactName', 'source1', 'kpi_cloud_bl', 'source1_contact', 'contactName', 'N','direct'),
# MAGIC   (3, 'Source1', 'Contact', 'contactGender', 'source1', 'kpi_cloud_bl', 'source1_contact', 'contactGender', 'N','direct'),
# MAGIC   (4, 'Source1', 'Contact', 'contactEmail', 'source1', 'kpi_cloud_bl', 'source1_contact', 'contactEmail', 'N','direct'),
# MAGIC   (5, 'Source1', 'Contact', 'contactPhone', 'source1', 'kpi_cloud_bl', 'source1_contact', 'contactPhone', 'N','direct'),
# MAGIC   (6, 'Source1', 'Contact', 'LastLoadDate', 'source1', 'kpi_cloud_bl', 'source1_contact', 'LastLoadDate', 'N','direct'),
# MAGIC   (7, 'Source2', 'Contact', 'contactID', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactID', 'Y','direct'),
# MAGIC   (8, 'Source2', 'Contact', 'contactFirstName', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactFirstName', 'N','direct'),
# MAGIC   (9, 'Source2', 'Contact', 'contactLastName', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactLastName', 'N','direct'),
# MAGIC   (10, 'Source2', 'Contact', 'contactGender', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactGender', 'N','direct'),
# MAGIC   (11, 'Source2', 'Contact', 'contactEmail', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactEmail', 'N','direct'),
# MAGIC   (12, 'Source2', 'Contact', 'contactPhone', 'source2', 'kpi_cloud_bl', 'source2_contact', 'contactPhone', 'N','direct'),
# MAGIC   (13, 'Source2', 'Contact', 'LastLoadDate', 'source2', 'kpi_cloud_bl', 'source2_contact', 'LastLoadDate', 'N','direct'),
# MAGIC   (14, 'Source3', 'Ext_Contact', 'contactID', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactID', 'Y','direct'),
# MAGIC   (15, 'Source3', 'Ext_Contact', 'contactFirstName', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactFirstName', 'N','direct'),
# MAGIC   (16, 'Source3', 'Ext_Contact', 'contactLastName', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactLastName', 'N','direct'),
# MAGIC   (17, 'Source3', 'Ext_Contact', 'contactGender', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactGender', 'N','direct'),
# MAGIC   (18, 'Source3', 'Ext_Contact', 'contactAge', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactAge', 'N','direct'),
# MAGIC   (19, 'Source3', 'Ext_Contact', 'contactEthnicity', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactEthnicity', 'N','direct'),
# MAGIC   (20, 'Source3', 'Ext_Contact', 'contactEmail', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactEmail', 'N','direct'),
# MAGIC   (21, 'Source3', 'Ext_Contact', 'contactPhone', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactPhone', 'N','direct'),
# MAGIC   (22, 'Source3', 'Ext_Contact', 'LastLoadDate', 'source3', 'kpi_cloud_bl', 'Source3_Ext_contact', 'LastLoadDate', 'N','direct')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_source_bl_mapping (Mapping_ID, Src_Schema_nm, Src_tbl_nm, Src_cl_nm, Src_nm, Dst_Schema_nm, Dst_tbl_nm, Dst_cl_nm, Is_join_cl,Transformation)
# MAGIC VALUES
# MAGIC   -- (23, 'Source4', 'Contact', 'contactID', 'source4', 'kpi_cloud_bl', 'source4_contact', 'contactID', 'Y','direct'),
# MAGIC   -- (24, 'Source4', 'Contact', 'contactName', 'source4', 'kpi_cloud_bl', 'source4_contact', 'contactName', 'N','direct'),
# MAGIC   -- (25, 'Source4', 'Contact', 'contactGender', 'source4', 'kpi_cloud_bl', 'source4_contact', 'contactGender', 'N','direct'),
# MAGIC   -- (26, 'Source4', 'Contact', 'contactEmail', 'source4', 'kpi_cloud_bl', 'source4_contact', 'contactEmail', 'N','direct'),
# MAGIC   -- (27, 'Source4', 'Contact', 'contactPhone', 'source4', 'kpi_cloud_bl', 'source4_contact', 'contactPhone', 'N','direct'),
# MAGIC   -- (28, 'Source4', 'Contact', 'LastLoadDate', 'source4', 'kpi_cloud_bl', 'source4_contact', 'LastLoadDate', 'N','direct');
# MAGIC   (29, 'APIData', 'Entries', 'API', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'API', 'Y','direct'),
# MAGIC   (30, 'APIData', 'Entries', 'Description', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'Description', 'N','direct'),
# MAGIC   (31, 'APIData', 'Entries', 'Auth', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'Auth', 'N','direct'),
# MAGIC   (32, 'APIData', 'Entries', 'HTTPS', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'HTTPS', 'N','direct'),
# MAGIC   (33, 'APIData', 'Entries', 'Cors', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'Cors', 'N','direct'),
# MAGIC   (34, 'APIData', 'Entries', 'Link', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'Link', 'N','direct'),
# MAGIC   (35, 'APIData', 'Entries', 'Category', 'APIData', 'kpi_cloud_bl', 'APIData_Entries', 'Category', 'N','direct');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_etl_analytics_conf.ctl_source_bl_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists kpi_etl_analytics_conf.ctl_bl_sl_mapping ;
# MAGIC CREATE TABLE kpi_etl_analytics_conf.ctl_bl_sl_mapping 
# MAGIC (
# MAGIC     Mapping_Id INT,
# MAGIC     Src_Schema_nm VARCHAR(50),
# MAGIC     Src_tbl_nm VARCHAR(50),
# MAGIC     Src_cl_nm VARCHAR(50),
# MAGIC     Src_nm VARCHAR(50),
# MAGIC     Dst_Schema_nm VARCHAR(50),
# MAGIC     Dst_tbl_nm VARCHAR(50),
# MAGIC     Dst_cl_nm VARCHAR(50),
# MAGIC     Is_join_cl BOOLEAN,
# MAGIC     Transformation VARCHAR(250)
# MAGIC ) USING DELTA LOCATION 'dbfs:/mnt/adls_landing/metadata/ctl_bl_sl_mapping';
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_bl_sl_mapping (Mapping_ID, Src_Schema_nm, Src_tbl_nm, Src_cl_nm, Src_nm, Dst_Schema_nm, Dst_tbl_nm, Dst_cl_nm, Is_join_cl,Transformation)
# MAGIC VALUES
# MAGIC   (1, 'kpi_cloud_bl', 'source1_contact', 'contactID', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactID_c', 'Y','direct'),
# MAGIC   (2, 'kpi_cloud_bl', 'source1_contact', 'contactName', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N','direct'),
# MAGIC   (3, 'kpi_cloud_bl', 'source1_contact', 'contactGender', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactGender_c', 'N','direct'),
# MAGIC   (4, 'kpi_cloud_bl', 'source1_contact', 'contactEmail', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactEmail_c', 'N','direct'),
# MAGIC   (5, 'kpi_cloud_bl', 'source1_contact', 'contactPhone', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactPhone_c', 'N','direct'),
# MAGIC   (6, 'kpi_cloud_bl', 'source1_contact', 'LastLoadDate', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_LastLoadDate_c', 'N','direct'),
# MAGIC   (7, 'kpi_cloud_bl', 'source1_contact', 'DataSourceID', 'source1', 'kpi_cloud_sl', 'Hed_Contact', 'DataSourceID', 'N','direct'),
# MAGIC   (8, 'kpi_cloud_bl', 'source2_contact', 'contactID', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactID_c', 'Y','direct'),
# MAGIC   (9, 'kpi_cloud_bl', 'source2_contact', 'contactFirstName', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N',"concat(kpi_cloud_bl.source2_contact.contactFirstName,' ',kpi_cloud_bl.source2_contact.ContactLastName)"),
# MAGIC   (10, 'kpi_cloud_bl', 'source2_contact', 'contactLastName', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N',"concat(kpi_cloud_bl.source2_contact.contactFirstName,' ',kpi_cloud_bl.source2_contact.ContactLastName)"),
# MAGIC   (11, 'kpi_cloud_bl', 'source2_contact', 'contactGender', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactGender_c', 'N','direct'),
# MAGIC   (12,'kpi_cloud_bl', 'source2_contact', 'contactEmail', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactEmail_c', 'N','direct'),
# MAGIC   (13,'kpi_cloud_bl', 'source2_contact', 'contactPhone', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactPhone_c', 'N','direct'),
# MAGIC   (14, 'kpi_cloud_bl', 'source2_contact', 'LastLoadDate', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_LastLoadDate_c', 'N','direct'),
# MAGIC   (15, 'kpi_cloud_bl', 'source2_contact', 'DataSourceID', 'source2', 'kpi_cloud_sl', 'Hed_Contact', 'DataSourceID', 'N','direct'),
# MAGIC   (16, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactID', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactID_c', 'Y','direct'),
# MAGIC   (17, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactFirstName', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N',"concat(kpi_cloud_bl.Source3_Ext_contact.contactFirstName,' ',kpi_cloud_bl.Source3_Ext_contact.ContactLastName)"),
# MAGIC   (18, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactLastName', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N',"concat(kpi_cloud_bl.Source3_Ext_contact.contactFirstName,' ',kpi_cloud_bl.Source3_Ext_contact.ContactLastName)");

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_bl_sl_mapping (Mapping_ID, Src_Schema_nm, Src_tbl_nm, Src_cl_nm, Src_nm, Dst_Schema_nm, Dst_tbl_nm, Dst_cl_nm, Is_join_cl,Transformation)
# MAGIC VALUES
# MAGIC   (19, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactGender', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactGender_c', 'N','direct'),
# MAGIC   (20, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactAge', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactAge_c', 'N','direct'),
# MAGIC   (21, 'kpi_cloud_bl', 'Source3_Ext_contact', 'ContactEthnicity', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_ContactEthnicity_c', 'N','direct'),
# MAGIC   (22, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactEmail', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactEmail_c', 'N','direct'),
# MAGIC   (23, 'kpi_cloud_bl', 'Source3_Ext_contact', 'contactPhone', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactPhone_c', 'N','direct'),
# MAGIC   (24, 'kpi_cloud_bl', 'Source3_Ext_contact', 'LastLoadDate', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_LastLoadDate_c', 'N','direct'),
# MAGIC   (25, 'kpi_cloud_bl', 'Source3_Ext_contact', 'DataSourceID', 'source3', 'kpi_cloud_sl', 'Hed_Contact', 'DataSourceID', 'N','direct'),
# MAGIC   (26, 'kpi_cloud_bl', 'source4_contact', 'contactID', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactID_c', 'Y','direct'),
# MAGIC   (27, 'kpi_cloud_bl', 'source4_contact', 'contactName', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_Choosen_full_name_c', 'N','direct'),
# MAGIC   (28, 'kpi_cloud_bl', 'source4_contact', 'contactGender', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactGender_c', 'N','direct'),
# MAGIC   (29, 'kpi_cloud_bl', 'source4_contact', 'contactEmail', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactEmail_c', 'N','direct'),
# MAGIC   (30, 'kpi_cloud_bl', 'source4_contact', 'contactPhone', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_contactPhone_c', 'N','direct'),
# MAGIC   (31, 'kpi_cloud_bl', 'source4_contact', 'LastLoadDate', 'source4', 'kpi_cloud_sl', 'Hed_Contact', 'Hed_LastLoadDate_c', 'N','direct');
# MAGIC   -- (32, 'kpi_cloud_bl', 'APIData_Entries', 'API', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_API', 'Y','direct'),
# MAGIC   -- (33, 'kpi_cloud_bl', 'APIData_Entries', 'Description', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Description', 'N','direct'),
# MAGIC   -- (34, 'kpi_cloud_bl', 'APIData_Entries', 'Auth', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Auth', 'N','direct'),
# MAGIC   -- (35, 'kpi_cloud_bl', 'APIData_Entries', 'HTTPS', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_HTTPS', 'N','direct'),
# MAGIC   -- (36, 'kpi_cloud_bl', 'APIData_Entries', 'Cors', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Cors', 'N','direct'),
# MAGIC   -- (37, 'kpi_cloud_bl', 'APIData_Entries', 'Link', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Link', 'N','direct'),
# MAGIC   -- (38, 'kpi_cloud_bl', 'APIData_Entries', 'Category', 'source4', 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Category', 'N','direct')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_etl_analytics_conf.ctl_bl_sl_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kpi_etl_analytics_conf.etl_batch_history_test;
# MAGIC
# MAGIC CREATE TABLE kpi_etl_analytics_conf.etl_batch_history_test
# MAGIC (
# MAGIC 	BatchID VARCHAR(255) NOT NULL,
# MAGIC 	JobID INT,
# MAGIC 	BatchName varchar(255) ,
# MAGIC 	ExecuteBeginTS TIMESTAMP ,
# MAGIC 	ExecuteEndTS TIMESTAMP ,
# MAGIC 	ExtractWindowBeginTS TIMESTAMP ,
# MAGIC 	ExtractWindowEndTS TIMESTAMP ,
# MAGIC 	Status varchar(25) ,
# MAGIC 	ErrorLine varchar(25) ,
# MAGIC 	ErrorDescription varchar(500) ,
# MAGIC 	LoadType varchar(20),
# MAGIC 	RetryAttempt INT
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (BatchName,ExecuteBeginTS) 
# MAGIC LOCATION 'dbfs:/mnt/adls_landing/metadata/etl_batch_history_test';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended 
# MAGIC kpi_etl_analytics_conf.etl_batch_history_test

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history kpi_etl_analytics_conf.etl_batch_history_test

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /dbfs/mnt/adls_landing/metadata/etl_batch_history_test/

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kpi_etl_analytics_conf.etl_batch_history;
# MAGIC
# MAGIC CREATE TABLE kpi_etl_analytics_conf.etl_batch_history
# MAGIC (
# MAGIC 	BatchID int NOT NULL,
# MAGIC 	JobID INT,
# MAGIC 	BatchName varchar(255) ,
# MAGIC 	ExecuteBeginTS TIMESTAMP ,
# MAGIC 	ExecuteEndTS TIMESTAMP ,
# MAGIC 	ExtractWindowBeginTS TIMESTAMP ,
# MAGIC 	ExtractWindowEndTS TIMESTAMP ,
# MAGIC 	Status varchar(25) ,
# MAGIC 	ErrorLine varchar(25) ,
# MAGIC 	ErrorDescription varchar(500) ,
# MAGIC 	LoadType varchar(20),
# MAGIC 	RetryAttempt INT
# MAGIC )
# MAGIC USING DELTA LOCATION 'dbfs:/mnt/adls_landing/metadata/etl_batch_history';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists kpi_etl_analytics_conf.etl_process_history_test;
# MAGIC
# MAGIC CREATE TABLE kpi_etl_analytics_conf.etl_process_history_test
# MAGIC (
# MAGIC 	ExecutionID VARCHAR(255) NOT NULL,
# MAGIC 	BatchID VARCHAR(255)  ,
# MAGIC 	JobID INT,
# MAGIC 	ExecutionStartTime TIMESTAMP ,
# MAGIC 	ExecutionEndTime TIMESTAMP ,
# MAGIC 	TableName varchar(150) ,
# MAGIC 	Status varchar(25) ,
# MAGIC 	TargetInsertRecCount int ,
# MAGIC 	TargetUpdateRecCount int ,
# MAGIC 	InitialTargetRecCount int ,
# MAGIC 	FinalTargetRecCount int ,
# MAGIC 	TimeTakenInSecs int ,
# MAGIC 	ErrorDescription varchar(8000) ,
# MAGIC 	ErrorLine int 
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY(TableName,ExecutionStartTime) 
# MAGIC LOCATION 'dbfs:/mnt/adls_landing/metadata/etl_process_history_test';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists kpi_etl_analytics_conf.etl_process_history;
# MAGIC
# MAGIC CREATE TABLE kpi_etl_analytics_conf.etl_process_history
# MAGIC (
# MAGIC 	ExecutionID int NOT NULL,
# MAGIC 	BatchID int ,
# MAGIC 	JobID INT,
# MAGIC 	ExecutionStartTime TIMESTAMP ,
# MAGIC 	ExecutionEndTime TIMESTAMP ,
# MAGIC 	TableName varchar(150) ,
# MAGIC 	Status varchar(25) ,
# MAGIC 	TargetInsertRecCount int ,
# MAGIC 	TargetUpdateRecCount int ,
# MAGIC 	InitialTargetRecCount int ,
# MAGIC 	FinalTargetRecCount int ,
# MAGIC 	TimeTakenInSecs int ,
# MAGIC 	ErrorDescription varchar(8000) ,
# MAGIC 	ErrorLine int 
# MAGIC ) USING DELTA LOCATION 'dbfs:/mnt/adls_landing/metadata/etl_process_history';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists kpi_etl_analytics_conf.ctl_table_dims_facts;
# MAGIC
# MAGIC CREATE TABLE spark_catalog.kpi_etl_analytics_conf.ctl_table_dims_facts (
# MAGIC     ID INT,
# MAGIC     Source_Schema STRING,
# MAGIC     Source_Table STRING,
# MAGIC     Source_Column STRING,
# MAGIC     Target_Schema STRING,
# MAGIC     Target_Table STRING,
# MAGIC     Target_Column STRING,
# MAGIC     Transformation STRING,
# MAGIC     Is_Join_Key BOOLEAN,
# MAGIC     Is_Driving_Table BOOLEAN,       -- New column to specify if the table is a driving table
# MAGIC     Join_Table string,               -- Resocetive table name of join column
# MAGIC     Join_Table_Column STRING,        -- New column to store the corresponding join table column
# MAGIC     Join_Type STRING                 -- New column to specify if it is a UNION table or join table
# MAGIC ) USING delta LOCATION 'dbfs:/mnt/adls_landing/metadata/ctl_table_dims_facts' 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists kpi_etl_analytics_conf.ETL_JOB_HISTORY;
# MAGIC CREATE TABLE kpi_etl_analytics_conf.ETL_JOB_HISTORY (
# MAGIC     JobID INT,
# MAGIC     JobStartTime TIMESTAMP,
# MAGIC     JobEndTime TIMESTAMP,
# MAGIC     Status varchar(25),
# MAGIC     RetryAttempt INT,
# MAGIC     ErrorMessage varchar(8000) 
# MAGIC
# MAGIC )USING DELTA LOCATION 'dbfs:/mnt/adls_landing/metadata/ETL_JOB_HISTORY';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO kpi_etl_analytics_conf.ctl_table_dims_facts (
# MAGIC     ID, 
# MAGIC     Source_Schema, 
# MAGIC     Source_Table, 
# MAGIC     Source_Column, 
# MAGIC     Target_Schema, 
# MAGIC     Target_Table, 
# MAGIC     Target_Column, 
# MAGIC     Transformation, 
# MAGIC     Is_Join_Key,
# MAGIC     Is_Driving_Table,
# MAGIC     Join_Table,
# MAGIC     Join_Table_Column,
# MAGIC     Join_Type
# MAGIC )
# MAGIC VALUES
# MAGIC     (1, 'kpi_cloud_sl', 'hed_contact', 'Hed_contactID_c', 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'direct', 'true', true, null, null, null),
# MAGIC     (2, 'kpi_cloud_sl', 'hed_contact', 'Hed_Choosen_full_name_c', 'kpi_cloud_gl', 'dim_contact', 'FullName', 'direct', 'false', true, null, null, null),
# MAGIC     (3, 'kpi_cloud_sl', 'hed_contact', 'Hed_contactGender_c', 'kpi_cloud_gl', 'dim_contact', 'Gender', 'direct', 'false', true, null, null, null),
# MAGIC     (4, 'kpi_cloud_sl', 'hed_contact', 'Hed_contactEmail_c', 'kpi_cloud_gl', 'dim_contact', 'Email', 'direct', 'false', true, null, null, null),
# MAGIC     (5, 'kpi_cloud_sl', 'hed_contact', 'Hed_contactPhone_c', 'kpi_cloud_gl', 'dim_contact', 'Phone', 'direct', 'false', true, null, null, null),
# MAGIC     (6, 'kpi_cloud_sl', 'hed_contact', 'Hed_LastLoadDate_c', 'kpi_cloud_gl', 'dim_contact', 'Effective_Start_Date', 'direct', 'false', true, null, null, null),
# MAGIC     (7, 'kpi_cloud_sl', 'hed_contact', 'DataSourceID', 'kpi_cloud_gl', 'dim_contact', 'DataSourceID', 'direct', 'true', true, 'kpi_cloud_sl.hed_contact', 'DataSourceID', null),
# MAGIC     (8, 'kpi_cloud_sl', 'hed_contact', 'checksum', 'kpi_cloud_gl', 'dim_contact', 'checksum', 'direct', 'false', true, null, null, null),
# MAGIC     (9, 'kpi_cloud_sl', 'hed_contact', 'Hed_ContactEthnicity_c', 'kpi_cloud_gl', 'dim_contact', 'Ethnicity', 'direct', 'false', true, null, null, null),
# MAGIC     (10, 'kpi_cloud_sl', 'hed_contact', 'Hed_contactAge_c', 'kpi_cloud_gl', 'dim_contact', 'Age', 'direct', 'false', true, null, null, null),
# MAGIC     (11, 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'kpi_cloud_gl', 'fact_contact', 'ContactID', 'direct', 'false', true, null, null, null),
# MAGIC     (12, 'kpi_cloud_gl', 'dim_contact', 'FullName', 'kpi_cloud_gl', 'fact_contact', 'FullName', 'direct', 'false', true, null, null, null),
# MAGIC     (13, 'kpi_cloud_gl', 'dim_contact', 'Gender', 'kpi_cloud_gl', 'fact_contact', 'Gender', 'direct', 'false', true, null, null, null),
# MAGIC     (14, 'kpi_cloud_gl', 'dim_contact', 'Phone', 'kpi_cloud_gl', 'fact_contact', 'Email', 'direct', 'false', true, null, null, null),
# MAGIC     (15, 'kpi_cloud_gl', 'dim_contact', 'Email', 'kpi_cloud_gl', 'fact_contact', 'Phone', 'direct', 'false', true, null, null, null),
# MAGIC     (16, 'kpi_cloud_gl', 'dim_contact', 'Ethnicity', 'kpi_cloud_gl', 'fact_contact', 'Ethnicity', 'direct', 'false', true, null, null, null),
# MAGIC     (17, 'kpi_cloud_gl', 'dim_contact', 'Age', 'kpi_cloud_gl', 'fact_contact', 'Age', 'direct', 'false', true, null, null, null),
# MAGIC     (18, 'kpi_cloud_gl', 'dim_contact', 'Contact_ik', 'kpi_cloud_gl', 'fact_contact', 'Dim_Contact_sk', 'direct', 'true', true, null, null, null),
# MAGIC
# MAGIC     (19, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_API', 'kpi_cloud_gl', 'dim_API_Entries', 'API', 'direct', 'true', true, null, null, null),
# MAGIC     (20, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Description', 'kpi_cloud_gl', 'dim_API_Entries', 'Description', 'direct', 'false', true, null, null, null),
# MAGIC     (21, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Auth', 'kpi_cloud_gl', 'dim_API_Entries', 'Gender', 'Auth', 'false', true, null, null, null),
# MAGIC     (22, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_HTTPS', 'kpi_cloud_gl', 'dim_API_Entries', 'Email', 'HTTPS', 'false', true, null, null, null),
# MAGIC     (23, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_CORS', 'kpi_cloud_gl', 'dim_API_Entries', 'Phone', 'CORS', 'false', true, null, null, null),
# MAGIC     (24, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Link', 'kpi_cloud_gl', 'dim_API_Entries', 'Link', 'direct', 'false', true, null, null, null),
# MAGIC     (25, 'kpi_cloud_sl', 'Hed_API_Entries', 'DataSourceID', 'kpi_cloud_gl', 'dim_API_Entries', 'DataSourceID', 'direct', 'true', true, 'kpi_cloud_sl.hed_contact', 'DataSourceID', null),
# MAGIC     (26, 'kpi_cloud_sl', 'Hed_API_Entries', 'Hed_Category', 'kpi_cloud_gl', 'dim_API_Entries', 'Category', 'direct', 'false', true, null, null, null),
# MAGIC     (27, 'kpi_cloud_sl', 'Hed_API_Entries', 'checksum', 'kpi_cloud_gl', 'dim_API_Entries', 'checksum', 'direct', 'false', true, null, null, null),
# MAGIC     (28, 'kpi_cloud_gl', 'dim_API_Entries', 'API', 'kpi_cloud_gl', 'fact_API_Entries', 'API', 'direct', 'true', true, null, null, null),
# MAGIC     (29, 'kpi_cloud_gl', 'dim_API_Entries', 'Description', 'kpi_cloud_gl', 'fact_API_Entries', 'Description', 'direct', 'false', true, null, null, null),
# MAGIC     (30, 'kpi_cloud_gl', 'dim_API_Entries', 'Auth', 'kpi_cloud_gl', 'fact_API_Entries', 'Auth', 'direct', 'false', true, null, null, null),
# MAGIC     (31, 'kpi_cloud_gl', 'dim_API_Entries', 'HTTPS', 'kpi_cloud_gl', 'fact_API_Entries', 'HTTPS', 'direct', 'false', true, null, null, null),
# MAGIC     (32, 'kpi_cloud_gl', 'dim_API_Entries', 'CORS', 'kpi_cloud_gl', 'fact_API_Entries', 'CORS', 'direct', 'false', true, null, null, null),
# MAGIC     (33, 'kpi_cloud_gl', 'dim_API_Entries', 'Link', 'kpi_cloud_gl', 'fact_API_Entries', 'Link', 'direct', 'false', true, null, null, null),
# MAGIC     (34, 'kpi_cloud_gl', 'dim_API_Entries', 'Category', 'kpi_cloud_gl', 'fact_API_Entries', 'Category', 'direct', 'false', true, null, null, null),
# MAGIC     (35, 'kpi_cloud_gl', 'dim_API_Entries', 'APIEntries_ik', 'kpi_cloud_gl', 'fact_API_Entries', 'Dim_APIEntries_sk', 'direct', 'false', true, null, null, null);
# MAGIC
# MAGIC     -- (9, 'kpi_cloud_sl', 'hr_contact', 'Hr_contactID_c', 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'direct', 'true', false, 'kpi_cloud_sl.hed_contact', 'Hed_contactID_c', 'JOIN'),
# MAGIC     -- (10, 'kpi_cloud_sl', 'hr_contact', 'Hr_Choosen_full_name_c', 'kpi_cloud_gl', 'dim_contact', 'FullName', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (11, 'kpi_cloud_sl', 'hr_contact', 'Hr_contactGender_c', 'kpi_cloud_gl', 'dim_contact', 'Gender', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (12, 'kpi_cloud_sl', 'hr_contact', 'Hr_contactEmail_c', 'kpi_cloud_gl', 'dim_contact', 'Email', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (13, 'kpi_cloud_sl', 'hr_contact', 'Hr_contactPhone_c', 'kpi_cloud_gl', 'dim_contact', 'Phone', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (14, 'kpi_cloud_sl', 'hr_contact', 'Hr_LastLoadDate_c', 'kpi_cloud_gl', 'dim_contact', 'Effective_Start_Date', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (15, 'kpi_cloud_sl', 'hr_contact', 'DataSourceID', 'kpi_cloud_gl', 'dim_contact', 'DataSourceID', 'direct', 'true', false, 'kpi_cloud_sl.hed_contact', 'DataSourceID', 'JOIN'),
# MAGIC     -- (16, 'kpi_cloud_sl', 'hr_contact', 'checksum', 'kpi_cloud_gl', 'dim_contact', 'checksum', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (17, 'kpi_cloud_sl', 'erp_contact', 'Erp_contactID_c', 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'direct', 'true', false, 'kpi_cloud_sl.hr_contact', 'Hr_contactID_c', 'JOIN'),
# MAGIC     -- (18, 'kpi_cloud_sl', 'erp_contact', 'Erp_Choosen_full_name_c', 'kpi_cloud_gl', 'dim_contact', 'FullName', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (19, 'kpi_cloud_sl', 'erp_contact', 'Erp_contactGender_c', 'kpi_cloud_gl', 'dim_contact', 'Gender', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (20, 'kpi_cloud_sl', 'erp_contact', 'Erp_contactEmail_c', 'kpi_cloud_gl', 'dim_contact', 'Email', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (21, 'kpi_cloud_sl', 'erp_contact', 'Erp_contactPhone_c', 'kpi_cloud_gl', 'dim_contact', 'Phone', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (22, 'kpi_cloud_sl', 'erp_contact', 'Erp_LastLoadDate_c', 'kpi_cloud_gl', 'dim_contact', 'Effective_Start_Date', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (23, 'kpi_cloud_sl', 'erp_contact', 'DataSourceID', 'kpi_cloud_gl', 'dim_contact', 'DataSourceID', 'direct', 'true', true, 'kpi_cloud_sl.hed_contact', 'DataSourceID', 'JOIN'),
# MAGIC     -- (24, 'kpi_cloud_sl', 'erp_contact', 'checksum', 'kpi_cloud_gl', 'dim_contact', 'checksum', 'direct', 'false', false, null, null, 'JOIN'),
# MAGIC     -- (25, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_contactID_c', 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'direct', 'true', false, NULL, NULL, 'UNION'),
# MAGIC     -- (26, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_Choosen_full_name_c', 'kpi_cloud_gl', 'dim_contact', 'FullName', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (27, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_contactGender_c', 'kpi_cloud_gl', 'dim_contact', 'Gender', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (28, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_contactEmail_c', 'kpi_cloud_gl', 'dim_contact', 'Email', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (29, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_contactPhone_c', 'kpi_cloud_gl', 'dim_contact', 'Phone', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (30, 'kpi_cloud_sl', 'ebs_contact', 'Ebs_LastLoadDate_c', 'kpi_cloud_gl', 'dim_contact', 'Effective_Start_Date', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (31, 'kpi_cloud_sl', 'ebs_contact', 'DataSourceID', 'kpi_cloud_gl', 'dim_contact', 'DataSourceID', 'direct', 'true', true, NULL, NULL, 'UNION'),
# MAGIC     -- (32, 'kpi_cloud_sl', 'ebs_contact', 'checksum', 'kpi_cloud_gl', 'dim_contact', 'checksum', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (33, 'kpi_cloud_sl', 'sap_contact', 'Sap_contactID_c', 'kpi_cloud_gl', 'dim_contact', 'ContactID', 'direct', 'true', false, NULL, NULL, 'UNION'),
# MAGIC     -- (34, 'kpi_cloud_sl', 'sap_contact', 'Sap_Choosen_full_name_c', 'kpi_cloud_gl', 'dim_contact', 'FullName', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (35, 'kpi_cloud_sl', 'sap_contact', 'Sap_contactGender_c', 'kpi_cloud_gl', 'dim_contact', 'Gender', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (36, 'kpi_cloud_sl', 'sap_contact', 'Sap_contactEmail_c', 'kpi_cloud_gl', 'dim_contact', 'Email', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (37, 'kpi_cloud_sl', 'sap_contact', 'Sap_contactPhone_c', 'kpi_cloud_gl', 'dim_contact', 'Phone', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (38, 'kpi_cloud_sl', 'sap_contact', 'Sap_LastLoadDate_c', 'kpi_cloud_gl', 'dim_contact', 'Effective_Start_Date', 'direct', 'false', false, NULL, NULL, 'UNION'),
# MAGIC     -- (39, 'kpi_cloud_sl', 'sap_contact', 'DataSourceID', 'kpi_cloud_gl', 'dim_contact', 'DataSourceID', 'direct', 'true', true, NULL, NULL, 'UNION'),
# MAGIC     -- (40, 'kpi_cloud_sl', 'sap_contact', 'checksum', 'kpi_cloud_gl', 'dim_contact', 'checksum', 'direct', 'false', false, NULL, NULL, 'UNION');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpi_etl_analytics_conf.CTL_TABLE_DIMS_FACTS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists kpi_cloud_bl.source1_contact;
# MAGIC drop table if exists kpi_cloud_bl.source2_contact;
# MAGIC
# MAGIC CREATE or replace TABLE kpi_cloud_bl.source1_contact (   
# MAGIC   ContactID STRING,   
# MAGIC   ContactName STRING,   
# MAGIC   ContactGender STRING,   
# MAGIC   ContactPhone STRING,   
# MAGIC   ContactEmail STRING,   
# MAGIC   LastLoadDate TIMESTAMP,   
# MAGIC   DataSourceID STRING,
# MAGIC   LastmodifiedDate TIMESTAMP
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/mnt/adls_landing/bronze/source1_Contact' ;
# MAGIC
# MAGIC CREATE or replace TABLE kpi_cloud_bl.source2_contact (   
# MAGIC   ContactID STRING,   
# MAGIC   ContactFirstName STRING,   
# MAGIC   ContactLastName STRING,   
# MAGIC   ContactPhone STRING,   
# MAGIC   ContactGender STRING,   
# MAGIC   ContactEmail STRING,   
# MAGIC   LastLoadDate TIMESTAMP,   
# MAGIC   DataSourceID STRING,
# MAGIC   LastmodifiedDate TIMESTAMP
# MAGIC   ) 
# MAGIC   USING delta LOCATION 'dbfs:/mnt/adls_landing/bronze/source2_Contact' 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kpi_cloud_bl.source3_Ext_Contact;
# MAGIC CREATE or replace TABLE kpi_cloud_bl.source3_Ext_Contact (   
# MAGIC   ContactID STRING,   
# MAGIC   ContactFirstName STRING,   
# MAGIC   ContactLastName STRING,   
# MAGIC   ContactPhone STRING,   
# MAGIC   ContactGender STRING,  
# MAGIC   ContactAge INT,
# MAGIC   ContactEthnicity STRING, 
# MAGIC   ContactEmail STRING,   
# MAGIC   LastLoadDate TIMESTAMP,   
# MAGIC   DataSourceID STRING,
# MAGIC   LastmodifiedDate TIMESTAMP
# MAGIC   ) 
# MAGIC   USING delta LOCATION 'dbfs:/mnt/adls_landing/bronze/source3_Ext_Contact' 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists kpi_cloud_sl.hed_contact;
# MAGIC
# MAGIC   CREATE or replace TABLE kpi_cloud_sl.hed_contact (   
# MAGIC   Hed_contactID_c STRING,  
# MAGIC   Hed_Choosen_full_name_c STRING,  
# MAGIC   Hed_contactGender_c STRING,
# MAGIC   Hed_contactEmail_c STRING,   
# MAGIC   Hed_contactPhone_c STRING, 
# MAGIC   Hed_ContactAge_c int,  
# MAGIC   Hed_ContactEthnicity_c string,
# MAGIC   Hed_LastLoadDate_c TIMESTAMP,   
# MAGIC   DataSourceID STRING,
# MAGIC   checksum STRING
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/mnt/adls_landing/silver/hed_contact' 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table kpi_cloud_sl.hed_contact add columns (LastLoadDate timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists kpi_cloud_gl.dim_contact;
# MAGIC
# MAGIC CREATE or replace TABLE kpi_cloud_gl.dim_contact (
# MAGIC   Contact_ik INT,
# MAGIC   Hash_Join_key STRING,
# MAGIC   ContactID STRING,   
# MAGIC   FullName STRING,   
# MAGIC   Gender STRING,   
# MAGIC   Phone STRING,   
# MAGIC   Email STRING, 
# MAGIC   Effective_Start_Date Date,
# MAGIC   Effective_End_date Date,
# MAGIC   Current_Record STRING,  
# MAGIC   DataSourceID STRING,
# MAGIC   checksum STRING
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/mnt/adls_landing/gold/dim_contact' 

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table kpi_cloud_gl.dim_contact add columns (Age int, Ethnicity string,LastLoadDate timestamp)
# MAGIC --ALTER TABLE kpi_cloud_gl.dim_contact SET TBLPROPERTIES (
# MAGIC --   'delta.columnMapping.mode' = 'name',
# MAGIC  --  'delta.minReaderVersion' = '2',
# MAGIC --   'delta.minWriterVersion' = '5')
# MAGIC --alter table kpi_cloud_gl.dim_contact rename column tAge TO Age

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kpi_cloud_gl.fact_contact;
# MAGIC
# MAGIC CREATE or replace TABLE kpi_cloud_gl.fact_contact (
# MAGIC   Dim_Contact_sk STRING,
# MAGIC   ContactID STRING,   
# MAGIC   FullName STRING,   
# MAGIC   Gender STRING,   
# MAGIC   Phone STRING,   
# MAGIC   Email STRING
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/mnt/adls_landing/gold/fact_contact' 

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table kpi_cloud_gl.fact_contact add columns (Age int, Ethnicity string,LastLoadDate timestamp)

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
import json
#import explode

# Replace 'V_Query' with your API URL
V_Query = "https://api.publicapis.org/entries"

# Fetch JSON data from API
response = requests.get(V_Query)
json_data = json.loads(response.text)
#print(json_data)

dbutils.fs.put("dbfs:/FileStore/publicapidata.json",response.text)
# with open(f"dbfs:/FileStore/FileStore/publicapidata.csv","w") as f:

#     f.write(response.content)
# json_con = json.dumps(json_data)
# print(type(json_con))
# json_rdd = spark.sparkContext.parallelize(json_data).collect()
# print(json_rdd)

# source_df = spark.read.option("multiLine", "true").option("inferSchema","true").json(json_con)
# display(source_df)
# Explode arrays if present
# exploded_df = source_df.select(explode(col("_corrupt_record"))).alias("exploded_array_column")

# # Show DataFrame
#exploded_df.show()


# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/publicapidata.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table kpi_etl_analytics_conf.etl_process_history_test

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE kpi_etl_analytics_conf.etl_batch_history_test SET ExecuteEndTS='2023-09-05T08:22:23.3168792Z',      STATUS='SUCCESS'      
# MAGIC  WHERE BatchID='a6a5b5c444bbd1919e7ff80d274301ca' AND ErrorDescription IS NULL AND Batchname = 'source3_BronzeExtraction' AND RetryAttempt = (select max(RetryAttempt) from kpi_etl_analytics_conf.etl_batch_history_test where BatchId='a6a5b5c444bbd1919e7ff80d274301ca' )

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions kpi_etl_analytics_conf.etl_batch_history_test

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.rowLevelConcurrencyPreview = true
