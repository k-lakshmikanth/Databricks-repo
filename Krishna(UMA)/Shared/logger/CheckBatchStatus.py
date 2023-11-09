# Databricks notebook source
dbutils.widgets.text("RunDate", "")
dbutils.widgets.text("BatchId", "")
dbutils.widgets.text("Error", "")
dbutils.widgets.text("BatchName", "")

# COMMAND ----------

V_RunDate = dbutils.widgets.get("RunDate")
V_BatchId = dbutils.widgets.get("BatchId")
V_Error = dbutils.widgets.get("Error")
V_BatchName = dbutils.widgets.get("BatchName")
print(V_RunDate,V_BatchId,V_Error,V_BatchName)
V_ProcessLogTable="kpi_etl_analytics_conf.etl_process_history_test"
V_BatchLogTable="kpi_etl_analytics_conf.etl_batch_history_test"
V_JobLogTable="kpi_etl_analytics_conf.etl_job_history"

# COMMAND ----------

try:
    V_Status = ''
    V_ErrorMessage = ''
    if V_BatchName=='DIM_FACT_LOAD':
        print("Job is already running and update it with the values!")
        V_NewJobId=0
        result_df = spark.sql(f"""SELECT MAX(JobId) AS JobId_NEW FROM {V_JobLogTable}""")
        V_NewJobId = result_df.collect()[0]['JobId_NEW']
        print(V_NewJobId)
        df = spark.sql(f"""
        SELECT ErrorDescription AS Error
        FROM {V_BatchLogTable}
        WHERE JobId = '{V_NewJobId}' and Status='FAILED'
        ORDER BY RetryAttempt DESC LIMIT 1
        """)
        if df.count()>0:
            V_Status = 'FAILED'
            V_ErrorMessage= df.collect()[0]['Error']
        else:
            V_Status = 'SUCCESS'
            print("Update the LoadType in metadata table!")
            spark.sql(f"""UPDATE kpi_etl_analytics_conf.ctl_table_sync SET LoadType='I' where IsAlwaysFullLoad='N'""")
        current_system_date = spark.sql("SELECT current_timestamp() as CurrentSystemDate").first().CurrentSystemDate
        dml = f"""
        UPDATE {V_JobLogTable} SET JobEndTime='{current_system_date}',
            Status='{V_Status}',
            ErrorMessage = '{V_ErrorMessage}'
            WHERE JobId='{V_NewJobId}' and RetryAttempt = (
            SELECT MAX(RetryAttempt)
            FROM {V_JobLogTable}
            WHERE JobId = '{V_NewJobId}')"""
        spark.sql(dml) 
        print(dml)
except Exception as e:
    print(e)
    pass

# COMMAND ----------

if V_BatchId:
    df = spark.sql(f"""
        SELECT ErrorDescription AS n
        FROM {V_ProcessLogTable}
        WHERE BatchID = '{V_BatchId}'
         order by ExecutionStartTime desc LIMIT 1
    """).collect()[0][0]
    print(df)
    if df is not None:
         raise Exception("Notebook failed.")
elif V_RunDate:
    df = spark.sql(f"""
        SELECT COUNT(DISTINCT ErrorDescription) AS n
        FROM {V_BatchLogTable}
        WHERE ExecuteBeginTS >='{V_RunDate}'
    """).collect()[0][0]
    print(df)
    if df!=0:
        raise Exception("Notebook failed.")
elif V_Error:
    raise Exception("Notebook failed.") 
