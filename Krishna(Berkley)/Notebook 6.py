# Databricks notebook source
dest_path = "dbfs:/FileStore/berkley_ds/dest.csv"
src_path = "dbfs:/FileStore/berkley_ds/src.csv"

# COMMAND ----------

dest_path[:-4]+"_After"

# COMMAND ----------

spark.read.csv(src_path,header=True,inferSchema=True).createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`/FileStore/berkley_ds/dest`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src

# COMMAND ----------

spark.sql("""
select Distinct dest.* from parquet.`/FileStore/berkley_ds/dest` dest ,src
where
dest.cond_cols = src.cond_cols
AND dest.IsLatest = 1""").createOrReplaceTempView("temp_dest")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_dest

# COMMAND ----------

columns = spark.sql("select * from temp_dest").columns
src_columns = spark.sql("select * from src").columns

# COMMAND ----------

set(columns)-set(src_columns)

# COMMAND ----------

spark.sql(f"select {','.join(columns)} from temp_dest").union(spark.sql(f"select {','.join(src_columns)},Null,Null from src")).createOrReplaceTempView("temp_dest_1")
spark.sql("select * from temp_dest_1").display()
df = spark.sql("select * from temp_dest_1")

# COMMAND ----------

Latest_entry_ids = [str(i.Max_id) for i in spark.sql("select max(id) Max_id from temp_dest_1 group by cond_cols").collect()]

# COMMAND ----------

Latest_entry_ids

# COMMAND ----------

src_columns

# COMMAND ----------

sql(f"""
select {','.join(src_columns)},
case when id in ({','.join(Latest_entry_ids)})
     then 1 else 0 end IsLatest,
case when id in ({','.join(Latest_entry_ids)})
     then '9999-12-31' else trans_proc_ts end trans_proc_end_ts
from temp_dest_1""").createOrReplaceTempView("Final_dest")
spark.sql("select * from Final_dest").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/berkley_ds/dest_After`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Final_dest

# COMMAND ----------

print(f"""
merge into parquet.`dbfs:/FileStore/berkley_ds/dest_After` t1
using Final_dest t2 on 
t1.cond_cols = t2.cond_cols and
t1.id = t2.id
when matched then
  update
  set IsLatest = t2.IsLatest and trans_proc_end_ts = t2.trans_proc_end_ts
when not matched then
  insert *
  --({','.join(columns)}) values(t2.{', t2.'.join(columns)})""")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dest as
# MAGIC select * from parquet.`dbfs:/FileStore/berkley_ds/dest`

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into dest t1
# MAGIC using Final_dest t2 on 
# MAGIC t1.cond_cols = t2.cond_cols and
# MAGIC t1.id = t2.id
# MAGIC when matched then
# MAGIC   update
# MAGIC   set *
# MAGIC when not matched then
# MAGIC   insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dest order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite directory 'dbfs:/FileStore/berkley_ds/dest_After' STORED AS PARQUET select * from dest order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/berkley_ds/dest_After`
