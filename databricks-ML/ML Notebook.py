# Databricks notebook source
path = "dbfs:/FileStore/diabetes_prediction_dataset.csv"
diabetes_ds = spark.read.csv(path,header=True,inferSchema=True)
diabetes_ds.createOrReplaceTempView("diabetes_ds")
diabetes_ds.display()

# COMMAND ----------

print("Columns :",end="\n - ")
print(*diabetes_ds.columns,sep="\n - ")

# COMMAND ----------

def add_id(col_name):
    df_raw = diabetes_ds.select(col_name).distinct().orderBy(col_name).collect()
    df_raw = [i[col_name] for i in df_raw]
    return spark.createDataFrame([i for i in enumerate(df_raw)],["id",col_name])

# COMMAND ----------

add_id("smoking_history").createOrReplaceTempView("smoking_history")
add_id("gender").createOrReplaceTempView("gender")

# COMMAND ----------

final_df = sql("""
SELECT 
 GN.ID AS gender,
 DS.age,
 DS.hypertension,
 DS.heart_disease,
 SH.ID as smoking_history,
 DS.bmi,
 DS.HbA1c_level,
 DS.blood_glucose_level,
 DS.diabetes
FROM DIABETES_DS DS
, GENDER GN, SMOKING_HISTORY SH
WHERE DS.GENDER=GN.GENDER AND DS.SMOKING_HISTORY=SH.SMOKING_HISTORY""")

# COMMAND ----------

from pyspark.ml.

# COMMAND ----------


