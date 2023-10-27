-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This is the setup notebook
-- MAGIC ---
-- MAGIC *In this notebook we create FILE_LOAD_LOG table in Databricks environment for incremental load*

-- COMMAND ----------

CREATE TABLE FILE_LOAD_LOG(ID VARCHAR(200) NOT NULL,PATH VARCHAR(200),MODIFICATIONTIME BIGINT,MESSAGE VARCHAR(2000))
