# Databricks notebook source
jdbcHostname = "kr-az-sql-pool2.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "krishna-sqlpool"
jdbcUsername = "sqladmin"
jdbcPassword = "Azure$2023"
jdbcurl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user ={jdbcUsername};password={jdbcPassword}"


# COMMAND ----------

# MAGIC %store jdbcurl
