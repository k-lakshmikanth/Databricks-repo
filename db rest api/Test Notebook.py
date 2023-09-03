# Databricks notebook source
from dbapi import repos

# COMMAND ----------

from config import config

# COMMAND ----------

repos().get_repos_list()
