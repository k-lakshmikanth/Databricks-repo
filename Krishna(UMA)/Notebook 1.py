# Databricks notebook source
spark.sql("SELECT current_timestamp() AS CURRENTSYSTEMDATE").first().CURRENTSYSTEMDATE
