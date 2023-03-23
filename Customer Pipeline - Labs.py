# Databricks notebook source
# MAGIC %md
# MAGIC #Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC A store has two seperate datasets storing customer information and order information. The store manager wants a single dataset to analyze sales by customers.

# COMMAND ----------

display(spark.read.format("csv")
        .option("header",True)
        .load("/databricks-datasets/retail-org/customers/"))

# COMMAND ----------

display(spark.read.format("json")
        .load("/databricks-datasets/retail-org/sales_orders/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS data_engineering_workshop;
# MAGIC USE data_engineering_workshop;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in customer dataset and populate bronze_customer table

# COMMAND ----------

# Read in customer dataset from /databricks-datasets/retail-org/customers/

# COMMAND ----------

# save the loaded dataset to "bronze_customer" Delta Lake table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in sales dataset and populate bronze_sales table

# COMMAND ----------

# Read in json dataset from /databricks-datasets/retail-org/sales_orders/ to populate "bronze_sales" table


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and clean up data in bronze_customer to populate silver_customer

# COMMAND ----------

# select only customer_id, customer_name, state, and city columns from "bronze_customer" table

# COMMAND ----------

# remove all rows with city = null and convert "city" column to upper case

# COMMAND ----------

# load the cleaned dataset to silver_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and clean up data in bronze_sales to populate silver_sales

# COMMAND ----------

display(spark.read.table("bronze_sales"))

# COMMAND ----------

# select only the following columns: customer_id, number_of_line_items, order_datetime, order_number


# COMMAND ----------

#remove rows without an "order_datetime", convert "order_datetime" column to datetime


# COMMAND ----------

# populate silver_sales table


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold Layer

# COMMAND ----------

# join silver_customer with silver_sales on "customer_id" column to populate "gold_customer_records" table


# COMMAND ----------

# MAGIC %md
# MAGIC #Read from "gold_customer_records" table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_records;

# COMMAND ----------

# MAGIC %md
# MAGIC #Clean Up

# COMMAND ----------

# MAGIC %sql DROP DATABASE data_engineering_workshop CASCADE;
