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
df = spark.read.format("csv").option("header", True).load("/databricks-datasets/retail-org/customers/")
display(df)

# COMMAND ----------

# save the loaded dataset to "bronze_customer" Delta Lake table
df.write.saveAsTable("bronze_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in sales dataset and populate bronze_sales table

# COMMAND ----------

# Read in json dataset from /databricks-datasets/retail-org/sales_orders/ to populate "bronze_sales" table
spark.read.format("json").load("/databricks-datasets/retail-org/sales_orders/").write.saveAsTable("bronze_sales")

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
df4= spark.read.table("bronze_customer").select("customer_id", "customer_name", "state", "city")
display(df4)

# COMMAND ----------

# remove all rows with city = null and convert "city" column to upper case
import pyspark.sql.functions as F
from pyspark.sql.functions import *
df4=df4.filter(~df4.city.isin(["null"])).withColumn('city', F.upper(col('city')))
display(df4)

# COMMAND ----------

# load the cleaned dataset to silver_customer
df4.write.saveAsTable("silver_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and clean up data in bronze_sales to populate silver_sales

# COMMAND ----------

display(spark.read.table("bronze_sales"))

# COMMAND ----------

# select only the following columns: customer_id, number_of_line_items, order_datetime, order_number
df5 = spark.read.table("bronze_sales").select("customer_id", "number_of_line_items", "order_datetime", "order_number")

# COMMAND ----------

#remove rows without an "order_datetime", convert "order_datetime" column to datetime
df5 = df5.withColumn("order_datetime",from_unixtime(col("order_datetime")))
df5 = df5.filter(~df5.order_datetime.isin(["null"]))
display(df5)

# COMMAND ----------

# populate silver_sales table
df5.write.saveAsTable("silver_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold Layer

# COMMAND ----------

# join silver_customer with silver_sales on "customer_id" column to populate "gold_customer_records" table
df_customer = spark.read.table("silver_customer")
df_sales = spark.read.table("silver_sales")
df = df_customer.join(df_sales, ["customer_id"])
df.write.saveAsTable("gold_customer_records")

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
