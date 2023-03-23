# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in customer dataset and populate bronze_customer table

# COMMAND ----------

# Read in customer dataset from /databricks-datasets/retail-org/customers/
@dlt.table
def bronze_customer_dlt():
  return spark.read.format("csv").option("header", True).load("/databricks-datasets/retail-org/customers/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in sales dataset and populate bronze_sales table

# COMMAND ----------

# Read in json dataset from /databricks-datasets/retail-org/sales_orders/ to populate "bronze_sales" table
@dlt.table
def bronze_sales_dlt():
  return spark.read.format("json").load("/databricks-datasets/retail-org/sales_orders/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and clean up data in bronze_customer to populate silver_customer

# COMMAND ----------

# select only customer_id, customer_name, state, and city columns from "bronze_customer" table
@dlt.table
def silver_customer_dlt():
  df4= dlt.read("bronze_customer_dlt").select("customer_id", "customer_name", "state", "city")
  # remove all rows with city = null and convert "city" column to upper case
  df4=df4.filter(~df4.city.isin(["null"])).withColumn('city', F.upper(col('city')))
  return df4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and clean up data in bronze_sales to populate silver_sales

# COMMAND ----------

@dlt.table
def silver_sales_dlt():
  # select only the following columns: customer_id, number_of_line_items, order_datetime, order_number
  df5 = dlt.read("bronze_sales_dlt").select("customer_id", "number_of_line_items", "order_datetime", "order_number")
  #remove rows without an "order_datetime", convert "order_datetime" column to datetime
  df5 = df5.withColumn("order_datetime",from_unixtime(col("order_datetime")))
  df5 = df5.filter(~df5.order_datetime.isin(["null"]))
  return df5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold Layer

# COMMAND ----------

@dlt.table
def gold_customer_dlt():
  # join silver_customer with silver_sales on "customer_id" column to populate "gold_customer_records" table
  df_customer = dlt.read("silver_customer_dlt")
  df_sales = dlt.read("silver_sales_dlt")
  df = df_customer.join(df_sales, ["customer_id"])
  return df
