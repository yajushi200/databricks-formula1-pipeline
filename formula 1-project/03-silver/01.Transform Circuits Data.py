# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circuits Data
# MAGIC - Read bronze circuits table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case `(circuitId -> circuit_id)`
# MAGIC - Rename columns to make it more meaningful `(lat -> latitude and long-> longitude)
# MAGIC - Filter out rows where circuit_id is null (business key validation)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns circuit_name and locality to Title Case
# MAGIC - Write the transformed data to silver circuits delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# Creating a variable (bronze_table) which has the table name of our bronze table

bronze_table= f"{catalog_name}.{bronze_schema}.circuits"

# silver_table - required to write the data

silver_table= f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read bronze circuits table

# COMMAND ----------

# METHOD-1 returns a DataFrame and has the columns in the bronze delta table

# circuits_df= 
# (
#     spark.read
#     .option('versionAsOf', 0)
#     .table(bronze_table)
# )

# COMMAND ----------

# METHOD-2 
circuits_df=spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Keep only the columns that are required (Drop url column)

# COMMAND ----------

# Using select method on req columns
# METHOD 1
# circuits_selected_df = circuits.df.select(
#     "circuitid",
#     "circuitName",
#     "lat",
#     "long",
#     "locality",
#     "country",
#     "ingestion_timestamp",
#     "source_file"
# )

# COMMAND ----------

from pyspark.sql import functions as F

# METHOD 2
# Using f.col gives us more options to play with the columns like aliasing and even we can change the column values 
circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step -3 & 4 Standardize column names 
# MAGIC - Standardize column names using snake_case `(circuitId -> circuit_id)`
# MAGIC - Rename columns to make it more meaningful (lat -> latitude and long-> longitude)
# MAGIC

# COMMAND ----------

# circuits_renamed_df= 
# (
#     circuits_selected_df
#     .withColumnRenamed("circuitId","circuit_id")
#     .withColumnRenamed("circuitName","circuit_name")
#     .withColumnRenamed("long","longitude")
#     .withColumnRenamed("lat","latitude")
# )

# COMMAND ----------

# To make the renaming more simplier, Spark offers another method 

circuits_renamed_df =(
    circuits_selected_df
    .withColumnsRenamed({
        "circuitId":"circuit_id",
        "circuitName":"circuit_name",
        "long":"longitude",
        "lat":"latitude"
     })
)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5- Filter out rows where circuit_id is null (business key validation)

# COMMAND ----------

# # METHOD1- Using filter with SQL statements
# circuits_valid_df = circuits_renamed_df.filter
# (
#     "circuit_id IS NOT NULL"
# )

# COMMAND ----------

# DBTITLE 1,Cell 17
# METHOD-2 Using filter method with column expressions
circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6- Remove duplicate records

# COMMAND ----------

# Using DISTINCT method
# circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

# Using drop method if columns arent specified as parameters it will behavee like distinct
circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7- Transform values of columns circuit_name and locality to Title Case

# COMMAND ----------

circuits_final_df =(
    circuits_distinct_df
    .withColumn("circuit_name",F.initcap(F.col("circuit_name")))
    .withColumn("locality",F.initcap(F.col("locality")))
) 


# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8- Write the transformed data to silver circuits delta table

# COMMAND ----------

(
    circuits_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))