# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC - Read bronze races table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case (raceName -> race_name, circuitId -> circuit_id)
# MAGIC - Rename columns to make it more meaningful (date -> race_date)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns race_name and locality to Title Case
# MAGIC - Write the transformed data to silver races delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table= f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read bronze Races Table

# COMMAND ----------

races_df=spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Keep only the columns that are required (Drop url column)
# MAGIC

# COMMAND ----------

# Using f col
from pyspark.sql import functions as F
races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 & 4- Standardize column names 
# MAGIC - Standardize column names using snake_case (raceName ->race_name, circuitId -> circuit_id)
# MAGIC - Rename columns to make it more meaningful (date -> race_date)

# COMMAND ----------

races_renamed_df=(races_selected_df
    .withColumnsRenamed({
        "raceName":"race_name", 
        "circuitId":"circuit_id",
        "date":"race_date"
    })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5- Remove duplicate records

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6- Transform values of columns race_name and to Title Case

# COMMAND ----------

races_final_df =(
    races_distinct_df
    .withColumn("race_name",F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8- Write the transformed data to silver races delta table

# COMMAND ----------

(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))