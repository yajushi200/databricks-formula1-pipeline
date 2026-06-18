# Databricks notebook source
# MAGIC %md
# MAGIC # Transform constructors Data
# MAGIC - Read bronze constructors table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case (constructorId -> constructor_id)
# MAGIC - Rename columns to make it more meaningful (name -> constructor_name)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns nationality to Title Case
# MAGIC - Write the transformed data to silver constructors delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.constructors"
silver_table=f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read bronze constructors table

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Keep only the columns that are required (Drop url column)
# MAGIC

# COMMAND ----------

# constructors_selected_df = (
#     constructors_df.select(
#         F.col("constructorId"),
#         F.col("name"),
#         F.col("nationality")
#     )  
# )

# COMMAND ----------

# METHOD-2 using drop method
constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 & 4- Standardize column names using snake_case 
# MAGIC - Standardize column names using snake_case (constructorId -> constructor_id)
# MAGIC - Rename columns to make it more meaningful (name -> constructor_name)
# MAGIC

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
    .withColumnsRenamed(
    {"constructorId": "constructor_id",
    "name": "constructor_name"}
    )
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 - Remove duplicate records

# COMMAND ----------

constructors_distinct_df=(
    constructors_renamed_df.dropDuplicates(["constructor_id"])
)

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-6 Transform values of columns nationality to Title Case
# MAGIC

# COMMAND ----------

constructors_final_df=(
    constructors_distinct_df
    .withColumn("nationality", F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-7 Write the transformed data to silver constructors delta tab

# COMMAND ----------

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

