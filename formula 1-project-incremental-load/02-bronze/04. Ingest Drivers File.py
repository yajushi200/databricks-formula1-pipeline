# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers.json
# MAGIC - Read the file using Spark DataFrameReader API
# MAGIC - Enforce Schema (PRESERVE THE NESTED STRUCTURE)
# MAGIC - Add Metadata columns (source file, ingestion timestamp)
# MAGIC - Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config 

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze_helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# Defining the source_name and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -1 Reading the Data 
# MAGIC

# COMMAND ----------

# Definig the schema

# First we will define the schema for the name object (inner schema)

from pyspark.sql.types import StructType , StructField, StringType , DateType

name_schema= StructType([
    StructField('givenName', StringType()),
    StructField('familyName',StringType()),
])

drivers_schema = StructType ([
    StructField('driverId', StringType()),
    StructField('name',name_schema),
    StructField('dateOfBirth',DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType())  
])

# COMMAND ----------

drivers_df=(
    spark.read
        .format('json')
        .option('mode','FAILFAST')
        .schema(drivers_schema)
        .load(source_file)

)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -2 Adding the metadata
# MAGIC

# COMMAND ----------

drivers_final_df = (add_ingestion_metadata(drivers_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-3 Writing the Data

# COMMAND ----------

# (
#     drivers_final_df 
#     .write
#     .format('delta')
#     .mode('overwrite')
#     .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df= drivers_final_df,
    target_table= table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display (spark.table(table_name))

# COMMAND ----------

