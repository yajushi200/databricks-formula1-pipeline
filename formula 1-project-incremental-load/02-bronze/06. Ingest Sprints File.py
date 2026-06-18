# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Sprints.json
# MAGIC - Read the file using Spark DataFrameReader API
# MAGIC - Enforce Schema
# MAGIC - Add Metadata columns (source file, ingestion timestamp)
# MAGIC - Write to bronze delta table
# MAGIC
# MAGIC NOTE: JSON IS IN MULTI LINE FORMAT

# COMMAND ----------

# MAGIC  %run ../00-common/01.environment-config 

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze_helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# Defining source file and the table name
source_file=f"{landing_folder_path}/{v_batch_id}/sprints"
table_name=f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the JSON file using the reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType ,FloatType

# Defining the Schema

sprints_schema = StructType([
StructField ('date',DateType()),
StructField ('raceName',StringType()),
StructField('round',IntegerType()),
StructField('season',IntegerType()),
StructField('url',StringType()),
StructField('constructorId',StringType()),
StructField('driverId',StringType()),
StructField('grid',IntegerType()),
StructField('laps',IntegerType()),
StructField('number',IntegerType()),
StructField('points',FloatType()),
StructField('position',IntegerType()),
StructField('positionText',StringType()),
StructField('status',StringType())
])

# COMMAND ----------

sprints_df=(
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode','FAILFAST')
    .option('multiLine',True)
    .load(source_file)
)

# COMMAND ----------

display (sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -2 Adding the metadata
# MAGIC

# COMMAND ----------

sprints_final_df= (add_ingestion_metadata (sprints_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -3 Writing the data as Delta table in the bronze layer
# MAGIC

# COMMAND ----------

write_to_bronze(
    input_df = sprints_final_df,
    target_table= table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))