# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Results.json
# MAGIC - Read the file using Spark DataFrameReader API
# MAGIC - Enforce Schema 
# MAGIC - Add Metadata columns (source file, ingestion timestamp)
# MAGIC - Write to bronze delta table

# COMMAND ----------

# MAGIC  %run ../00-common/01.environment-config 

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze_helpers

# COMMAND ----------

# Defining source file and the table name
source_file=f"{landing_folder_path}/results"
table_name=f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -1 Read the data from the json file 
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType ,FloatType

# Defining the Schema

results_schema = StructType([
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

results_df =(
    spark.read
    .format('json')
    .schema(results_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -2 Add metadata

# COMMAND ----------

results_final_df =(add_ingestion_metadata(results_df))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -3 Writing the data as delta table in bronze layer

# COMMAND ----------

(
    results_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC from formula1.bronze.results
# MAGIC group by season
# MAGIC order by season;

# COMMAND ----------

