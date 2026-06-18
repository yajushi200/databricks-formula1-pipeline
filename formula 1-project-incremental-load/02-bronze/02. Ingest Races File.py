# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Races.csv
# MAGIC - Read the file using Spark DataFrameReader API
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

source_file=f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name=f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
races_schema = StructType([
     StructField('season',IntegerType()),
     StructField("round",IntegerType()),
     StructField("url",StringType()),
     StructField("raceName",StringType()),
     StructField("date",DateType()),
     StructField("circuitId",StringType())
     ])


# COMMAND ----------

races_df= (
    spark.read
    .format('csv')
    .option('header','true')
    .option('mode','FAILFAST')
    #.option('inferSchema','true')
    .schema(races_schema)
    .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2 Add metadata columns
# MAGIC - Source file
# MAGIC - Ingestion timestamp

# COMMAND ----------


races_final_df= add_ingestion_metadata(races_df)


# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3- Write to bronze delta table

# COMMAND ----------

# (
#     races_final_df
#     .write
#     .format('delta')
#     .mode('overwrite').option('mergeSchema', 'true')
#     .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df = races_final_df,
    target_table=table_name,
    batch_id= v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

