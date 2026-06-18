# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json
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

# Define source file and table name

source_file=f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name =f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-1 Read the JSON file using the dataframe reader API

# COMMAND ----------

# Define Schema (DDL format)
constructors_schema= """constructorId STRING, 
                        name STRING,
                        nationality STRING ,
                        url STRING
                        """


# COMMAND ----------

# Read the constructors file
constructors_df=(
    spark.read
        .format('json')
        .option('mode','FAILFAST')
        .schema(constructors_schema)
        .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -2 Adding metadata 

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -3 Writing the data in form of delta table to bronze layer

# COMMAND ----------

# (
#         constructors_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df= constructors_final_df,
    target_table=table_name,
    batch_id= v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

