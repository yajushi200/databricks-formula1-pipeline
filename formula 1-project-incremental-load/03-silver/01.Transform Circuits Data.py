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
# MAGIC
# MAGIC **Changes required to implement incremental Load Processing:**
# MAGIC
# MAGIC - Accept batch_id as a parameter to the notebook
# MAGIC - Process data for only the batch_id that is being passed in (i.e, filter reading from bronze using the batch_id)
# MAGIC
# MAGIC - Add created_timestamp , updated timestamp and the batch_id to the silver table
# MAGIC
# MAGIC - Merge the processed data to the silver table
# MAGIC     - created_timestamp should only be populated at the time of inserting/creating record. It should not be updated during th merge update.
# MAGIC     - Ensure that we are not overwriting the data in the silver table by old bronze data (re-run scenario)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# Creating a variable (bronze_table) which has the table name of our bronze table

bronze_table= f"{catalog_name}.{bronze_schema}.circuits"

# silver_table - required to write the data

silver_table= f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read bronze circuits table

# COMMAND ----------

# circuits_df=spark.table(bronze_table)

# COMMAND ----------

circuits_df =(
    spark.table(bronze_table)
    .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Keep only the columns that are required (Drop url column)

# COMMAND ----------

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
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step -3 & 4 Standardize column names 
# MAGIC - Standardize column names using snake_case `(circuitId -> circuit_id)`
# MAGIC - Rename columns to make it more meaningful (lat -> latitude and long-> longitude)
# MAGIC

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

# circuits_final_df=(
#     circuits_final_df
#     .withColumn("current_timestamp" , F.current_timestamp())
#     .withColumn("updated_timestamp", F.current_timestamp())
# )

# COMMAND ----------

# circuits_final_df = (
#     circuits_final_df
#     .drop("current_timestamp")        # remove the wrongly named one
#     .drop("updated_timestamp")        # remove duplicates first
#     .withColumn("current_timestamp", F.current_timestamp())  
#     .withColumn("updated_timestamp", F.current_timestamp())    
# )

# COMMAND ----------

# DBTITLE 1,Cell 28

# if not spark.catalog.tableExists(silver_table):
#     (
#         circuits_final_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .saveAsTable(silver_table)
#     )
# else:
#     from delta.tables import DeltaTable

#     delta_table = DeltaTable.forName(spark, silver_table)
#     (
#         delta_table.alias("t")
#         .merge(
#             circuits_final_df.alias("s"), # give the source table 
#             "t.circuit_id = s.circuit_id"
#         )
#         # pass the values as dictionary
#         .whenMatchedUpdate(
#             # the re-run scenario avoid condition
#             condition="s.batch_id >= t.batch_id",
#             set={
#                 "circuit_name": "s.circuit_name",
#                 "latitude":"s.latitude",
#                 "longitude": "s.longitude",
#                 "locality" : "s.locality",
#                 "country": "s.country",
#                 "ingestion_timestamp":"s.ingestion_timestamp",
#                 "source_file" : "s.source_file",
#                 "batch_id": "s.batch_id",
#                 "updated_timestamp" :"s.updated_timestamp"
#             }  
#         )
#         .whenNotMatchedInsertAll()
#         .execute()
#     )

# COMMAND ----------

write_to_silver(
    input_df = circuits_final_df,
    target_table= silver_table,
    merge_condition = "t.circuit_id = s.circuit_id",
    columns_to_update = [
     "circuit_name",
     "latitude",
     "longitude",
     "locality",
     "country",
     "ingestion_timestamp",
     "source_file",
     "batch_id",
    ]

)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

