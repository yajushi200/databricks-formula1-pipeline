# Databricks notebook source
# MAGIC %md
# MAGIC # Transform results Data
# MAGIC - Read bronze results table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case (constructorId -> contructor_id, driverId -> driver_id , raceName -> race_name, positionText -> finish_position_text)
# MAGIC - Rename columns to make them more meaningful (date -> race_date, grid -> grid_position , laps -> completed_laps, number -> car_number,position -> finish_position)
# MAGIC - Filter rows where season, round,constructorid,driverId is null(business key validation)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns race_name to Title Case
# MAGIC - Write the transformed data to silver results delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.results"
silver_table=f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read bronze results table

# COMMAND ----------

results_df=(
    spark.table(bronze_table)
    .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Keep only the columns that are required (Drop url column)

# COMMAND ----------

results_dropped_df = results_df.drop("url")

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 & 4 Standardize column names 
# MAGIC - Standardize column names using snake_case (constructorId -> contructor_id, driverId -> driver_id , raceName -> race_name, positionText -> finish_position_text)
# MAGIC - Rename columns to make them more meaningful (date -> race_date, grid -> grid_position , laps -> completed_laps, number -> car_number,position -> finish_position)

# COMMAND ----------

results_renamed_df = (
    results_dropped_df
    .withColumnsRenamed({
        "constructorId":"constructor_id",
        "driverId":"driver_id",
        "raceName":"race_name",
        "date":"race_date",
        "grid":"grid_position",
        "laps":"completed_laps",
        "number":"car_number",
        "positiontext":"finish_position_text",
        "position":"final_position"
    })
)

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 Filter rows where season, round,  constructor_id, driver_id is null (business key validation)

# COMMAND ----------

results_valid_df=(
    results_renamed_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull()
        )
)

# COMMAND ----------

# To check how many columns we dropped
display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-6 Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season","round","constructor_id","driver_id"])

# COMMAND ----------

display(results_valid_df.count()- results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-7 Transform values of columns race_name to Title Case

# COMMAND ----------

results_final_df=(
    results_distinct_df
        .withColumn("race_name",F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-8 Write the transformed data to silver results delta table

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "finish_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

# (
#     results_final_df
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(silver_table)
# )

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

