# Databricks notebook source
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC - Read silver `races` table
# MAGIC - read silver `circuits` table
# MAGIC - Join the data from `races` with `circuits` using `circuit_id`
# MAGIC - Select the required columns
# MAGIC     - races.season
# MAGIC     - races.round
# MAGIC     - races.race_name
# MAGIC     - circuits.circuit_name
# MAGIC     - circuits.locality
# MAGIC     - circuits.country
# MAGIC - Write the transformed data to gold `dim_races` table
# MAGIC
# MAGIC **Below are the changes that are required to implement incremental Load Processing**
# MAGIC
# MAGIC - Accept batch_id as a parameter to the notebook
# MAGIC - Process data only for the batch_id being processed in (i.e., filter reading from silver using batch_id)
# MAGIC - Add craeted timestamp, updated_timestamp to the gold table
# MAGIC - Merge the processed data to the gold table
# MAGIC     - created timestamp shpuld only be populated at the time of inserting/creating record. It should not be updated during merge update.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

target_table=f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read source tables
# MAGIC - circuits
# MAGIC - races

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
    .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------


races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
    .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 - Join the data from `races` with `circuits` using `circuit_id`
# MAGIC Select the required columns:
# MAGIC - races.season
# MAGIC - races.round
# MAGIC - races.race_name
# MAGIC - circuits.circuit_name
# MAGIC - circuits.locality
# MAGIC - circuits.country

# COMMAND ----------

dim_races_df = (
    races_df
        .join(
        circuits_df,
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
        )
        .select(
            races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.race_date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country
        )
)

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Write the transformed data to gold target table

# COMMAND ----------

# (
#     dim_races_df
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(target_table)
# )

# COMMAND ----------

write_to_gold (
    input_df = dim_races_df,
    target_table= target_table,
    merge_condition = "t.season = s.season AND t.round = s.round",
    columns_to_update =[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

