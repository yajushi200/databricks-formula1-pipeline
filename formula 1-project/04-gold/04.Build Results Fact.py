# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact Table
# MAGIC - Read silver `results` table
# MAGIC - Read silver `sprints` table
# MAGIC - Add new column `session_type` with values RACE or SPRINT
# MAGIC - UNION `results` and `sprints`
# MAGIC - Derive additional columns:
# MAGIC     - is_win -> indicates that the driver won (true or false)
# MAGIC     - is_podium -> indicates that the driver scored a podium result(1,2,3)
# MAGIC     - has_points -> indicates that the driver has scored points
# MAGIC - Write the transformed data to gold `fact_session_results` table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

target_table= f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 & 2 Read sources tables & Adding new column session_type with values RACE or SPRINT
# MAGIC - Results silver table
# MAGIC - Sprints silver table
# MAGIC

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .withColumn("session_type",F.lit("RACE"))
        .drop("race_name","race_date","ingestion_timestamp","source_file")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
        .withColumn("session_type",F.lit("SPRINT"))
        .drop("race_name","race_date","ingestion_timestamp","source_file")        
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 UNION results and sprints

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-4 Derive additional columns:
# MAGIC - is_win -> indicates that the driver won (true or false)
# MAGIC - is_podium -> indicates that the driver scored a podium result(1,2,3)
# MAGIC - has_points -> indicates that the driver has scored points

# COMMAND ----------

fact_session_results= (
    results_sprints_df
    .withColumn("is_win",F.col("final_position") == 1)
    .withColumn("is_podium",F.col("final_position").between(1,3))
    .withColumn("has_points",F.col("points") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 Write the transformed data to gold fact_session_results table

# COMMAND ----------

(
    fact_session_results
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

display(spark.table(target_table).filter("season = 2025"))

# COMMAND ----------

