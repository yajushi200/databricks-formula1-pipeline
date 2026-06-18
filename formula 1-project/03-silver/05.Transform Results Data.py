# Databricks notebook source
# MAGIC %md
# MAGIC # Transform sprints Data
# MAGIC - Read bronze sprints table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case (constructorId -> contructor_id, driverId -> driver_id , raceName -> race_name, positionText -> finish_position_text)
# MAGIC - Rename columns to make them more meaningful (date -> race_date, grid -> grid_position , laps -> completed_laps, number -> car_number,position -> finish_position)
# MAGIC - Filter rows where season, round,constructorid,driverId is null(business key validation)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns race_name to Title Case
# MAGIC - Write the transformed data to silver sprints delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.sprints"
silver_table=f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 -4 Read , transform and rename columns 

# COMMAND ----------

sprints_df=(
    spark.table(bronze_table)
    .drop("url")
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

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 & 6 Data Quality Checks

# COMMAND ----------

sprints_valid_df=(
    sprints_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull()
        )
        .dropDuplicates(["season","round","constructor_id","driver_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-7 Transform values of columns race_name to Title Case

# COMMAND ----------

sprints_final_df=(
    sprints_valid_df
        .withColumn("race_name",F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-8 Write the transformed data to silver sprints delta table

# COMMAND ----------

(
    sprints_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

