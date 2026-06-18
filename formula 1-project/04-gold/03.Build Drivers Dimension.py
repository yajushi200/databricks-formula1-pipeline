# Databricks notebook source
# MAGIC %md
# MAGIC # Build Drivers Dimension
# MAGIC - Read silver drivers table
# MAGIC - Read gold ref_nationality_region table
# MAGIC - Join the data with ref_nationality_region using nationality
# MAGIC - Select the required columns
# MAGIC     - drivers.driver_id
# MAGIC     - drivers.driver_name
# MAGIC     - drivers.date_of_birth
# MAGIC     - drivers.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC - Write the transformed data to gold dim_drivers table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table=f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step -1 Read sources table
# MAGIC - drivers table
# MAGIC - ref_nationality_region

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df=spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Join the data with ref_nationality_region using nationality
# MAGIC Select the required columns
# MAGIC - drivers.driver_id
# MAGIC - drivers.driver_name
# MAGIC - drivers.date_of_birth
# MAGIC - drivers.nationality
# MAGIC - ref_nationality_region.region

# COMMAND ----------

dim_drivers_df = (
    drivers_df
    .join(
        ref_nationality_region_df,
        drivers_df.nationality == ref_nationality_region_df.nationality,
        "left"
    )
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Write the transformed data to gold dim_drivers table

# COMMAND ----------

(
    dim_drivers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

