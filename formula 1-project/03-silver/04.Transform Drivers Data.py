# Databricks notebook source
# MAGIC %md
# MAGIC # Transform drivers Data
# MAGIC - Read bronze drivers table
# MAGIC - Keep only the columns that are required (Drop url column)
# MAGIC - Standardize column names using snake_case (driverId -> driver_id , dateOfBirth -> date_of_birth)
# MAGIC - Concatenate name.givenName and name.familyName to create a new column called driver_name and transform the value to Title Case
# MAGIC - Remove duplicate records
# MAGIC - Transform values of columns nationality to Title Case
# MAGIC - Write the transformed data to silver drivers delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.drivers"
silver_table=f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read bronze drivers table

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Keep only the columns that are required (Drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Standardize column names using snake_case (driverId -> driver_id , dateOfBirth -> date_of_birth)

# COMMAND ----------

drivers_renamed_df= (
    drivers_dropped_df
    .withColumnsRenamed(
        {"driverId":"driver_id",
        "dateOfBirth":"date_of_birth"
        }
    )
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-4 Concatenate name.givenName and name.familyName to create a new column called driver_name and transform the value to Title Case
# MAGIC

# COMMAND ----------

drivers_concatenated_df= (
    drivers_renamed_df
    .withColumn("driver_name",
                F.initcap(F.concat_ws(" ",F.col("name.givenName"),("name.familyName"))))
    .drop("name")
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 Remove duplicate records

# COMMAND ----------

drivers_distinct_df=drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-6 Transform values of columns nationality to Title Case

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn("nationality",F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-7 Write the transformed data to silver drivers delta table

# COMMAND ----------

(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)


# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

