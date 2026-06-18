# Databricks notebook source
# MAGIC %md
# MAGIC ### Build Constructors Dimension
# MAGIC - Read silver constructors table
# MAGIC - Read gold ref_nationality_region table
# MAGIC - Join the data from constructors with ref_nationality_region using nationality
# MAGIC - Select the required columns:
# MAGIC     -         constructors.constructor_id
# MAGIC     -         constructors.constructor_name
# MAGIC     -         constructors.nationality
# MAGIC     -         ref_nationality_region.region
# MAGIC - Write the transformed data to gold dim_constructors table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

target_table=f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read source tables
# MAGIC - constructors table
# MAGIC - ref_nationality_region 

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
    .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

ref_nationality_region_df = (
    spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Join the data from constructors with ref_nationality_region using nationality
# MAGIC Select the required columns:
# MAGIC - constructors.constructor_id
# MAGIC - constructors.constructor_name
# MAGIC - constructors.nationality
# MAGIC - ref_nationality_region.region

# COMMAND ----------

dim_constructors_df= (
    constructors_df
    .join(
        ref_nationality_region_df,
        constructors_df.nationality==ref_nationality_region_df.nationality,
        "left"
    )
    .select
    (
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
    )

)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Write the transformed data to gold target table

# COMMAND ----------

# (
#     dim_constructors_df
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(target_table)
# )

# COMMAND ----------

write_to_gold (
    input_df = dim_constructors_df,
    target_table= target_table,
    merge_condition = "t.constructor_id = s.constructor_id",
    columns_to_update =[
        "constructor_name",
        "nationality",
        "nationality_region",
    ]
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

