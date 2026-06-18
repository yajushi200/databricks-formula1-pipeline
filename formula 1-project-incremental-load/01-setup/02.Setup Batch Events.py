# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Batch Events
# MAGIC - Create control schema
# MAGIC - Create batch_events table
# MAGIC - Insert an event record

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Create control schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.control
# MAGIC     MANAGED LOCATION 'abfss://formula1@databrickscourseextdlak.dfs.core.windows.net/control';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Create batch_events table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.control.batch_events
# MAGIC (
# MAGIC     batch_id INT,
# MAGIC     event_timestamp TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Insert an event record

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO formula1.control.batch_events
# MAGIC VALUES (1, current_timestamp());
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC INSERT INTO formula1.control.batch_events
# MAGIC VALUES (2, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.control.batch_events;

# COMMAND ----------

