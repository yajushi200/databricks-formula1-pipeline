-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project enviornemnt for the formula 1 project
-- MAGIC
-- MAGIC 1.  Create the external location (as we already have the external location we will clone the notebook or copy the code by opening the notebooks in multiple tabs)
-- MAGIC
-- MAGIC 2. Create Catalog -formula1_incr
-- MAGIC 3. Create schemas - landing, bronze, silver and gold
-- MAGIC 4. Create Volume named Files in the landing schema
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dlak_formula1_incr
URL 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula-1-incr container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Catalog formula 1-incr

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_incr
   MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/' 
   COMMENT 'This is the main catalog for the formula 1-incr project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create schemas- landing, bronze silver, gold

-- COMMAND ----------

CREATE  SCHEMA IF NOT EXISTS  formula1_incr.landing;
CREATE  SCHEMA IF NOT EXISTS  formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/bronze';
CREATE  SCHEMA IF NOT EXISTS  formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/silver';
CREATE  SCHEMA IF NOT EXISTS  formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/gold'

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

use catalog formula1_incr;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@databrickscourseextdlak.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files

-- COMMAND ----------

