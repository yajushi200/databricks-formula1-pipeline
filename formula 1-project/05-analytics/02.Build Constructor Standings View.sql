-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Constructor Standings
-- MAGIC
-- MAGIC ### Sources
-- MAGIC - fact_sessions_results
-- MAGIC - dim_constructors
-- MAGIC
-- MAGIC ### Output Columns
-- MAGIC - season
-- MAGIC - constructor_id
-- MAGIC - constructor_name
-- MAGIC - nationality
-- MAGIC - race starts
-- MAGIC - total points
-- MAGIC - number of wins
-- MAGIC - number of podiums
-- MAGIC - standing position
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.v_constructor_standing
AS
WITH constructor_session_summary
AS
(SELECT 
    r.season,
    c.constructor_id,
    c.constructor_name,
    c.nationality,
    COUNT (*) AS race_starts,
    SUM(r.points) AS total_points,
    COUNT_IF(r.is_win) AS number_of_wins,
    COUNT_IF(r.is_podium) AS number_of_podiums
FROM formula1.gold.fact_session_results r
JOIN formula1.gold.dim_constructors c
        ON r.constructor_id = c.constructor_id
GROUP BY 
    r.season,
    c.constructor_id,
    c.constructor_name,
    c.nationality
)
SELECT 
    season,
    constructor_id,
    constructor_name,
    nationality,
    race_starts,
    total_points,
    number_of_wins,
    number_of_podiums,
    RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing
FROM constructor_session_summary

-- COMMAND ----------

select * from formula1.gold.v_constructor_standing where season = 2025

-- COMMAND ----------

