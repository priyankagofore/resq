# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists resq_testdemo.squirrel_activities;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What are the activities squirrels are doing and how many of them are there under different parks and park conditions
# MAGIC create table resq_testdemo.squirrel_activities
# MAGIC as 
# MAGIC select sq.park_name, 
# MAGIC case 
# MAGIC when pk.park_conditions = '"""Cool"" - Hank"' then 'cool' 
# MAGIC when pk.park_conditions like '%baseball%' then 'baseball_playing_time' 
# MAGIC when pk.park_conditions like '%ppl%' then '20_to_30_people_around'
# MAGIC when pk.park_conditions like '%hawk%' then 'hawk_around'
# MAGIC when pk.park_conditions is null then 'no_info'
# MAGIC else pk.park_conditions
# MAGIC end as park_conditions,
# MAGIC pk.temperature_and_weather,
# MAGIC sq.activities, 
# MAGIC count(sq.activities) as sq_activity_count 
# MAGIC from 
# MAGIC resq_testdemo.staging_squirrels sq 
# MAGIC join resq_testdemo.staging_parks pk
# MAGIC on sq.park_id = pk.park_id
# MAGIC where sq.activities is not null and pk.park_conditions is not null 
# MAGIC group by sq.park_name, 
# MAGIC pk.park_conditions,
# MAGIC pk.temperature_and_weather,
# MAGIC sq.activities 
# MAGIC order by sq_activity_count

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from resq_testdemo.squirrel_activities;