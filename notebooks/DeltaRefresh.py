# Databricks notebook source

# Assigning variable to source files(s3) into strings
parks = 's3://squirrelcensus/park-data.csv'
squirrel= 's3://squirrelcensus/squirrel-data.csv'
stories = 's3://squirrelcensus/stories.csv'


# COMMAND ----------


#Creating respective dataframes 
parksdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(parks)

#creating squirrel dataframe
squirrelsdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(squirrel)

#creating stories dataframe
storiesdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(stories)

# COMMAND ----------

#parks column name changes
parksRenamedColsDf= parksdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Date", "date").withColumnRenamed("Start Time", "start_time").withColumnRenamed("End Time", "end_time").withColumnRenamed("Total Time (in minutes, if available)", "total_time_in_minutes").withColumnRenamed("Park Conditions", "park_conditions").withColumnRenamed("Other Animal Sightings", "other_animal_sightings").withColumnRenamed("Litter", "litter").withColumnRenamed("Temperature & Weather", "temperature_and_weather").withColumnRenamed("Number of Squirrels", "number_of_squirrels").withColumnRenamed("Squirrel Sighter(s)", "squirrel_sighters").withColumnRenamed("Number of Sighters", "number_of_sighters")

# COMMAND ----------


# squirrels column name changes
squirrelsRenamedColsDf = squirrelsdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Squirrel ID", "squirrel_id").withColumnRenamed("Primary Fur Color", "primary_fur_color").withColumnRenamed("Highlights in Fur Color", "highlights_in_fur_color").withColumnRenamed("Color Notes", "color_notes").withColumnRenamed("Location", "location").withColumnRenamed("Above Ground (Height in Feet)", "above_ground_height").withColumnRenamed("Specific Location", "specific_location").withColumnRenamed("Activities", "activities").withColumnRenamed("Interactions with Humans", "interactions_with_humans").withColumnRenamed("Other Notes or Observations", "other_observations").withColumnRenamed("Squirrel Latitude (DD.DDDDDD)", "squirrel_latitude").withColumnRenamed("Squirrel Longitude (-DD.DDDDDD)", "squirrel_longitude")


# COMMAND ----------


# stories column name changes
storiesRenamedColsDf = storiesdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Squirrels, Parks & The City Stories", "squirrels_parks_city_stories")


# COMMAND ----------


import pyspark 
from pyspark.sql.functions import *

parksNewColumnAdditionDF = parksRenamedColsDf.withColumn("etl_timestamp", current_timestamp())
squirrelsNewColumnAdditionDF = squirrelsRenamedColsDf.withColumn("etl_timestamp", current_timestamp())
storiesNewColumnAdditionDF = storiesRenamedColsDf.withColumn("etl_timestamp", current_timestamp())

# COMMAND ----------



# COMMAND ----------


#creating tables from parks source file
parksNewColumnAdditionDF.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_parks_new")

#creating tables from squirrels source file 
squirrelsNewColumnAdditionDF.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_squirrels_new")

#creating tables from stories  source file
storiesNewColumnAdditionDF.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_stories_new")


# COMMAND ----------


display(spark.read.table("resq_testdemo.staging_parks_new"))


# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- create database resq_base
# MAGIC create database if not exists resq_base

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC -- insert data into resq_base.staging_parks
# MAGIC insert into resq_base.staging_parks
# MAGIC select * from resq_testdemo.staging_parks_new 
# MAGIC where date(etl_timestamp) = '2023-03-07' 
