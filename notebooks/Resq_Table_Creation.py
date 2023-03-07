# Databricks notebook source
# Assigning variable to source files(s3) into strings
parks = 's3://squirrelcensus/park-data.csv'
squirrel= 's3://squirrelcensus/squirrel-data.csv'
stories = 's3://squirrelcensus/stories.csv'

# COMMAND ----------

print(parks)
print(squirrel)
print(stories)

# COMMAND ----------

#Creating respective dataframes 
parksdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(parks)

# COMMAND ----------

display(parksdf)

# COMMAND ----------

#creating squirrel dataframe
squirrelsdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(squirrel)

# COMMAND ----------

#creating stories dataframe
storiesdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(stories)

# COMMAND ----------

display(parksdf)
display(squirrelsdf)
display(storiesdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists resq_testdemo

# COMMAND ----------

#parks column name changes
parksRenamedColsDf= parksdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Date", "date").withColumnRenamed("Start Time", "start_time").withColumnRenamed("End Time", "end_time").withColumnRenamed("Total Time (in minutes, if available)", "total_time_in_minutes").withColumnRenamed("Park Conditions", "park_conditions").withColumnRenamed("Other Animal Sightings", "other_animal_sightings").withColumnRenamed("Litter", "litter").withColumnRenamed("Temperature & Weather", "temperature_and_weather").withColumnRenamed("Number of Squirrels", "number_of_squirrels").withColumnRenamed("Squirrel Sighter(s)", "squirrel_sighters").withColumnRenamed("Number of Sighters", "number_of_sighters")

# COMMAND ----------

#creating tables from parks source file
parksRenamedColsDf.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_parks")

# COMMAND ----------

# squirrels column name changes
squirrelsRenamedColsDf = squirrelsdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Squirrel ID", "squirrel_id").withColumnRenamed("Primary Fur Color", "primary_fur_color").withColumnRenamed("Highlights in Fur Color", "highlights_in_fur_color").withColumnRenamed("Color Notes", "color_notes").withColumnRenamed("Location", "location").withColumnRenamed("Above Ground (Height in Feet)", "above_ground_height").withColumnRenamed("Specific Location", "specific_location").withColumnRenamed("Activities", "activities").withColumnRenamed("Interactions with Humans", "interactions_with_humans").withColumnRenamed("Other Notes or Observations", "other_observations").withColumnRenamed("Squirrel Latitude (DD.DDDDDD)", "squirrel_latitude").withColumnRenamed("Squirrel Longitude (-DD.DDDDDD)", "squirrel_longitude")

# COMMAND ----------

#creating tables from squirrels source file 
squirrelsRenamedColsDf.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_squirrels")

# COMMAND ----------

# stories column name changes
storiesRenamedColsDf = storiesdf.withColumnRenamed("Area Name", "area_name").withColumnRenamed("Area ID", "area_id").withColumnRenamed("Park Name", "park_name").withColumnRenamed("Park ID", "park_id").withColumnRenamed("Squirrels, Parks & The City Stories", "squirrels_parks_city_stories")

# COMMAND ----------

#creating tables from stories  source file
storiesRenamedColsDf.write.format("delta").mode("overwrite").saveAsTable("resq_testdemo.staging_stories")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To show the metadata - columns, datatypes, partition info and also location.
# MAGIC describe extended resq_testdemo.staging_stories

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/resq_testdemo.db/staging_stories"))

# COMMAND ----------

#Column Addition
import pyspark 
from pyspark.sql.functions import *
parksWithTimeStampDf = parksdf.withColumn("timestamp",current_timestamp())
display(parksWithTimeStampDf)