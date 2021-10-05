# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.option ("inferSchema", True)\
.csv("/mnt/storageaccount20210929/f1-data/raw/races.csv")

# COMMAND ----------

races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("round"), col("circuitId"), to_date(races_df.date, 'yyyy-MM-dd').alias("date"))

# COMMAND ----------

races_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/storageaccount20210929/f1-data-processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageaccount20210929/f1-data-processed/races"))
