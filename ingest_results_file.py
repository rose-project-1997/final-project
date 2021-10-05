# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.option ("inferSchema", True)\
.json("/mnt/storageaccount20210929/f1-data/raw/results.json")

# COMMAND ----------

results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_selected_df = results_df.select(col("resultId"), col("raceId"), col("constructorId"), col("driverId"), col("fastestLapSpeed"), col("fastestLapTime"), col("grid"), col("points"), col("position"), col("positionOrder"),col("rank"))

# COMMAND ----------

results_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

results_renamed_df = results_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

results_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3.5 - Fix datatypes and nulls

# COMMAND ----------

results_renamed_df.select("*").filter(col("fastestLapSpeed")=="\\N").show()

# COMMAND ----------

results_nulled_df = results_renamed_df.replace("\\N", None)

# COMMAND ----------

results_nulled_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3.7 - Convert strings to numbers

# COMMAND ----------

results_nulled_df.select(results_nulled_df.fastestLapSpeed.cast("double")).show()

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

split_col = split(results_nulled_df['fastestLapTime'], ':')
results_nulled_df = results_nulled_df.withColumn('minutes', split_col.getItem(0).astype("float"))
results_nulled_df = results_nulled_df.withColumn('seconds', split_col.getItem(1).astype("float"))

# COMMAND ----------

results_nulled_df.select(col("minutes")*60).show()

# COMMAND ----------

results_type_df = results_nulled_df.select(col("result_id"), col("race_id"), col("constructor_id"), col("driver_id"), results_nulled_df.fastestLapSpeed.cast("double"), (col("minutes")*60 + col("seconds")).alias("fastestLapTime"), col("grid"), col("points"), col("position"), col("positionOrder"),col("rank"))

# COMMAND ----------

results_type_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_final_df = results_type_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

results_final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

results_final_df.write \
.format("parquet") \
.mode("overwrite") \
.save("/mnt/storageaccount20210929/f1-data-processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageaccount20210929/f1-data-processed/results"))
