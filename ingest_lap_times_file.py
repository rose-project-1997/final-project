# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.option("header", True) \
.schema(lap_times_schema) \
.csv("/mnt/storageaccount20210929/f1-data/raw/lap_times/lap_times_split_1.csv")

# COMMAND ----------

for i in range(2,6):
  df = spark.read \
  .option("header", True) \
  .schema(lap_times_schema) \
  .csv(f"/mnt/storageaccount20210929/f1-data/raw/lap_times/lap_times_split_{i}.csv")
  
  lap_times_df = lap_times_df.union(df)

# COMMAND ----------

lap_times_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, split

# COMMAND ----------

split_col = split(lap_times_df['time'], ':')
lap_times_df = lap_times_df.withColumn('minutes', split_col.getItem(0).astype("float"))
lap_times_df = lap_times_df.withColumn('seconds', split_col.getItem(1).astype("float"))

# COMMAND ----------

lap_times_selected_df = lap_times_df.select(col("raceId"), col("driverId"), col("lap"), col("position"), (col("minutes")*60 + col("seconds")).alias("time"))

# COMMAND ----------

lap_times_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

lap_times_renamed_df = lap_times_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/storageaccount20210929/f1-data-processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageaccount20210929/f1-data-processed/lap_times"))
