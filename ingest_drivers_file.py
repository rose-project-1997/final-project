# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

drivers_df = spark.read \
.option("header", True) \
.option ("inferSchema", True)\
.json("/mnt/storageaccount20210929/f1-data/raw/drivers.json")

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import explode, udf, concat_ws

# COMMAND ----------

drivers_df.printSchema()
drivers_df.show()

# COMMAND ----------

drivers_selected_df = drivers_df.select(col("driverId"), col("dob"), concat_ws(' ',col("name.forename"),col("name.surname")).alias("name"), col("nationality"))

# COMMAND ----------

drivers_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

drivers_renamed_df = drivers_selected_df.withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

drivers_final_df = drivers_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/storageaccount20210929/f1-data-processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageaccount20210929/f1-data-processed/drivers"))
