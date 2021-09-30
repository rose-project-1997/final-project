# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

constructors_df = spark.read \
.option("header", True) \
.option ("inferSchema", True)\
.json("/mnt/storageaccount20210929/f1-data/raw/constructors.json")

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_selected_df = constructors_df.select(col("constructorId"), col("name"), col("nationality"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

constructors_renamed_df = constructors_selected_df.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/storageaccount20210929/f1-data-processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageaccount20210929/f1-data-processed/constructors"))
