# Databricks notebook source
storage_account_name = "storageaccount20210929"
container_name = "f1-data-processed"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import all of the data

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.parquet(f"/mnt/{storage_account_name}/{container_name}/circuits")

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.parquet(f"/mnt/{storage_account_name}/{container_name}/races")

# COMMAND ----------

constructors_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.parquet(f"/mnt/{storage_account_name}/{container_name}/constructors")

# COMMAND ----------

drivers_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.parquet(f"/mnt/{storage_account_name}/{container_name}/drivers")

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.parquet(f"/mnt/{storage_account_name}/{container_name}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Try some joins etc

# COMMAND ----------

results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id).select(drivers_df.name, results_df.constructor_id, results_df.fastestLapTime).orderBy(results_df.fastestLapTime).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id).join(constructors_df, results_df.constructor_id == constructors_df.constructor_id).select(drivers_df.name.alias("Driver"), constructors_df.name.alias("Constructor"), results_df.fastestLapTime).orderBy(results_df.fastestLapTime).filter(results_df.fastestLapTime.isNotNull()).display()

# COMMAND ----------

results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
.join(races_df, results_df.race_id == races_df.race_id) \
.select(drivers_df.name.alias("Driver"), constructors_df.name.alias("Constructor"), results_df.fastestLapTime) \
.orderBy(results_df.fastestLapTime) \
.filter(results_df.fastestLapTime.isNotNull()) \
.filter(col("date") > "2011-01-01") \
.display()
