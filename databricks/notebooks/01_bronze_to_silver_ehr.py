# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: EHR Visits Processing
# MAGIC 
# MAGIC **Purpose:** Transform raw EHR visit data from Bronze to clean Silver layer
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Data cleansing (nulls, duplicates)
# MAGIC - Date standardization
# MAGIC - Derived metrics (length_of_stay calculation)
# MAGIC - Data quality checks

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Define storage paths
bronze_path = "abfss://bronze@<storage_account>.dfs.core.windows.net/ehr/"
silver_path = "abfss://silver@<storage_account>.dfs.core.windows.net/fact_visit/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

# Read raw EHR data from Bronze
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

print(f"Bronze records count: {df_bronze.count()}")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for nulls in critical columns
print("\nNull counts by column:")
df_bronze.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_bronze.columns]).display()

# Check for duplicates
duplicates = df_bronze.groupBy("visit_id").count().filter(F.col("count") > 1)
print(f"\nDuplicate visit_ids found: {duplicates.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# Apply transformations
df_silver = df_bronze \
    .dropDuplicates(["visit_id"]) \
    .filter(F.col("visit_id").isNotNull()) \
    .withColumn("admit_date", F.to_date(F.col("admit_date"))) \
    .withColumn("discharge_date", F.to_date(F.col("discharge_date"))) \
    .withColumn("length_of_stay", 
                F.datediff(F.col("discharge_date"), F.col("admit_date"))) \
    .withColumn("age_group", 
                F.when(F.col("age") < 40, "<40")
                 .when((F.col("age") >= 40) & (F.col("age") < 65), "40-64")
                 .otherwise("65+")) \
    .withColumn("processing_timestamp", F.current_timestamp()) \
    .withColumn("data_source", F.lit("EHR_System"))

print(f"\nSilver records count after transformation: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------

# Validate length of stay is positive
invalid_los = df_silver.filter(F.col("length_of_stay") < 0)
print(f"Records with negative length_of_stay: {invalid_los.count()}")

if invalid_los.count() > 0:
    print("\nInvalid records:")
    invalid_los.display()
    # Filter out invalid records
    df_silver = df_silver.filter(F.col("length_of_stay") >= 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer (Delta)

# COMMAND ----------

# Write to Delta format with partition
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(silver_path)

print(f"\nSuccessfully written {df_silver.count()} records to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# Display summary
print("\nSummary by Hospital and Age Group:")
df_silver.groupBy("hospital_id", "age_group") \
    .agg(
        F.count("*").alias("visit_count"),
        F.avg("length_of_stay").alias("avg_length_of_stay"),
        F.max("age").alias("max_age")
    ) \
    .orderBy("hospital_id", "age_group") \
    .display()

print("\n✅ Bronze to Silver transformation completed successfully!")
