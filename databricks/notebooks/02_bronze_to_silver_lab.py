# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Lab Results Processing
# MAGIC 
# MAGIC **Purpose:** Transform raw lab results data from Bronze to clean Silver layer
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Data cleansing and validation
# MAGIC - Standardize test names and result flags
# MAGIC - Filter invalid test results
# MAGIC - Join with visit data for context

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

bronze_path = "abfss://bronze@<storage_account>.dfs.core.windows.net/lab_results/"
silver_path = "abfss://silver@<storage_account>.dfs.core.windows.net/fact_lab_result/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

print(f"Bronze lab results count: {df_bronze.count()}")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("Null counts:")
df_bronze.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_bronze.columns]).display()

# Check for negative or invalid test values
invalid_results = df_bronze.filter(
    (F.col("test_result_value").isNull()) | 
    (F.col("test_result_value") < 0)
)
print(f"\nInvalid test results: {invalid_results.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

df_silver = df_bronze \
    .dropDuplicates(["lab_id"]) \
    .filter(F.col("lab_id").isNotNull()) \
    .filter(F.col("test_result_value").isNotNull()) \
    .withColumn("test_date", F.to_date(F.col("test_date"))) \
    .withColumn("test_name_standardized", 
                F.trim(F.upper(F.col("test_name")))) \
    .withColumn("result_flag_clean",
                F.when(F.upper(F.col("result_flag")).isin(["HIGH", "H"]), "High")
                 .when(F.upper(F.col("result_flag")).isin(["LOW", "L"]), "Low")
                 .otherwise("Normal")) \
    .withColumn("is_critical",
                F.when(F.col("result_flag_clean") != "Normal", 1)
                 .otherwise(0)) \
    .withColumn("processing_timestamp", F.current_timestamp()) \
    .withColumn("data_source", F.lit("Lab_System"))

print(f"\nSilver records after transformation: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Result Categories

# COMMAND ----------

# Categorize common tests
df_silver = df_silver.withColumn("test_category",
    F.when(F.col("test_name_standardized").contains("GLUCOSE"), "Metabolic")
     .when(F.col("test_name_standardized").contains("HBA1C"), "Metabolic")
     .when(F.col("test_name_standardized").contains("CREATININE"), "Renal")
     .when(F.col("test_name_standardized").contains("BUN"), "Renal")
     .when(F.col("test_name_standardized").contains("TROPONIN"), "Cardiac")
     .when(F.col("test_name_standardized").contains("BNP"), "Cardiac")
     .when(F.col("test_name_standardized").contains("WBC"), "Hematology")
     .otherwise("Other")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(silver_path)

print(f"\nSuccessfully written {df_silver.count()} lab results to Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("\nLab Results Summary by Test Category and Flag:")
df_silver.groupBy("test_category", "result_flag_clean") \
    .agg(
        F.count("*").alias("test_count"),
        F.avg("test_result_value").alias("avg_result_value")
    ) \
    .orderBy("test_category", "result_flag_clean") \
    .display()

print("\nBronze to Silver lab transformation completed!")
