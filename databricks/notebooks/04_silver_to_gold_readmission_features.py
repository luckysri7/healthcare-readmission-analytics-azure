# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER TO GOLD: Build Readmission Features
# MAGIC 
# MAGIC **Critical transformation using window functions to identify 30-day readmissions**
# MAGIC 
# MAGIC Joins: Visits + Labs + Claims from Silver layer
# MAGIC Output: Gold layer fact_readmission_risk

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Starting Silver to Gold transformation...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Layer Data

# COMMAND ----------

# Define paths
silver_visit = "abfss://silver@<storage>.dfs.core.windows.net/fact_visit/"
silver_lab = "abfss://silver@<storage>.dfs.core.windows.net/fact_lab_result/"
silver_claim = "abfss://silver@<storage>.dfs.core.windows.net/fact_claim/"
gold_path = "abfss://gold@<storage>.dfs.core.windows.net/fact_readmission_risk/"

# Read Delta tables
df_visit = spark.read.format("delta").load(silver_visit)
df_lab = spark.read.format("delta").load(silver_lab)
df_claim = spark.read.format("delta").load(silver_claim)

print(f"Loaded: {df_visit.count()} visits | {df_lab.count()} labs | {df_claim.count()} claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KEY LOGIC: Identify 30-Day Readmissions Using Window Functions

# COMMAND ----------

# Create window partitioned by patient, ordered by discharge date
window_spec = Window.partitionBy("patient_id").orderBy("discharge_date")

# Use lead() to get next visit for same patient
df_readmission = df_visit \
    .withColumn("next_admit_date", F.lead("admit_date").over(window_spec)) \
    .withColumn("next_visit_id", F.lead("visit_id").over(window_spec)) \
    .withColumn("days_to_readmission", 
                F.datediff(F.col("next_admit_date"), F.col("discharge_date"))) \
    .withColumn("is_readmitted_30d",
                F.when((F.col("days_to_readmission") <= 30) & 
                       (F.col("days_to_readmission") >= 0), 1)
                 .otherwise(0))

readmissions = df_readmission.filter(F.col("is_readmitted_30d") == 1).count()
print(f"30-day readmissions identified: {readmissions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Lab Features per Visit

# COMMAND ----------

lab_features = df_lab.groupBy("visit_id").agg(
    F.count("*").alias("lab_test_count"),
    F.sum(F.when(F.col("is_critical") == 1, 1).otherwise(0)).alias("critical_lab_count"),
    F.countDistinct("test_category").alias("unique_test_categories"),
    F.max(F.when(F.col("test_category") == "Cardiac", F.col("test_result_value"))).alias("max_cardiac_marker")
)

print(f"Lab features aggregated for {lab_features.count()} visits")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join All Layers and Create Risk Score

# COMMAND ----------

df_gold = df_readmission \
    .join(lab_features, "visit_id", "left") \
    .join(df_claim.select("visit_id", "total_billed_amount", "insurance_type_clean", "patient_responsibility"), 
          "visit_id", "left") \
    .withColumn("risk_score",
                F.when(F.col("age") > 65, 2).otherwise(1) +
                F.when(F.col("critical_lab_count") > 2, 2).otherwise(0) +
                F.when(F.col("length_of_stay") > 5, 1).otherwise(0)) \
    .withColumn("high_risk_flag", F.when(F.col("risk_score") >= 4, 1).otherwise(0)) \
    .withColumn("processing_timestamp", F.current_timestamp()) \
    .withColumn("data_layer", F.lit("gold"))

print(f"Gold records created: {df_gold.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_path)

print("Silver to Gold transformation COMPLETE! Readmission features built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("Readmission Summary:")
df_gold.groupBy("is_readmitted_30d").count().display()

print("\nRisk Score Distribution:")
df_gold.groupBy("risk_score").count().orderBy("risk_score").display()
