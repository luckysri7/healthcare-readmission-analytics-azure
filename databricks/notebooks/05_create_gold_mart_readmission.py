# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Create Readmission Analytics Mart
# MAGIC 
# MAGIC **Purpose**: Aggregates the Gold fact table into business-ready KPI mart for Power BI dashboards
# MAGIC 
# MAGIC **Transformations**:
# MAGIC - Calculate readmission rate by facility, specialty, month
# MAGIC - Aggregate total costs by patient cohort
# MAGIC - Identify high-risk patient segments
# MAGIC - Create month-over-month trends
# MAGIC - Build executive summary metrics
# MAGIC 
# MAGIC **Output**: `gold/mart_readmission_analytics` (Delta table optimized for BI queries)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, round as _round, 
    date_format, month, year, when, countDistinct
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Gold Fact Table

# COMMAND ----------

df_fact = spark.read.format("delta").load("/mnt/gold/fact_readmission_risk/")

print(f"Total records in fact table: {df_fact.count()}")
df_fact.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregated Mart

# COMMAND ----------

# Calculate KPIs by facility and month
mart_facility_month = df_fact \
    .withColumn("admission_year", year(col("admission_date"))) \
    .withColumn("admission_month", month(col("admission_date"))) \
    .groupBy("facility_id", "admission_year", "admission_month") \
    .agg(
        countDistinct("patient_id").alias("unique_patients"),
        count("*").alias("total_visits"),
        _sum(when(col("is_readmitted_30d") == True, 1).otherwise(0)).alias("readmissions_30d"),
        _round(avg("risk_score"), 2).alias("avg_risk_score"),
        _round(_sum("total_charges"), 2).alias("total_revenue"),
        _round(avg("days_to_readmission"), 1).alias("avg_days_to_readmission")
    )

# Calculate readmission rate
mart_facility_month = mart_facility_month \
    .withColumn(
        "readmission_rate_pct",
        _round((col("readmissions_30d") / col("total_visits")) * 100, 2)
    )

print("Facility-Month Mart Sample:")
mart_facility_month.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-Risk Patient Segment Analysis

# COMMAND ----------

# Identify high-risk cohorts
mart_risk_segments = df_fact \
    .withColumn(
        "risk_category",
        when(col("risk_score") >= 7, "High Risk")
        .when(col("risk_score") >= 4, "Medium Risk")
        .otherwise("Low Risk")
    ) \
    .groupBy("risk_category") \
    .agg(
        count("*").alias("patient_count"),
        _sum(when(col("is_readmitted_30d") == True, 1).otherwise(0)).alias("readmissions"),
        _round(avg("total_charges"), 2).alias("avg_charges_per_visit"),
        _round(avg("length_of_stay"), 1).alias("avg_length_of_stay")
    ) \
    .withColumn(
        "readmission_rate_pct",
        _round((col("readmissions") / col("patient_count")) * 100, 2)
    )

print("Risk Segment Analysis:")
mart_risk_segments.orderBy(col("readmission_rate_pct").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specialty Performance

# COMMAND ----------

mart_specialty = df_fact \
    .groupBy("specialty") \
    .agg(
        count("*").alias("total_visits"),
        _sum(when(col("is_readmitted_30d") == True, 1).otherwise(0)).alias("readmissions_30d"),
        _round(avg("risk_score"), 2).alias("avg_risk_score"),
        _round(_sum("total_charges"), 2).alias("total_revenue")
    ) \
    .withColumn(
        "readmission_rate_pct",
        _round((col("readmissions_30d") / col("total_visits")) * 100, 2)
    ) \
    .orderBy(col("readmission_rate_pct").desc())

print("Specialty Performance:")
mart_specialty.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Combined Mart to Gold Layer

# COMMAND ----------

# Union all mart dimensions for comprehensive analytics
final_mart = mart_facility_month \
    .select(
        col("facility_id").alias("dimension_key"),
        col("admission_year"),
        col("admission_month"),
        col("unique_patients"),
        col("total_visits"),
        col("readmissions_30d"),
        col("readmission_rate_pct"),
        col("avg_risk_score"),
        col("total_revenue")
    )

mart_path = "/mnt/gold/mart_readmission_analytics/"

final_mart.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(mart_path)

print("Gold Mart COMPLETE! Readmission analytics ready for Power BI consumption.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

print("=== EXECUTIVE SUMMARY ===")
print(f"Total Unique Patients: {df_fact.select(countDistinct('patient_id')).collect()[0][0]}")
print(f"Total Visits: {df_fact.count()}")
print(f"30-Day Readmissions: {df_fact.filter(col('is_readmitted_30d') == True).count()}")
readmission_rate = (df_fact.filter(col('is_readmitted_30d') == True).count() / df_fact.count()) * 100
print(f"Overall Readmission Rate: {readmission_rate:.2f}%")
print(f"Average Risk Score: {df_fact.select(avg('risk_score')).collect()[0][0]:.2f}")
print(f"Total Revenue: ${df_fact.select(_sum('total_charges')).collect()[0][0]:,.2f}")
print("\nGold Layer Pipeline COMPLETE - Ready for Power BI dashboards!")

# COMMAND ----------
