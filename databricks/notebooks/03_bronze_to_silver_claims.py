# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Claims Processing
# MAGIC Transform insurance claims from Bronze to Silver

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_path = "abfss://bronze@<storage_account>.dfs.core.windows.net/claims/"
silver_path = "abfss://silver@<storage_account>.dfs.core.windows.net/fact_claim/"

# COMMAND ----------

df_bronze = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(bronze_path)
print(f"Bronze claims count: {df_bronze.count()}")

# COMMAND ----------

df_silver = df_bronze \
    .dropDuplicates(["claim_id"]) \
    .withColumn("processing_date", F.to_date(F.col("processing_date"))) \
    .withColumn("insurance_type_clean",
                F.when(F.upper(F.col("insurance_type")).contains("MEDICARE"), "Medicare")
                 .when(F.upper(F.col("insurance_type")).contains("MEDICAID"), "Medicaid")
                 .otherwise("Private")) \
    .withColumn("payment_ratio",
                F.round(F.col("total_paid_amount") / F.col("total_billed_amount"), 2)) \
    .withColumn("patient_share_pct",
                F.round((F.col("patient_responsibility") / F.col("total_billed_amount")) * 100, 1)) \
    .withColumn("processing_timestamp", F.current_timestamp())

print(f"Silver claims: {df_silver.count()}")

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").save(silver_path)
print("Claims transformation complete!")
