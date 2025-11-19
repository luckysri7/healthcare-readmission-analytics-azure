# Healthcare Patient Readmission Analytics on Azure

##  Project Overview

This project demonstrates an **end-to-end Azure Data Engineering solution** for analyzing 30-day patient readmissions in healthcare. Built using the **Medallion Architecture (Bronze/Silver/Gold)**, it showcases real-world data pipeline development, transformation, and analytics using Azure's modern data stack.

**Business Objective:** Help healthcare organizations identify high-risk patients, reduce readmission rates, and improve patient outcomes through data-driven insights.

---

##  Architecture

### Tech Stack
- **Azure Data Lake Gen2** - Scalable data storage with hierarchical namespaces
- **Azure Data Factory (ADF)** - Orchestration and data ingestion
- **Azure Databricks** - PySpark transformations and Delta Lake
- **Power BI / Synapse SQL Warehouse** - Analytics and visualization
- **Delta Lake** - ACID transactions and time travel

### Medallion Architecture Layers

```
 Bronze Layer (Raw Data)
   ├── ehr/              # Electronic Health Records
   ├── lab_results/      # Laboratory test results  
   └── claims/           # Insurance claims data

 Silver Layer (Cleaned & Standardized)
   ├── dim_patient/      # Patient dimension
   ├── dim_hospital/     # Hospital dimension
   ├── fact_visit/       # Visit facts
   ├── fact_lab_result/  # Lab result facts
   └── fact_claim/       # Claim facts

 Gold Layer (Analytics-Ready)
   ├── fact_readmission_risk/       # Readmission features
   └── mart_readmission_dashboard/  # Aggregated KPIs
```

---

##  Data Model

### Source Data (Bronze)
1. **EHR Visits** - Patient admission and discharge records
2. **Lab Results** - Clinical test results and flags
3. **Claims** - Billing and insurance information

### Key Metrics (Gold)
- **30-Day Readmission Rate** by hospital, department, diagnosis
- **Average Length of Stay** trends
- **Patient Risk Segmentation** by age, diagnosis, lab results
- **Cost Analysis** for readmitted vs non-readmitted patients

---

##  Pipeline Flow

### 1. Ingestion (Azure Data Factory)
```
Source Systems → ADF Copy Activity → Bronze Layer (ADLS Gen2)
```
- **PL_INGEST_EHR**: Daily ingestion of EHR data
- **PL_INGEST_LAB_RESULTS**: Lab results data pipeline
- **PL_INGEST_CLAIMS**: Claims data pipeline
- **PL_MASTER_DAILY_LOAD**: Master orchestration pipeline

### 2. Transformation (Azure Databricks)
```
Bronze → Silver → Gold (PySpark + Delta Lake)
```
**Bronze to Silver:**
- Data cleansing (nulls, duplicates, invalid records)
- Standardization (dates, codes, formats)
- Derived columns (length_of_stay, age_group)

**Silver to Gold:**
- Feature engineering for readmission prediction
- Window functions to identify 30-day readmissions
- Aggregations by hospital, diagnosis, demographics

### 3. Serving (Power BI / SQL Warehouse)
```
Gold Layer → SQL Endpoint → Power BI Dashboards
```

---

##  Project Structure

```
healthcare-readmission-analytics-azure/
│
├── README.md                          # This file
├── architecture/
│   └── architecture-diagram.png       # Solution architecture
│
├── data-samples/                      # Sample datasets
│   ├── ehr_visits.csv
│   ├── lab_results.csv
│   └── claims.csv
│
├── adf/                               # Azure Data Factory pipelines
│   ├── pipeline_ingest_ehr.json
│   ├── pipeline_ingest_lab.json
│   ├── pipeline_ingest_claims.json
│   └── pipeline_master_orchestration.json
│
├── databricks/
│   └── notebooks/                     # PySpark notebooks
│       ├── 01_bronze_to_silver_ehr.py
│       ├── 02_bronze_to_silver_lab.py
│       ├── 03_bronze_to_silver_claims.py
│       ├── 04_build_readmission_features.py
│       └── 05_create_gold_mart_readmission.py
│
├── sql-warehouse/                     # SQL scripts
│   ├── create_tables.sql
│   └── create_views_gold.sql
│
└── powerbi/
    ├── screenshots/                   # Dashboard previews
    └── dataset_model_description.md
```

---

## 🚀 How to Deploy

### Prerequisites
- Azure subscription
- Azure Data Lake Gen2 storage account
- Azure Data Factory instance
- Azure Databricks workspace
- Power BI Desktop (optional)

### Setup Steps

1. **Create Azure Resources**
   ```bash
   # Create resource group
   az group create --name rg-healthcare-analytics --location eastus
   
   # Create storage account with hierarchical namespace
   az storage account create --name sahealth --resource-group rg-healthcare-analytics --enable-hierarchical-namespace true
   ```

2. **Set Up ADLS Gen2 Containers**
   - Create containers: `bronze`, `silver`, `gold`
   - Create folder structure as shown in architecture

3. **Deploy ADF Pipelines**
   - Import pipeline JSON files from `/adf` folder
   - Update linked services with your storage account details
   - Configure triggers for daily execution

4. **Configure Databricks**
   - Import notebooks from `/databricks/notebooks`
   - Create cluster (Runtime 13.3 LTS recommended)
   - Mount ADLS Gen2 to Databricks
   - Update storage paths in notebooks

5. **Upload Sample Data**
   - Upload CSV files from `/data-samples` to Bronze layer
   - Or connect your own data sources

6. **Run Pipelines**
   - Execute `PL_MASTER_DAILY_LOAD` in ADF
   - Monitor pipeline runs in ADF Monitor tab

7. **Connect Power BI**
   - Connect to Gold layer via SQL endpoint
   - Import data model and create visualizations

---

##  Sample Business Insights

Based on the analytics platform, example insights include:

- **Cardiology department has 22% readmission rate** vs overall hospital average of 14%
- **Patients aged 65+** have 2.3x higher readmission probability
- **Diabetic patients with abnormal HbA1c** show 35% readmission rate
- **Weekend discharges** correlate with 18% higher readmission rates
- **Average cost of readmitted patients**: $12,500 vs $8,200 for single-visit patients

---

##  Key Features

✅ **Medallion Architecture** - Industry-standard Bronze/Silver/Gold layers  
✅ **Delta Lake** - ACID transactions, schema evolution, time travel  
✅ **Incremental Processing** - Efficient data refresh patterns  
✅ **Data Quality Checks** - Validation at each layer  
✅ **Scalable Design** - Handles growing data volumes  
✅ **End-to-End Orchestration** - Automated daily workflows  

---

##  Technologies & Skills Demonstrated

- **Cloud Platform**: Azure (ADLS Gen2, ADF, Databricks, Synapse)
- **Programming**: PySpark, Python, SQL
- **Data Engineering**: ETL/ELT, Data Modeling, Pipeline Orchestration
- **Big Data**: Spark, Delta Lake, Distributed Computing
- **Architecture**: Medallion/Lakehouse, Dimensional Modeling
- **DevOps**: CI/CD (optional), Git version control
- **Analytics**: Power BI, DAX, Data Visualization

---

##  License

This project is created for educational and portfolio purposes.

---

##  Author

**Lakshmi Srinivas**  
Data Engineer | 3+ Years Experience | Azure & Databricks Specialist

*Feel free to connect with me on [LinkedIn](www.linkedin.com/in/lakshmisrinivas7981838181) or check out my other projects!*

---

##  References

- [Azure Data Lake Gen2 Documentation](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Azure Data Factory Best Practices](https://docs.microsoft.com/azure/data-factory/)
- [Databricks Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
- [Healthcare Analytics Use Cases](https://www.healthcatalyst.com/)
