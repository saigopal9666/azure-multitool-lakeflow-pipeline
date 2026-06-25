# azure-multitool-lakeflow-pipeline
Enterprise Data Ingestion via ADF and Spark Declarative Lakeflow Pipeline
# 🚀 Enterprise Data Ingestion & Spark Declarative Lakeflow Pipeline
This repository contains the end-to-end production setup for a Multi-Tool Cloud Data Engineering Framework. The project automates data ingestion from the Kaggle API into Azure Data Lake Storage (ADLS Gen2) using Azure Data Factory, transforms it via Databricks Lakeflow, and enforces strict DevOps automation using GitHub Actions CI/CD.

---

## 🗺️ System Architecture (The Big Picture)
```text
[Kaggle API] 
     │ (Secure Ingestion via ADF HTTP Linked Service)
     ▼
[Azure Data Lake Gen2 (Bronze Container)] 
     │ (Incremental Loading via Databricks Auto-Loader)
     ▼
[Databricks Lakeflow Pipeline] ➔ Bronze Table ➔ Silver Table (Quality Check) ➔ Gold Table
     ▲
     │ (Automated Code Validation & Quality Gates)
[GitHub Actions CI/CD] 
```

---

## 🏭 Phase 1: Secure Data Ingestion (Azure Data Factory)
* **HTTP Linked Service**: Configured with strict production compliance using **Secure Input** and **Secure Output** to encrypt Kaggle API credentials in compliance logs.
* **Performance Tuning**: Enforced parallel copying by tuning **Data Integration Units (DIUs)** to 16 and configuration of dynamic parallelism to achieve sub-minute latency.
* **Dynamic Loading**: Parameterized target storage locations using runtime variables (`@dataset().FolderPath`).

---

## 🧱 Phase 2: Spark Declarative Lakeflow Transformation
The data pipeline implements a modern **Medallion Architecture (Bronze -> Silver -> Gold)** using Databricks Lakeflow API to handle incremental streaming states:

1. **Bronze Layer**: Automatically discovers and streams raw incoming CSVs from cloud storage using **Databricks Auto Loader** (`format("cloudFiles")`).
2. **Silver Layer**: Enforces data governance and schema enforcement using declarative expectations (`@dlt.expect_or_drop`). Records with null dates or negative values are automatically dropped.
3. **Gold Layer**: Computes optimized aggregation metrics (Daily Revenue, Order Counts, and Average Order Values) optimized for Power BI and corporate data warehouse reporting.

---

## 🔄 Phase 3: DevOps & CI/CD Automation (GitHub Actions)
Every code check-in automatically triggers a continuous integration workflow defined in `.github/workflows/deploy.yml`:
* **Syntax Enforcement**: Automatic static code analysis using `flake8` to block malformed Python syntax from merging into the master branch.
* **Secrets Security**: Cloud authorization parameters are securely managed inside encrypted **GitHub Repository Secrets** (`AZURE_STORAGE_KEY`).
