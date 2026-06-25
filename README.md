# 🚀 Enterprise Multi-Tool Cloud Data Engineering Framework & Serverless Lakeflow Pipeline

An production-grade, end-to-end cloud data platform implementing an automated data architecture. This framework orchestrates secure data ingestion from the Kaggle API using Azure Data Factory, executes programmatic data cleansing and silver-gold multi-layer formatting via Databricks Serverless Lakeflow Pipelines, and enforces strict DevOps automation via GitHub Actions CI/CD workflows with automated linting gates.

---

## 🏛️ Comprehensive Architecture & Data Flow

```text
+-------------------+       Secure Ingestion Pipeline        +--------------------------------+

|    Kaggle API     | ══════════════════════════════════════> | Azure Data Lake Gen2 Storage   |
| (E-Commerce Data) |   ADF HTTP Source Linked Service       | (bronze1 Container/Raw Zone)   |
+-------------------+   - Secure Input/Output Enabled        +--------------------------------+
                        - Performance Tuned (16 DIUs)                        │
                                                                             │ Streaming Auto-Loader
                                                                             ▼
+-----------------------------------------------------------------------------------------------------+

|                                   Databricks Serverless Lakeflow                                    |
|                                                                                                     |
|  [Bronze Table] (Raw View) ➔➔ [Silver Table] (Quality Governance) ➔➔ [Gold Table] (BI Metrics Agg)  |
|                                 - @dlt.expect_or_drop               - sum(), count(), avg()         |
+-----------------------------------------------------------------------------------------------------+
                                                                             ▲
                                                                             │ Enforces Syntax & Linters
                                                                             │ Automated Trigger on Push
                                                             +-------------------------------+

                                                             |   GitHub Actions CI/CD Engine |
                                                             |   - Secrets: AZURE_STORAGE_KEY |
                                                             |   - Quality Check via Flake8  |
                                                             +-------------------------------+
```

---

## 🛠️ In-Depth Project Phases & Technical Deep-Dive

### 🏭 Phase 1: High-Performance Secure Data Ingestion (Azure Data Factory)
* **Production Credentials Isolation**: Implemented native token isolation via an **HTTP Source Linked Service** integrated with `Basic Authentication` to dynamically bind Kaggle API security keys.
* **Log Obfuscation & Security Compliance**: Enforced strict enterprise compliance by enabling **`Secure Input`** and **`Secure Output`** parameters on the ingestion activity. This forcefully masks all runtime payloads, environment keys, and access tokens from being written to plain-text cloud execution logs.
* **Throughput Optimization & Performance Tuning**: 
  * Overrode default low-bandwidth computation by scaling up **Data Integration Units (DIUs)** to `16`.
  * Configured **Degree of Copy Parallelism** to `8` to split streaming chunks and optimize parallel extraction pipelines.
  * Successfully ingested compressed multi-megabyte payloads (`allunia/e-commerce-sales-forecast/archive.zip`) straight into the immutable cloud landing zone (`abfss://bronze1@fabricmayurrslake.dfs.core.windows.net/`) under sub-minute execution thresholds.

### 🧱 Phase 2: Programmatic Medallion Architecture (Databricks Serverless Lakeflow)
Transitioned the core computation block from archaic static notebook loops into **Lakeflow Spark Declarative Pipelines (SDP)** driven by serverless infrastructure compute to build an automated Medallion layout:

1. **Bronze Layer (`bronz_ecommerce_sales_v4`)**: Leveraged **Databricks Auto Loader** (`format("cloudFiles")`) to spin up cloud directory notification state tracking. This ensures incremental processing, preventing reprocessing of older files when newer mutations hit the data lake.
2. **Silver Layer (`silver_ecommerce_sales_v4`)**: Established explicit data governance metrics at the architecture level using programmatic constraints (**`Expectations`**). Implemented `@dlt.expect_or_drop("valid_date", "date IS NOT NULL")` and `@dlt.expect_or_drop("positive_sales", "sales >= 0")` to automatically catch, isolate, and drop damaged data rows mid-stream without writing expensive custom procedural code.
3. **Gold Layer (`gold_ecommerce_bi_metrics_v4`)**: Downscaled from streaming states to high-speed batch aggregation tables (`dlt.read`) to avoid telemetry overhead during grouping matrix calculations. Outputs real-time executive reporting layers computing metrics like `total_daily_sales` and `total_daily_orders`.

### 🔄 Phase 3: Automated Quality Infrastructure & DevOps (GitHub Actions)
Constructed an automated regression testing and build deployment loop using GitHub workflows (`.github/workflows/deploy.yml`):
* **Automated Linting Gates**: Enforced static code evaluation workflows running on isolated `ubuntu-latest` nodes. Integrated the **Flake8 Linter** to intercept push events targeting the `main` branch, performing automated code syntax compilation validation to instantly block malformed code scripts from entering deployment stages.
* **Cloud Credential Encapsulation**: Avoided insecure plaintext hardcoding by encapsulating sensitive Azure environment keys inside asymmetric **GitHub Repository Secrets** (`AZURE_STORAGE_KEY`).
* **Environment Isolation & Gateways**: Initialized environment-aware pipeline states mapping down to a isolated `production` construct to track cross-stage codebase velocity.

---

## 🔬 Real-World Production Debugging Diary (Root Cause Analysis)

A core highlight of this project was engineering solutions around complex distributed systems and permission bottlenecks encountered during development:

1. **The Cloud RBAC Propagation Delay**: 
   * *Symptom*: Encountered `The operation is not allowed by RBAC` inside Azure Key Vault despite holding explicit Owner privileges.
   * *Root Cause*: Asynchronous IAM role assignment caching across global active directory tenants.
   * *Resolution*: Pivoted the Key Vault configuration matrix from modern Azure RBAC into local **`Vault Access Policies`** to instantly establish explicit secret management credentials without waiting for AD token synchronization delays.
2. **The Kaggle API 404 URL Matrix Break**:
   * *Symptom*: Copy activities repeatedly thrown out with `HttpRequestFailedWithClientError (404 NotFound)`.
   * *Root Cause*: Missed API gateway path signatures. Standard web paths point to `/datasets/`, whereas programmatic backend runtimes explicitly require routing targets through the hidden `/api/v1/datasets/download/` endpoint root.
   * *Resolution*: Reconfigured the Base URL inside the ADF Linked Service to include the full API prefix, while isolating the pure tenant dataset path inside the relative data property box.
3. **The Unity Catalog Missing Credential Scope Block**:
   * *Symptom*: Lakeflow pipelines throwing `MissingCredentialScopeException [UNITY_CREDENTIAL_SCOPE_MISSING_SCOPE]`.
   * *Root Cause*: Databricks serverless compute engines enforcing Unity Catalog governance guidelines, which prohibit unauthenticated `abfss://` raw string lookups without a cluster storage credential scope setup.
   * *Resolution*: Successfully debugged infrastructure boundaries by leveraging local **Databricks Secret Scopes CLI** configurations (`databricks secrets create-scope`) and wrapping credentials securely into a programmatic context (`dbutils.secrets.get(scope="azure-secrets", key="storage-key")`) inside line 16 of the Lakeflow script.
