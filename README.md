# FMCG Data Engineering Project with Databricks

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks">
  <img src="https://img.shields.io/badge/AWS-S3-FF9900?style=for-the-badge&logo=amazon-s3&logoColor=white" alt="AWS S3">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
</p>

A production-ready data engineering pipeline for Fast-Moving Consumer Goods (FMCG) domain, demonstrating parent-subsidiary company data integration using Databricks Medallion Architecture (Bronze-Silver-Gold). This project showcases automated ETL workflows, incremental data processing, data quality management, and BI-ready analytics through Databricks Jobs orchestration.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Business Scenario](#business-scenario)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Data Models](#data-models)
- [Advanced Concepts](#advanced-concepts)
- [Troubleshooting](#troubleshooting)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## 🎯 Project Overview

### Business Scenario

This project simulates a **parent-subsidiary company data integration** scenario in the FMCG (Fast-Moving Consumer Goods) industry:

**Parent Company (AtliQ)**:
- Operates at a **monthly aggregation level**
- Provides **Gold layer** data (analytics-ready) to subsidiaries
- Maintains dimension tables: `dim_customers`, `dim_products`, `dim_gross_price`, `dim_date`
- Maintains fact table: `fact_orders`

**Child Company (Sports Bar)**:
- Operates at a **daily transaction level**
- Processes raw data from OLTP systems (simulated via CSV files in AWS S3)
- Implements complete **Bronze → Silver → Gold** pipeline
- Aggregates daily data to monthly level for parent company integration
- Merges (upserts) processed data into parent's Gold layer tables

**Data Engineering Challenges Solved**:
1. **Multi-level data granularity**: Daily (child) vs Monthly (parent)
2. **Incremental data processing**: Handling daily order updates efficiently
3. **Data quality**: Handling duplicates, missing values, inconsistent formats
4. **Orchestration**: Automated nightly pipeline execution via Databricks Jobs
5. **BI Analytics**: Creating denormalized One Big Table (OBT) for dashboard consumption

---

## 🏗 Architecture

### High-Level Data Flow
```
┌─────────────────────────────────────────────────────────────────────┐
│                     PARENT COMPANY (AtliQ)                          │
│                                                                     │
│  Gold Layer Only (Monthly Aggregation)                              │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ dim_customers  │  dim_products  │  dim_gross_price            │  │
│  │ dim_date       │  fact_orders (monthly)                        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            ▲                                        │
│                            │ UPSERT (merge daily → monthly)         │
└────────────────────────────┼─────────────────────────────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────────┐
│                     CHILD COMPANY (Sports Bar)                      │
│                            │                                        │
│  ┌─────────────────────────┼─────────────────────────────────────┐  │
│  │                   AWS S3 (OLTP Export)                         │  │
│  │   customers/  │  products/  │  gross_price/  │  orders/       │  │
│  │   *.csv       │  *.csv      │  *.csv          │  landing/*.csv │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                        │
│                            ▼                                        │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │             📦 BRONZE Layer (Raw Historical)                   │  │
│  │  Materialized: Delta Table (append mode)                       │  │
│  │  CDC: delta.enableChangeDataFeed = true                        │  │
│  │  - bronze.customers  │  bronze.products                        │  │
│  │  - bronze.gross_price │ bronze.orders                          │  │
│  │  - bronze.staging_orders (temporary for incremental)           │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                        │
│                            ▼ PySpark Transformations                │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │        🔧 SILVER Layer (Cleaned + Enriched)                    │  │
│  │  Materialized: Delta Table (MERGE upsert)                      │  │
│  │  Transformations:                                              │  │
│  │  ├─ Deduplication (dropDuplicates)                             │  │
│  │  ├─ Data quality (trim, initcap, fix typos)                    │  │
│  │  ├─ Missing value imputation (business rules)                  │  │
│  │  ├─ Date normalization (coalesce, try_to_date)                 │  │
│  │  ├─ Join enrichment (product_code lookup)                      │  │
│  │  └─ Business logic (variant extraction, division mapping)      │  │
│  │                                                                │  │
│  │  - silver.customers  │  silver.products                        │  │
│  │  - silver.gross_price │ silver.orders                          │  │
│  │  - silver.staging_orders (temporary for incremental)           │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                        │
│                            ▼ Aggregation & Alignment                │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │          ✨ GOLD Layer (Analytics-Ready)                       │  │
│  │  Materialized: Delta Table (MERGE upsert)                      │  │
│  │                                                                │  │
│  │  Dimension Tables:                                             │  │
│  │  - sb_dim_customers  │  sb_dim_products                        │  │
│  │  - sb_dim_gross_price                                          │  │
│  │                                                                │  │
│  │  Fact Table:                                                   │  │
│  │  - sb_fact_orders (daily grain)                                │  │
│  │                                                                │  │
│  │  Aggregation Logic (for parent merge):                         │  │
│  │  ├─ Daily → Monthly: trunc(date, 'MM')                         │  │
│  │  ├─ Incremental recalculation (existing months)                │  │
│  │  └─ Group by (month, product_code, customer_code)              │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                        │
│                            ▼ Denormalization                        │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │         📊 One Big Table (BI Layer)                            │  │
│  │  Materialized: VIEW (vw_fact_orders_enriched)                  │  │
│  │                                                                │  │
│  │  Joins:                                                        │  │
│  │  - fact_orders (parent)                                        │  │
│  │  - dim_customers, dim_products, dim_gross_price, dim_date      │  │
│  │                                                                │  │
│  │  Purpose: Self-service BI analytics (no manual JOINs needed)   │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                        │
│                            ▼                                        │
│                    Databricks Dashboards                            │
│                    (Sales Insights, KPIs)                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture Layers

| Layer | Purpose | Materialization | Key Characteristics |
|-------|---------|-----------------|---------------------|
| **Bronze** | Raw data ingestion | Delta Table (append) | - Preserves source data exactly<br>- Includes metadata (file_name, file_size, read_timestamp)<br>- CDC enabled for audit trail |
| **Silver** | Cleaned & enriched data | Delta Table (merge/upsert) | - Deduplication<br>- Data quality fixes<br>- Business rule application<br>- Staging tables for incremental processing |
| **Gold** | Analytics-ready models | Delta Table (merge/upsert) | - Aggregated to business grain<br>- Joins with reference tables<br>- Optimized for BI consumption<br>- Upserts to parent company tables |

---

## 🛠 Tech Stack

| Technology | Version | Purpose | Key Features Used |
|------------|---------|---------|-------------------|
| **Databricks** | Runtime 14.3+ | Unified analytics platform | Notebooks, Jobs, Workflows, Dashboards |
| **Apache Spark** | 3.5+ | Distributed data processing | PySpark, DataFrame API, SQL |
| **Delta Lake** | 3.0+ | ACID transactions on data lake | Time travel, CDC, MERGE operations |
| **AWS S3** | - | Cloud object storage | Landing zones, processed archives |
| **Python** | 3.10+ | Scripting language | PySpark transformations, UDFs |
| **Databricks Workflows** | - | Job orchestration | Scheduled pipelines, task dependencies |
| **Databricks Dashboards** | - | BI visualization | Lakeview dashboards (JSON config) |

---

## ✨ Key Features

### 1. Secure S3-Databricks Integration

**Challenge**: Access CSV files stored in AWS S3 from Databricks securely.

**Solution**: External Location with credential management
```python
# Read from S3 with metadata enrichment
df = (
    spark.read.format('csv')
    .option("header", True)
    .option("inferSchema", True)
    .load("s3://sports-db-side-project/customers/*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)
```

**Setup in Databricks**:
1. Navigate to **Catalog → Connect → External Locations**
2. Provide S3 bucket name and Access Token
3. Test connection before proceeding

**Benefits**:
- ✅ Centralized credential management
- ✅ Audit logging via Databricks
- ✅ File-level metadata tracking

---

### 2. Incremental Data Processing

**Challenge**: Avoid reprocessing entire datasets daily when only 1-2% is new data.

**Solution**: Staging tables + Delta MERGE for efficient incremental loads

**Bronze Layer (Append Only)**:
```python
# Append new files to bronze
df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable(bronze_table)

# Move processed files to archive
dbutils.fs.mv(landing_path, processed_path, True)
```

**Silver Layer (Upsert)**:
```python
# Create staging table for new data only
df_transformed.write \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.silver.staging_orders")

# Merge staging into silver (upsert logic)
silver_delta = DeltaTable.forName(spark, silver_table)
silver_delta.alias("target").merge(
    df_transformed.alias("source"),
    "target.order_id = source.order_id AND target.date = source.date"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Gold Layer (Monthly Aggregation)**:
```python
# Recalculate only affected months
incremental_months = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

# Aggregate daily → monthly
df_monthly = (
    existing_monthly_data
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
)

# Merge into parent company fact table
parent_delta.merge(df_monthly, merge_condition) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

**Performance Impact**:
- Daily orders: ~1,000 rows → Process in 5 seconds
- Full reload: ~500,000 rows → Would take 5 minutes
- **97% time saved** with incremental approach

---

### 3. Data Quality Management

#### Deduplication
```python
# Remove duplicates based on business key
df_silver = df_bronze.dropDuplicates(['customer_id'])
```

#### Missing Value Imputation (Business Rules)
```python
# Business confirmation: Fill missing cities based on customer name patterns
customer_city_fix = {
    789403: "New Delhi",    # Sprintx Nutrition
    789420: "Bengaluru",    # Zenathlete Foods
    789521: "Hyderabad",    # Primefuel Nutrition
    789603: "Hyderabad"     # Recovery Lane
}

df_fix = spark.createDataFrame(customer_city_fix.items(), ["customer_id", "fixed_city"])
df_silver = df_silver.join(df_fix, "customer_id", "left") \
    .withColumn("city", F.coalesce("city", "fixed_city"))
```

#### Data Standardization
```python
# 1. Trim whitespace
df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name")))

# 2. Title case
df_silver = df_silver.withColumn("customer_name", F.initcap("customer_name"))

# 3. Fix typos via mapping
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Hyderabadd': 'Hyderabad',
    'NewDelhi': 'New Delhi'
}
df_silver = df_silver.replace(city_mapping, subset=["city"])

# 4. Spelling correction (regex replace)
df_silver = df_silver.withColumn(
    "product_name",
    F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
)
```

#### Date Normalization
```python
# Handle multiple date formats with coalesce
df_silver = df_silver.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy")
    )
)
```

---

### 4. Parent-Child Data Alignment

**Challenge**: Child company operates at **daily grain**, parent at **monthly grain**.

**Solution**: Intelligent aggregation with incremental recalculation

**Step 1: Identify affected months**
```python
# Extract months from new data
incremental_months = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()
```

**Step 2: Retrieve existing monthly records for those months**
```python
# Pull all daily records for affected months
monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {gold_table}
    INNER JOIN incremental_months
        ON trunc(date, 'MM') = start_month
""")
```

**Step 3: Re-aggregate entire month**
```python
# Group by month + dimensions
df_monthly = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)
```

**Step 4: Upsert to parent's fact table**
```python
parent_delta = DeltaTable.forName(spark, "fmcg.gold.fact_orders")
parent_delta.merge(df_monthly, join_condition) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

**Why this approach?**
- ✅ Handles late-arriving data (e.g., order placed on Dec 31, received Jan 2)
- ✅ Recalculates correct monthly totals
- ✅ Avoids double-counting

---

### 5. Databricks Jobs Orchestration

**Challenge**: Manual notebook execution is error-prone and doesn't scale.

**Solution**: Databricks Jobs with task dependencies and scheduling

**Job Configuration**:
```
dim_processing_customers
    ↓ (depends on)
dim_processing_products
    ↓ (depends on)
dim_processing_prices
    ↓ (depends on)
incremental_load_fact
```

**Key Settings**:
- **Notebook Path**: `/Workspace/.../1_customer_data_processing`
- **Parameters**: `catalog=fmcg, data_source=customers`
- **Cluster**: Shared (cost-effective for non-production)
- **Schedule**: Cron: `0 23 * * *` (11 PM daily, after business closes)
- **Trigger Type**: Scheduled (time-based) or File Arrival (event-based)

**Parameter Passing via dbutils.widgets**:
```python
# In notebook
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
```

**Benefits**:
- ✅ No manual intervention
- ✅ Guaranteed execution order
- ✅ Email alerts on failure
- ✅ Execution history tracking

---

### 6. One Big Table (OBT) for BI

**Challenge**: Business analysts need to write complex JOINs to answer questions.

**Solution**: Create denormalized VIEW joining all dimensions + facts
```sql
CREATE OR REPLACE VIEW fmcg.gold.vw_fact_orders_enriched AS (
    SELECT 
        fo.date,
        -- Date attributes
        dd.year, dd.quarter, dd.month_name,
        
        -- Customer attributes
        dc.customer, dc.market, dc.platform, dc.channel,
        
        -- Product attributes
        dp.division, dp.category, dp.product, dp.variant,
        
        -- Metrics
        fo.sold_quantity,
        gp.price_inr,
        (fo.sold_quantity * gp.price_inr) AS total_amount_inr
    
    FROM fmcg.gold.fact_orders fo
    LEFT JOIN fmcg.gold.dim_date dd ON fo.date = dd.month_start_date
    LEFT JOIN fmcg.gold.dim_customers dc ON fo.customer_code = dc.customer_code
    LEFT JOIN fmcg.gold.dim_products dp ON fo.product_code = dp.product_code
    LEFT JOIN fmcg.gold.dim_gross_price gp 
        ON fo.product_code = gp.product_code 
        AND YEAR(fo.date) = gp.year
);
```

**Why VIEW instead of TABLE?**
- ✅ Always shows latest data (no stale reports)
- ✅ No storage overhead
- ✅ Single source of truth for dashboards

**Dashboard Consumption**:
```sql
-- Analysts can now query easily
SELECT 
    year,
    channel,
    SUM(total_amount_inr) AS revenue
FROM vw_fact_orders_enriched
WHERE year = 2024
GROUP BY year, channel;
```

---

### 7. Delta Lake Advanced Features

#### Change Data Capture (CDC)
```python
# Enable CDC on all tables
.option("delta.enableChangeDataFeed", "true")

# Query historical changes
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 5) \
    .table("fmcg.silver.orders")
```

#### Time Travel
```python
# Query data as of yesterday
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-30") \
    .table("fmcg.gold.fact_orders")
```

#### Schema Evolution
```python
# Auto-merge schema changes
.option("mergeSchema", "true")
```

---

## 📁 Project Structure
```
adamxiang-fmcg-domain-data-engineering-project-with-databricks/
├── README.md
├── LICENSE
├── AtliQon BI 360 insights.lvdash.json    # Databricks dashboard config
│
└── Consolidated_Pipeline/
    │
    ├── 1_setup/
    │   ├── setup_catalog.ipynb            # Create catalog & schemas
    │   ├── utilities.ipynb                # Reusable config (bronze/silver/gold schema names)
    │   └── dim_date_table_creation.ipynb  # Generate date dimension
    │
    ├── 2_dimension_data_processing/
    │   ├── 1_customer_data_processing.ipynb
    │   │   ├─ Bronze: Raw CSV → Delta (append)
    │   │   ├─ Silver: Dedup, trim, fix typos, impute missing cities
    │   │   ├─ Gold: Select relevant columns
    │   │   └─ Merge: Upsert to parent's dim_customers
    │   │
    │   ├── 2_products_data_processing.ipynb
    │   │   ├─ Bronze: Raw CSV → Delta
    │   │   ├─ Silver: Fix spelling (Protien→Protein), add division/variant
    │   │   ├─ Gold: Generate product_code (SHA2 hash)
    │   │   └─ Merge: Upsert to parent's dim_products
    │   │
    │   └── 3_pricing_data_processing.ipynb
    │       ├─ Bronze: Raw CSV → Delta
    │       ├─ Silver: Normalize date formats, fix negative prices, join product_code
    │       ├─ Gold: Latest price per year (window function)
    │       └─ Merge: Upsert to parent's dim_gross_price
    │
    ├── 3_fact_data_processing/
    │   ├── 1_full_load_fact.ipynb
    │   │   ├─ Bronze: Load from landing/ → append
    │   │   ├─ Silver: Clean dates, deduplicate, join product_code
    │   │   ├─ Gold: Daily grain fact table
    │   │   ├─ Aggregate: Daily → Monthly
    │   │   ├─ Merge: Upsert to parent's fact_orders
    │   │   └─ File Management: mv landing/ → processed/
    │   │
    │   └── 2_incremental_load_fact.ipynb
    │       ├─ Bronze: Append new files + create staging table
    │       ├─ Silver: Transform staging → merge into silver table
    │       ├─ Gold: Transform staging → merge into gold table
    │       ├─ Incremental Aggregation: Recalculate affected months only
    │       ├─ Merge: Upsert recalculated monthly data to parent
    │       └─ Cleanup: Drop staging tables
    │
    ├── 4_simulate_parent_incremental_load/
    │   └── incremental_data_parent_company_query.dbquery.ipynb
    │       └─ COPY INTO: Load new monthly data into parent's fact_orders
    │
    └── 5_one_big_table/
        └── denormalise_table_query_fmcg.dbquery.ipynb
            └─ CREATE VIEW: Join all dimensions + facts for BI consumption
```

---

## 📦 Prerequisites

### Required Tools

- **Databricks Workspace** (AWS, Azure, or GCP)
- **AWS Account** with S3 access
- **Python 3.10+** (for local testing, optional)
- **Git** (for cloning repository)

### Databricks Setup Requirements

You'll need:
- **Catalog**: `fmcg`
- **Schemas**: `bronze`, `silver`, `gold`
- **Cluster**: Runtime 14.3+ LTS with Spark 3.5
- **Permissions**: 
  - CREATE CATALOG
  - CREATE SCHEMA
  - CREATE TABLE
  - EXECUTE (for workflows)

### AWS Setup Requirements

- S3 bucket structure:
```
  s3://sports-db-side-project/
  ├── customers/*.csv
  ├── products/*.csv
  ├── gross_price/*.csv
  └── orders/
      ├── landing/*.csv        # New data arrives here
      └── processed/*.csv      # Archived after processing
```
- IAM permissions: `s3:GetObject`, `s3:ListBucket`, `s3:PutObject`

---

## 🚀 Installation & Setup

### Step 1: Clone Repository
```bash
git clone https://github.com/yourusername/fmcg-databricks-pipeline.git
cd fmcg-databricks-pipeline
```

### Step 2: Upload to Databricks Workspace

1. Open Databricks Workspace
2. Navigate to **Workspace → Users → [your-email]**
3. Create folder: `Consolidated_Pipeline`
4. Upload all notebooks from repo to respective folders

**Alternative (using Databricks CLI)**:
```bash
databricks workspace import_dir ./Consolidated_Pipeline /Workspace/Users/your-email@company.com/Consolidated_Pipeline
```

### Step 3: Configure AWS S3 Connection

1. Navigate to **Catalog → External Locations → Add**
2. **Name**: `sports-db-external`
3. **URL**: `s3://sports-db-side-project/`
4. **Credential**: Create new (provide AWS Access Key ID & Secret)
5. Test connection

### Step 4: Create Catalog and Schemas

Run notebook: `1_setup/setup_catalog.ipynb`
```sql
-- This creates:
CREATE CATALOG IF NOT EXISTS fmcg;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
```

### Step 5: Generate Date Dimension

Run notebook: `1_setup/dim_date_table_creation.ipynb`

This creates `fmcg.gold.dim_date` with:
- Year, Quarter, Month attributes
- Date range: 2024-01-01 to 2025-12-01

### Step 6: Prepare Parent Company Data

**Option A**: Import pre-existing tables (if provided)

**Option B**: Manually create empty tables for first run
```sql
CREATE TABLE fmcg.gold.dim_customers (
    customer_code STRING,
    customer STRING,
    market STRING,
    platform STRING,
    channel STRING
);

CREATE TABLE fmcg.gold.dim_products (
    product_code STRING,
    division STRING,
    category STRING,
    product STRING,
    variant STRING
);

CREATE TABLE fmcg.gold.dim_gross_price (
    product_code STRING,
    price_inr DOUBLE,
    year STRING
);

CREATE TABLE fmcg.gold.fact_orders (
    date DATE,
    product_code STRING,
    customer_code STRING,
    sold_quantity BIGINT
);
```

---

## 📖 Usage

### Full Load (Initial Run)

Execute notebooks in this order:
```bash
# 1. Dimension Processing
2_dimension_data_processing/1_customer_data_processing.ipynb
2_dimension_data_processing/2_products_data_processing.ipynb
2_dimension_data_processing/3_pricing_data_processing.ipynb

# 2. Fact Processing (Full Load)
3_fact_data_processing/1_full_load_fact.ipynb
```

**Parameters for each notebook**:
- `catalog`: `fmcg`
- `data_source`: `customers` / `products` / `gross_price` / `orders`

### Incremental Load (Daily Updates)

**Manual Execution**:
```bash
# Place new CSV files in S3
aws s3 cp new_orders.csv s3://sports-db-side-project/orders/landing/

# Run incremental notebook
3_fact_data_processing/2_incremental_load_fact.ipynb
```

**Automated Execution via Databricks Jobs**:

1. **Navigate to**: Workflows → Jobs → Create Job
2. **Job Name**: `Daily_FMCG_Pipeline`
3. **Add Tasks**:

| Task Name | Notebook Path | Parameters | Depends On |
|-----------|---------------|------------|------------|
| `dim_processing_customers` | `2_dimension.../1_customer...` | `catalog=fmcg, data_source=customers` | - |
| `dim_processing_products` | `2_dimension.../2_products...` | `catalog=fmcg, data_source=products` | `dim_processing_customers` |
| `dim_processing_prices` | `2_dimension.../3_pricing...` | `catalog=fmcg, data_source=gross_price` | `dim_processing_products` |
| `fact_incremental_load` | `3_fact.../2_incremental...` | `catalog=fmcg, data_source=orders` | `dim_processing_prices` |

4. **Set Schedule**: 
   - Trigger Type: `Scheduled`
   - Cron Expression: `0 23 * * *` (11 PM daily)

5. **Run Now** (for testing) or **Enable** schedule

### Query One Big Table
```sql
-- Connect to BI tool or query in Databricks SQL
SELECT * FROM fmcg.gold.vw_fact_orders_enriched
WHERE year = 2024 AND channel = 'Acquisition'
LIMIT 100;
```

### Create Dashboard

1. **SQL Warehouse**: Create/attach to existing warehouse
2. **Import Dashboard**: 
   - Upload `AtliQon BI 360 insights.lvdash.json`
3. **Connect Dataset**: `vw_fact_orders_enriched`
4. **Add Visualizations**:
   - Counter: Total Revenue, Quantity Sold
   - Bar Chart: Top 10 Products
   - Pie Chart: Revenue by Channel
   - Line Chart: Monthly Sales Trend
  
---

## 📊 Data Models

The pipeline follows a multi-hop architecture to progressively improve data quality.

### Bronze Layer Tables
*Raw data snapshots ingested directly from source files.*

| Table | Grain | Key Columns | Purpose |
| :--- | :--- | :--- | :--- |
| `bronze.customers` | 1 row per customer | `customer_id`, `customer_name`, `city`, `read_timestamp` | Raw customer data snapshot |
| `bronze.products` | 1 row per product | `product_id`, `product_name`, `category`, `read_timestamp` | Raw product catalog |
| `bronze.gross_price` | 1 row per product-month | `product_id`, `month`, `gross_price` | Raw pricing data |
| `bronze.orders` | 1 row per order line | `order_id`, `order_date`, `customer_id`, `product_id` | Raw order transactions (Append-only) |

### Silver Layer Tables
*Cleaned, standardized, and enriched data with Change Data Capture (CDC).*

| Table | Grain | Key Transformations | CDC Enabled |
| :--- | :--- | :--- | :---: |
| `silver.customers` | 1 row per customer | Deduplicated, trimmed names, fixed typos, added market info | ✅ |
| `silver.products` | 1 row per product | Fixed spelling (Protien→Protein), added division, generated `product_code` | ✅ |
| `silver.gross_price` | 1 row per product-month | Normalized dates, handled negative prices, joined `product_code` | ✅ |
| `silver.orders` | 1 row per order line | Cleaned `customer_id`, normalized dates, joined `product_code` | ✅ |

### Gold Layer Tables
*Business-level aggregates and dimensional models for analytics.*

| Table | Grain | Source & Merge Logic |
| :--- | :--- | :--- |
| `sb_dim_customers` | 1 row per customer | From `silver.customers`. Match on `customer_code`. |
| `sb_dim_products` | 1 row per product | From `silver.products`. Match on `product_code`. |
| `sb_dim_gross_price` | 1 row per product-year | Aggregated from `silver.gross_price`. Match on `product_code` + `year`. |
| `sb_fact_orders` | 1 row per order line (daily) | From `silver.orders`. Match on `date` + `order_id` + keys. |

### Parent Company Tables (Upsert Targets)
*Centralized tables where child subsidiary data is merged.*

| Table | Grain | Child Contribution |
| :--- | :--- | :--- |
| `dim_customers` | 1 row per customer | Child customers appended/updated via Upsert |
| `dim_products` | 1 row per product | Child products appended/updated |
| `dim_gross_price` | 1 row per product-year | Child prices appended/updated (Takes latest price per year) |
| `fact_orders` | 1 row per month/customer/product | Child **daily** data is aggregated to **monthly** before upsert |

---

## 🎓 Advanced Concepts

### 1. DeltaTable vs spark.table()

* **`DeltaTable.forName()`**: Returns a `DeltaTable` object. Used for DML operations (MERGE, UPDATE, DELETE).
* **`spark.table()`**: Returns a `DataFrame`. Used for read-only operations (SELECT, JOIN).

```python
# MERGE (Requires DeltaTable)
delta_table = DeltaTable.forName(spark, "fmcg.silver.orders")
delta_table.merge(new_data, condition) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# READ (Standard DataFrame)
df = spark.table("fmcg.silver.orders")
df.filter(F.col("date") > "2024-01-01").groupBy("product_code").count().show()
```

Key Difference:

- `DeltaTable` returns a DeltaTable object → Enables MERGE
- `spark.table()` returns a DataFrame → Read-only operations

---
### 2. Window Functions for Latest Price
**Challenge**: Multiple prices exist for same product in different months. Need latest non-zero price per year.
```python
from pyspark.sql.window import Window

# Rank prices: non-zero first, then most recent date
w = Window.partitionBy("product_code", "year") \
          .orderBy(
              F.when(F.col("gross_price") == 0, 1).otherwise(0),  # 0=non-zero, 1=zero
              F.col("month").desc()
          )

df_latest_price = df.withColumn("rank", F.row_number().over(w)) \
                    .filter(F.col("rank") == 1) \
                    .drop("rank")
```
---
### 3. Incremental Month Recalculation Logic
**Problem**: New orders on Jan 31 affect January's monthly total.
**Solution**: Re-aggregate entire month when new data arrives
```python
# Step 1: Extract affected months
incremental_months = df_new.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

# Step 2: Fetch all existing daily records for those months
monthly_data = spark.sql(f"""
    SELECT * FROM {gold_table}
    INNER JOIN incremental_months
        ON trunc(date, 'MM') = start_month
""")

# Step 3: Re-aggregate
df_monthly = monthly_data.groupBy(
    F.trunc("date", "MM").alias("month_start"),
    "product_code",
    "customer_code"
).agg(F.sum("sold_quantity").alias("sold_quantity"))
```

Why this works:
- Ensures accurate monthly totals
- Handles late-arriving data
- Prevents double-counting

---

### 4. PySpark Functions Deep Dive
`lit()` - Create constant column
```python
df.withColumn("market", F.lit("India"))
```

```coalesce()``` - First non-null value
```python
df.withColumn("city", F.coalesce("city", "fixed_city", F.lit("Unknown")))
```

```try_to_date()``` - Safe date parsing
```python
# Returns NULL instead of error if format doesn't match
F.try_to_date("date_column", "yyyy-MM-dd")
```

```trunc()``` - Date truncation
```python
# 2024-01-15 → 2024-01-01
F.trunc("date", "MM")
```

```regexp_replace()``` - Pattern matching
```python
# Remove weekday prefix: "Monday, Jan 01, 2024" → "Jan 01, 2024"
F.regexp_replace("date_text", r"^[A-Za-z]+,\s*", "")
```
---

### 5. File Movement Pattern
**Challenge**: Prevent reprocessing same files.
**Solution**: Archive processed files
```python
landing_path = "s3://bucket/orders/landing/"
processed_path = "s3://bucket/orders/processed/"

# After successful processing
files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path, f"{processed_path}/{file_info.name}", True)
```

Benefits:

- ✅ Idempotent pipeline (safe to re-run)
- ✅ Audit trail of processed files
- ✅ Easy rollback (mv back to landing)

---

### 6. Schema Evolution with mergeSchema
**Scenario**: Source adds new column `discount_amount` to orders CSV.
```python
# Without mergeSchema → Error: "Column 'discount_amount' not found"
# With mergeSchema → Column automatically added with NULL for existing rows

df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("fmcg.bronze.orders")


---

## 🐛 Troubleshooting

### Common Issues and Solutions

#### 1. S3 Access Denied

**Error**:

AccessDenied: User is not authorized to perform: s3:GetObject
```

**Solution**:
1. Check IAM policy attached to credentials
2. Verify bucket name and path are correct
3. Test connection in External Locations

**Required IAM Policy**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::sports-db-side-project",
        "arn:aws:s3:::sports-db-side-project/*"
      ]
    }
  ]
}

---

#### 2. MERGE Conflict Error

**Error**:

ConcurrentAppendException: Files were added to partition during commit
```


**Cause**: Multiple jobs writing to same table simultaneously.
**Solution**:
```python
# Add retry logic
from delta.tables import DeltaTable
import time

max_retries = 3
for attempt in range(max_retries):
    try:
        delta_table.merge(...).execute()
        break
    except Exception as e:
        if "ConcurrentAppend" in str(e) and attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
        else:
            raise

---

#### 3. Date Parsing Failure

**Error**:
Cannot parse 'Tuesday, July 01, 2025' with format 'yyyy-MM-dd'
```

**Solution**: Use `coalesce()` with multiple formats
```python
df.withColumn(
    "date",
    F.coalesce(
        F.try_to_date("date_str", "yyyy-MM-dd"),
        F.try_to_date("date_str", "MMMM dd, yyyy"),
        F.try_to_date("date_str", "dd/MM/yyyy")
    )
)

---

#### 4. Job Fails with "Widget Not Found"

**Error**:

WidgetNotFoundException: Widget 'catalog' not found


**Cause**: Job didn't pass parameters to notebook.

**Solution**: In Job configuration, add Parameters:

catalog = fmcg
data_source = customers
```

---

### 5. View Shows Stale Data
**Problem**: OBT view doesn't reflect latest fact table updates.
**Cause**: View caching or not using `CURRENT` keyword.
**Solution**:
```sql
-- Force view refresh
REFRESH TABLE fmcg.gold.vw_fact_orders_enriched;

-- Or recreate view
CREATE OR REPLACE VIEW fmcg.gold.vw_fact_orders_enriched AS (...)
```

---

## 📝 License
This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Databricks for the unified analytics platform
- Delta Lake for ACID transactions on data lakes
- Apache Spark for distributed data processing
- Data Engineering community for best practices
- Tutorial Provider - [Codebasics](https://www.youtube.com/watch?v=U6ZUKWdfSLY)

---

If this project helped you, please give it a ⭐️ on GitHub!
