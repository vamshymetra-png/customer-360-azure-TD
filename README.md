# Customer 360 Azure Banking Data Pipeline

## Overview

This repository demonstrates an Azure-ready banking data engineering pipeline for Customer 360, credit card transaction analytics, product holding analysis, branch performance, and fraud/risk analytics.

The project is built to reflect an Azure Data Engineer delivery model from the 2018-2022 period. It uses Azure Data Factory, ADLS Gen2, Azure Databricks, Spark, Azure Synapse, Azure Key Vault, Log Analytics, ARM templates, Jenkins-style CI/CD, Power BI, and SSRS design patterns.

The repository supports two execution modes:

1. Local pandas notebook analysis for quick review and output generation.
2. Local PySpark pipeline execution that follows the same Bronze, Silver, Gold design used in Azure Databricks.

No real bank or TD customer data is included. All data is synthetic and safe for a public portfolio repository.

---

## Business Objective

The objective is to build a Customer 360 platform that consolidates customer, account, credit card, loan, transaction, branch, and risk data into curated analytics marts.

Business users should be able to answer:

- Which customers are high value or high risk?
- What products does each customer hold?
- Which transactions are high risk?
- How do rule-based and ML fraud scores compare?
- Which branches have higher deposits, loans, and risk exposure?
- What metrics should be exposed to Power BI and SSRS?

---

## Architecture

```text
Sources
Azure SQL DB, On-Prem MySQL, Cassandra, Blob/API feeds
        |
        v
Azure Data Factory
Copy activities, Self-hosted IR, triggers, orchestration, Databricks invocation
        |
        v
ADLS Gen2 Landing
Raw, Bronze, Silver, Gold, Audit zones
        |
        v
Databricks Bronze
Schema enforcement, data validation, audit columns, raw standardization
        |
        v
Databricks Silver
DQ checks, customer enrichment, SCD-1 contact updates, SCD-2 risk history,
mini-batch transaction processing, streaming-style aggregations
        |
        v
Databricks Gold
Customer 360, fraud risk scoring, fraud ML prediction,
product holding summary, branch performance metrics
        |
        v
Business Data Marts
Customer 360 Mart, Fraud Analytics Mart, Product Holding Mart,
Branch Performance Mart
        |
        v
Azure Synapse
External tables, dedicated SQL pool, columnstore tables, serving layer
        |
        v
Power BI / SSRS
Customer 360 dashboard, fraud analytics dashboard,
product analytics, branch performance, executive KPI reporting
```

Side components:

```text
Azure Key Vault
Storage keys, SQL credentials, Databricks tokens, Synapse secrets

Monitoring & Operations
Azure Log Analytics, Databricks job monitoring, ADF run history,
audit tables, pipeline alerts

Deployment / CI-CD
ARM templates, ADF CI/CD, Jenkins release pipeline,
environment parameters, Dev → QA → Prod promotion
```

The architecture diagram is available here:

```text
docs/architecture_diagram.png
```

---

## Repository Structure

```text
customer-360-azure/
├── README.md
├── requirements.txt
├── data/
│   ├── DATA_MANIFEST.json
│   ├── raw/
│   ├── reference/
│   └── sample_outputs/
├── notebooks/
│   └── 01_customer_360_analysis.ipynb
├── src/
│   ├── config.py
│   ├── spark_session.py
│   ├── schemas.py
│   ├── bronze.py
│   ├── silver.py
│   ├── scd_type1.py
│   ├── scd_type2.py
│   ├── gold.py
│   ├── data_quality.py
│   ├── audit.py
│   └── run_pipeline.py
├── azure/
│   ├── adf/
│   │   ├── pipelines/
│   │   ├── datasets/
│   │   └── linked_services/
│   ├── databricks/
│   │   └── jobs/
│   ├── synapse/
│   │   └── ddl/
│   ├── arm/
│   │   └── adf_arm_template.json
│   └── keyvault/
│       └── keyvault_reference_placeholders.json
└── docs/
    ├── architecture_diagram.png
    ├── powerbi_mockup.png
    └── data_dictionary.xlsx
```

---

## Included Data

Synthetic source files are included so the repository can be reviewed immediately.

| File | Rows | Purpose |
|---|---:|---|
| `data/raw/customers.csv` | 5,000 | Customer master/profile data |
| `data/raw/accounts.csv` | 8,000 | Account ownership and balance data |
| `data/raw/credit_cards.csv` | 2,000 | Card product data |
| `data/raw/loans.csv` | 1,000 | Loan exposure data |
| `data/raw/transactions.csv` | 50,000 | Transaction feed with fraud labels |
| `data/raw/branches.csv` | 100 | Branch reference data |

Reference files:

```text
data/reference/merchant_risk_reference.csv
data/reference/customer_risk_reference.csv
```

Sample output files:

```text
data/sample_outputs/gold_customer_360_sample.csv
data/sample_outputs/gold_daily_transaction_summary_sample.csv
data/sample_outputs/gold_fraud_risk_summary_sample.csv
data/sample_outputs/gold_product_holding_summary_sample.csv
data/sample_outputs/gold_branch_performance_sample.csv
```

---

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Run Notebook Analysis

```bash
jupyter notebook notebooks/01_customer_360_analysis.ipynb
```

The notebook uses pandas and regenerates sample outputs under:

```text
data/sample_outputs/
```

This is the fastest way for reviewers to validate the project without Spark setup issues.

---

## Run PySpark Pipeline

Use existing committed raw data:

```bash
python src/run_pipeline.py
```

Generate new synthetic raw data and then run the full pipeline:

```bash
python src/run_pipeline.py --generate-data
```

The PySpark pipeline writes full Bronze, Silver, Gold, Audit, and Business Mart outputs under:

```text
data/sample_outputs/bronze/
data/sample_outputs/silver/
data/sample_outputs/gold/
data/sample_outputs/marts/
data/sample_outputs/audit/
```

---

## Pipeline Layers

### Bronze

Bronze performs:

- Raw file loading
- Schema enforcement
- Source system tagging
- Load timestamp capture
- Audit column creation

Tables:

```text
bronze_customers
bronze_accounts
bronze_credit_cards
bronze_loans
bronze_transactions
bronze_branches
```

### Silver

Silver performs:

- Data standardization
- Null checks
- Duplicate checks
- Valid transaction filtering
- Customer/account/card enrichment
- SCD-1 customer contact processing
- SCD-2 customer risk profile processing
- Mini-batch transaction preparation
- Streaming-style transaction aggregations

Tables:

```text
silver_customers
silver_accounts
silver_credit_cards
silver_loans
silver_transactions
silver_customer_contact_current
silver_customer_risk_history
silver_mini_batch_transaction_summary
```

### Gold

Gold performs:

- Customer 360 aggregation
- Rule-based fraud scoring
- Spark ML fraud prediction
- Product holding summary
- Branch performance metrics

Tables:

```text
gold_customer_360
gold_transactions_scored
gold_daily_transaction_summary
gold_fraud_risk_summary
gold_product_holding_summary
gold_branch_performance
```

### Business Marts

Business marts align with Synapse and Power BI consumption.

```text
mart_customer_360
mart_fraud_analytics
mart_product_holding
mart_branch_performance
```

---

## SCD Design

### SCD Type 1

Used for contact fields where only the latest value matters:

- email
- phone number
- address
- city
- province
- postal code

### SCD Type 2

Used for historical profile/risk tracking:

- risk tier
- customer segment
- income band
- employment status
- credit score band

Columns:

```text
effective_start_date
effective_end_date
is_current
record_hash
```

---

## Fraud and Risk Analytics

### Rule-Based Risk Scoring

Rules:

| Rule | Score |
|---|---:|
| Transaction amount >= 1000 | 25 |
| Unusual hour, 11 PM to 5 AM | 15 |
| Risky merchant category | 20 |
| High-risk customer tier | 20 |
| Card not present | 10 |

Risk band:

```text
0-29   Low
30-59  Medium
60+    High
```

### Spark ML Fraud Prediction

The project also includes Spark ML predictive analytics.

Model:

```text
Logistic Regression
```

Features:

```text
transaction_amount
transaction_hour
age
credit_score
account_balance
risk_tier
merchant_category
transaction_channel
```

Outputs:

```text
fraud_ml_probability
fraud_ml_prediction
```

This is intentionally framed as predictive analytics with Spark ML, not a modern AI implementation.

---

## Azure Data Factory Artifacts

ADF templates are included under:

```text
azure/adf/
```

ADF design includes:

- Parameterized copy pipelines
- Self-hosted Integration Runtime pattern
- ADLS Gen2 dataset references
- Azure SQL dataset references
- Databricks notebook invocation
- Key Vault-backed linked services
- Trigger-ready parameters
- Dev/QA/Prod deployment alignment

---

## Databricks Job Design

Databricks job JSON is included under:

```text
azure/databricks/jobs/customer360_databricks_job.json
```

Task flow:

```text
bronze_load
silver_transform
scd_processing
fraud_risk_scoring
business_mart_publish
```

---

## Synapse Serving Layer

Synapse DDL is included under:

```text
azure/synapse/ddl/create_customer360_gold_tables.sql
```

It defines:

- Gold serving tables
- Business mart tables
- Hash and round-robin distribution choices
- Columnstore indexes for analytics workloads

---

## Monitoring and Operations

The design supports:

- ADF run history
- Databricks job monitoring
- Azure Log Analytics
- Audit tables
- Pipeline-level status tracking
- Failure alerts through ADF and Logic Apps pattern

---

## Deployment

Deployment is represented through:

- ARM template placeholder
- ADF linked service parameterization
- Key Vault secret references
- Environment-specific values
- Jenkins release pipeline pattern
- Dev → QA → Prod promotion

---

## Power BI / SSRS Metrics

Recommended dashboard pages:

### Customer 360

- Total customers
- Active customers
- Average products per customer
- Total deposits
- Total loan exposure
- Risk tier distribution

### Fraud Analytics

- Total transaction amount
- High-risk transaction count
- Confirmed fraud count
- Average fraud rule score
- ML-predicted fraud count

### Product Holding

- Account penetration
- Credit card penetration
- Loan penetration
- Products per customer
- Product holding by segment

### Branch Performance

- Customers by branch
- Deposits by branch
- Loan exposure by branch
- High-risk customer count
- Fraud transaction count

---

## Engineering Decisions

- Synthetic data is used because real bank customer data is private.
- The notebook uses pandas for fast local review.
- The pipeline uses PySpark to reflect Databricks/Spark engineering patterns.
- The Azure files are implementation-ready templates, not deployed infrastructure.
- Fraud analytics includes both rule-based scoring and Spark ML predictive scoring.
- SCD-1 and SCD-2 are included because customer contact fields and risk profile fields have different history requirements.
- The design reflects Azure Data Engineer responsibilities, not a pure data science project.
