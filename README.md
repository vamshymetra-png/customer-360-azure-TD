# Customer 360 Azure Banking Data Platform

## Executive Summary

This project demonstrates how a bank can bring together customer, account, credit card, loan, transaction, branch, and risk data into one trusted Customer 360 analytics platform.

In simple terms, the platform helps answer questions such as:

- Who are our customers?
- What banking products does each customer hold?
- Which customers or transactions look risky?
- Which branches are performing well?
- What data should business teams see in dashboards?
- Can the same pipeline support analytics, reporting, audit, and operations?

The repository includes synthetic banking data, a local PySpark pipeline, a notebook for quick review, Azure-oriented design artifacts, and sample dashboard-ready outputs. No real customer or bank data is included.

## Why This Project Matters

Banks usually store customer information across many systems. Customer profile data may live in a CRM or SQL database. Account balances may come from a core banking platform. Credit card information may come from a card system. Transactions may arrive from files, APIs, or streaming feeds. Branch reference data may be managed separately.

When those systems are not connected, teams do not have a complete customer view. A fraud analyst may see a suspicious transaction but not the customer's overall relationship. A branch manager may see deposits but not product holding or risk tier. A product team may know card ownership but not loan exposure.

This project shows how those separate sources can be connected through a governed data platform:

```text
Raw source data
  -> Bronze trusted landing tables
  -> Silver cleaned and enriched tables
  -> Gold business-ready analytics tables
  -> Business marts for Power BI, SSRS, and Synapse
```

The goal is to show both the business value and the engineering pattern behind a modern Customer 360 data platform.

## What Is Included

This repository includes:

- Synthetic banking source data
- A local PySpark pipeline using Bronze, Silver, and Gold layers
- Data quality checks after Bronze, Silver, and Gold
- Schema enforcement before pipeline outputs are written
- SCD Type 1 processing for current customer contact information
- SCD Type 2 processing for customer risk/profile history
- Rule-based fraud scoring
- Spark ML fraud prediction using logistic regression
- Business marts for Customer 360, fraud analytics, product holding, and branch performance
- Audit and data quality outputs
- A pandas notebook for quick local exploration
- Azure Data Factory, Databricks, Synapse, Key Vault, and ARM template examples
- Power BI and documentation assets

## Business Outcomes

### Customer 360

The Customer 360 output combines profile, product, balance, and risk information into one customer-level view.

Business users can answer:

- How many products does each customer hold?
- What is the customer's total deposit balance?
- Does the customer have credit exposure through cards or loans?
- Which segment and risk tier does the customer belong to?
- Which customers may need relationship, retention, or risk review?

### Fraud And Risk Analytics

The fraud outputs score transactions using clear business rules and a Spark ML model.

Business users can answer:

- Which transactions are high risk?
- Which merchant categories are associated with more suspicious activity?
- How many transactions are confirmed fraud?
- How do rule-based scores compare with ML predictions?
- Which customer risk tiers are linked to higher fraud exposure?

### Product Holding Analysis

The product holding output summarizes how customers use banking products.

Business users can answer:

- Which customer segments hold the most products?
- What is the average number of products per customer?
- Which risk tiers have higher loan exposure?
- Where are cross-sell or engagement opportunities?

### Branch Performance

The branch output summarizes customer counts, account counts, and deposit balances by branch.

Business users can answer:

- Which branches serve the most customers?
- Which branches hold the highest deposit balances?
- How is product or risk exposure distributed across branches?

## Architecture In Plain English

This solution uses a layered architecture. Each layer has a clear purpose.

### Source Data

Source data represents the different systems a bank might use:

- Customer master data
- Account data
- Credit card data
- Loan data
- Transaction data
- Branch reference data
- Risk reference data

In this project, those sources are represented by CSV files under `data/raw/` and `data/reference/`.

### Bronze Layer

Bronze is the trusted landing layer. It reads raw files, applies expected schemas, adds audit metadata, and records the source system.

Bronze answers:

```text
Did we receive the data, and does it match the expected structure?
```

### Silver Layer

Silver is the cleaned and enriched layer. It standardizes values, filters invalid records, joins related data, and creates SCD tables.

Silver answers:

```text
Is the data clean, connected, and ready for business logic?
```

### Gold Layer

Gold is the business-ready analytics layer. It creates Customer 360, fraud scoring, product holding, branch performance, and transaction scoring outputs.

Gold answers:

```text
Can business users and reporting tools consume this data directly?
```

### Business Marts

Business marts are curated Gold outputs aligned to dashboard and reporting needs.

They support:

- Customer 360 reporting
- Fraud analytics reporting
- Product holding reporting
- Branch performance reporting

## High-Level Architecture

```text
Banking source systems
  Azure SQL, on-prem databases, card platforms, transaction feeds
        |
        v
Azure Data Factory
  Orchestration, file movement, schedules, Databricks invocation
        |
        v
ADLS Gen2
  Raw files and managed lake zones
        |
        v
Databricks / Spark
  Bronze, Silver, and Gold transformations
        |
        v
Data Quality and Audit
  Row counts, duplicate checks, null checks, pipeline status
        |
        v
Synapse / Business Marts
  SQL serving layer for reporting and analytics
        |
        v
Power BI / SSRS
  Customer, fraud, branch, product, and executive dashboards
```

The local version of this project runs the Spark transformation logic on your machine and writes outputs under `data/sample_outputs/`.

## Repository Structure

```text
customer-360-azure/
|-- README.md
|-- requirements.txt
|-- data/
|   |-- DATA_MANIFEST.json
|   |-- raw/
|   |-- reference/
|   `-- sample_outputs/
|-- notebooks/
|   `-- 01_customer_360_analysis.ipynb
|-- src/
|   |-- config.py
|   |-- spark_session.py
|   |-- schemas.py
|   |-- bronze.py
|   |-- silver.py
|   |-- scd_type1.py
|   |-- scd_type2.py
|   |-- gold.py
|   |-- data_quality.py
|   |-- audit.py
|   `-- run_pipeline.py
|-- azure/
|   |-- adf/
|   |-- databricks/
|   |-- synapse/
|   |-- arm/
|   `-- keyvault/
`-- docs/
    |-- architecture_diagram.png
    |-- powerbi_mockup.png
    `-- data_dictionary.xlsx
```

## Source Data Included

All source data is synthetic. It is designed to look realistic enough for demonstration, but it does not represent real people, real accounts, or real transactions.

| File | Rows | Description |
|---|---:|---|
| `data/raw/customers.csv` | 5,000 | Customer profile and risk attributes |
| `data/raw/accounts.csv` | 8,000 | Bank accounts, balances, and branch ownership |
| `data/raw/credit_cards.csv` | 2,000 | Credit card products and limits |
| `data/raw/loans.csv` | 1,000 | Loan products, balances, and rates |
| `data/raw/transactions.csv` | 50,000 | Transaction activity with fraud labels |
| `data/raw/branches.csv` | 100 | Branch reference data |

Reference data:

```text
data/reference/customer_risk_reference.csv
data/reference/merchant_risk_reference.csv
```

## Sample Outputs

The repository includes review-friendly CSV outputs under `data/sample_outputs/`.

Important sample files:

```text
data/sample_outputs/gold_customer_360_sample.csv
data/sample_outputs/gold_daily_transaction_summary_sample.csv
data/sample_outputs/gold_fraud_risk_summary_sample.csv
data/sample_outputs/gold_product_holding_summary_sample.csv
data/sample_outputs/gold_branch_performance_sample.csv
data/sample_outputs/gold_transactions_scored_sample.csv
```

When the Spark pipeline runs, it also creates full Parquet and CSV outputs under:

```text
data/sample_outputs/bronze/
data/sample_outputs/silver/
data/sample_outputs/gold/
data/sample_outputs/marts/
data/sample_outputs/audit/
data/sample_outputs/data_quality/
```

These generated folders are runtime outputs and do not need to be committed to source control.

## Local Environment

The project can run with any Python environment that has the packages from `requirements.txt` installed. PySpark also needs a local Java runtime.

Recommended local setup:

```text
Python 3.12 or compatible
PySpark 3.5.1
OpenJDK 17
```

If your IDE highlights `pyspark.sql` as missing, select the Python interpreter where you installed `requirements.txt`.

## Install Dependencies

From the project root:

```bash
cd customer-360-azure
python -m pip install -r requirements.txt
```

If Java is missing, install OpenJDK into Anaconda:

```bash
conda install -y -c conda-forge openjdk=17
```

The pipeline also sets `JAVA_HOME` automatically from `src/config.py`.

## Run The Full Spark Pipeline

Use the existing synthetic raw data:

```bash
python src/run_pipeline.py
```

Regenerate synthetic raw data and then run the pipeline:

```bash
python src/run_pipeline.py --generate-data
```

Successful execution prints data quality results for Bronze, Silver, and Gold. Example:

```text
Bronze data quality checks:
  customers: PASS rows=5000 duplicates=0 null_failures=none
  accounts: PASS rows=8000 duplicates=0 null_failures=none

Silver data quality checks:
  customer_contact_current: PASS rows=5000 duplicates=0 null_failures=none
  customer_risk_history: PASS rows=5000 duplicates=0 null_failures=none

Gold data quality checks:
  customer_360: PASS rows=5000 duplicates=0 null_failures=none
  transactions_scored: PASS rows=50000 duplicates=0 null_failures=none
```

## Run The Notebook

For a faster, lighter review, open the notebook:

```bash
jupyter notebook notebooks/01_customer_360_analysis.ipynb
```

The notebook uses pandas and scikit-learn for local analysis. It is useful for exploring the data and regenerating small CSV sample outputs. The Spark pipeline is the main engineered implementation.

## Configuration

Configuration is centralized in `src/config.py`.

It controls:

- Project paths
- Raw data location
- Bronze, Silver, Gold, Mart, Audit, and DQ output locations
- Spark app name
- Spark master
- Shuffle partitions
- Java home
- Source system names
- Raw table definitions
- Data quality rules
- Synthetic data volumes

Useful environment overrides:

| Environment Variable | Purpose |
|---|---|
| `CUSTOMER360_PROJECT_ROOT` | Override project root |
| `CUSTOMER360_DATA_DIR` | Override data directory |
| `CUSTOMER360_OUTPUT_DIR` | Override output location defaults |
| `CUSTOMER360_BRONZE_DIR` | Override Bronze output path |
| `CUSTOMER360_SILVER_DIR` | Override Silver output path |
| `CUSTOMER360_GOLD_DIR` | Override Gold output path |
| `CUSTOMER360_MART_DIR` | Override Mart output path |
| `CUSTOMER360_DQ_DIR` | Override data quality output path |
| `CUSTOMER360_SPARK_MASTER` | Override Spark master |
| `CUSTOMER360_SPARK_SHUFFLE_PARTITIONS` | Override Spark shuffle partitions |
| `JAVA_HOME` | Override Java runtime path |

## Schema Enforcement

Schemas are defined in `src/schemas.py`.

The project enforces schemas before writing:

- Raw source schemas
- Bronze schemas with ingestion metadata
- Silver schemas with enrichment and SCD columns
- Gold schemas for business outputs and marts

This matters because analytics platforms should not silently accept unexpected columns, wrong data types, or inconsistent table shapes.

## Data Quality

Data quality checks are defined in `src/data_quality.py` and configured in `src/config.py`.

The pipeline checks:

- Required key fields are not null
- Primary or business keys are not duplicated
- Each layer has expected row-level integrity
- Invalid transactions are filtered before enrichment

DQ results are printed during the run and written to:

```text
data/sample_outputs/data_quality/
```

## Audit

Audit records are written by `src/audit.py`.

The audit output records:

- Timestamp
- Pipeline layer
- Table name
- Row count
- Status
- Message

Audit output is written to:

```text
data/sample_outputs/audit/
```

## Detailed Pipeline Layers

### Bronze

Code: `src/bronze.py`

Bronze reads raw CSV files using explicit schemas. It adds:

- `source_system`
- `bronze_load_ts`
- `ingestion_mode`

The Bronze layer is intentionally close to the source data, but with enough structure to make the data trustworthy.

### Silver

Code: `src/silver.py`

Silver performs:

- Customer standardization
- Province and email cleanup
- Credit score banding
- Transaction date and hour derivation
- Invalid transaction filtering
- Customer, account, and card enrichment
- SCD Type 1 current contact table
- SCD Type 2 customer risk history table
- Mini-batch transaction summaries

### Gold

Code: `src/gold.py`

Gold creates:

- `transactions_scored`
- `customer_360`
- `daily_transaction_summary`
- `fraud_risk_summary`
- `product_holding_summary`
- `branch_performance`

It also creates business marts:

- `mart_customer_360`
- `mart_fraud_analytics`
- `mart_product_holding`
- `mart_branch_performance`

## Slowly Changing Dimensions

Customer attributes change over time. Some changes should overwrite old values. Other changes should preserve history.

### SCD Type 1: Current Contact Information

SCD Type 1 keeps only the current/latest value.

Used for:

- Email
- Phone number
- Address
- City
- Province
- Postal code

Output:

```text
customer_contact_current
```

### SCD Type 2: Historical Risk Profile

SCD Type 2 keeps history. This is important when analysts need to understand how customer risk or profile changed over time.

Tracked attributes:

- Risk tier
- Customer segment
- Employment status
- Income band
- Credit score band

Output:

```text
customer_risk_history
```

Important columns:

```text
record_hash
effective_start_date
effective_end_date
is_current
```

## Fraud Scoring

The project uses two fraud and risk techniques.

### Rule-Based Score

This score is easy to explain to business users and auditors.

| Rule | Score |
|---|---:|
| Transaction amount is at least 1000 | 25 |
| Transaction occurs between 11 PM and 5 AM | 15 |
| Merchant category is high risk | 20 |
| Customer is high risk | 20 |
| Card is not present | 10 |

Risk band:

```text
0-29   Low
30-59  Medium
60+    High
```

### Spark ML Score

The project also trains a Spark ML logistic regression model.

Model features:

- Transaction amount
- Transaction hour
- Customer age
- Credit score
- Account balance
- Risk tier
- Merchant category
- Transaction channel

Model outputs:

```text
fraud_ml_probability
fraud_ml_prediction
```

This is a demonstration of predictive analytics in Spark. It is not presented as a production fraud model.

## Azure Artifacts

The `azure/` folder contains Azure-ready design assets.

### Azure Data Factory

Folder:

```text
azure/adf/
```

Includes:

- Pipelines
- Datasets
- Linked services
- Key Vault-backed connection placeholders
- Databricks orchestration pattern

### Azure Databricks

Folder:

```text
azure/databricks/
```

Includes a Databricks job design for orchestrating the main pipeline tasks.

### Azure Synapse

Folder:

```text
azure/synapse/
```

Includes SQL DDL for serving-layer tables and marts.

### Azure Resource Manager

Folder:

```text
azure/arm/
```

Includes an ARM template placeholder for ADF deployment.

### Azure Key Vault

Folder:

```text
azure/keyvault/
```

Includes placeholders for secret references such as storage keys, SQL credentials, and Databricks tokens.

## Reporting Layer

Power BI and SSRS can consume the Gold and Mart outputs.

Recommended reporting pages:

### Customer 360 Dashboard

- Total customers
- Active customers
- Products per customer
- Total deposits
- Total loan exposure
- Risk tier distribution

### Fraud Analytics Dashboard

- Transaction volume
- Confirmed fraud count
- High-risk transaction count
- Average fraud rule score
- ML-predicted fraud count
- Fraud by merchant category

### Product Holding Dashboard

- Account penetration
- Credit card penetration
- Loan penetration
- Products per customer
- Product holding by customer segment

### Branch Performance Dashboard

- Customers by branch
- Account count by branch
- Deposit balance by branch
- Product holding by branch
- Risk exposure by branch

Mockup:

```text
docs/powerbi_mockup.png
```

## Documentation Assets

Additional documentation is included under `docs/`.

```text
docs/architecture_diagram.png
docs/powerbi_mockup.png
docs/data_dictionary.xlsx
```

The data dictionary explains the fields used across the project.

## What To Review First

For a non-technical review, start with:

1. `README.md`
2. `docs/architecture_diagram.png`
3. `docs/powerbi_mockup.png`
4. `data/sample_outputs/gold_customer_360_sample.csv`
5. `data/sample_outputs/gold_fraud_risk_summary_sample.csv`
6. `data/sample_outputs/gold_product_holding_summary_sample.csv`

For a technical review, start with:

1. `src/run_pipeline.py`
2. `src/config.py`
3. `src/schemas.py`
4. `src/bronze.py`
5. `src/silver.py`
6. `src/gold.py`
7. `src/data_quality.py`

## Important Notes

- This is a portfolio/demo project.
- All data is synthetic.
- The Azure artifacts are templates and examples, not a deployed cloud environment.
- The local Spark pipeline is the executable implementation.
- Generated Spark outputs can be recreated by running `python src/run_pipeline.py`.
- Real production banking platforms would require stronger controls around security, privacy, lineage, access management, testing, deployment, and monitoring.

## License

See `LICENSE`.
