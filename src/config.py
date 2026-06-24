import os
import sys
from pathlib import Path


def _path_env(name, default):
    return Path(os.getenv(name, str(default))).expanduser().resolve()


def _int_env(name, default):
    return int(os.getenv(name, str(default)))


PROJECT_ROOT = _path_env("CUSTOMER360_PROJECT_ROOT", Path(__file__).resolve().parents[1])

DATA_DIR = _path_env("CUSTOMER360_DATA_DIR", PROJECT_ROOT / "data")
RAW_DIR = _path_env("CUSTOMER360_RAW_DIR", DATA_DIR / "raw")
REFERENCE_DIR = _path_env("CUSTOMER360_REFERENCE_DIR", DATA_DIR / "reference")
OUTPUT_DIR = _path_env("CUSTOMER360_OUTPUT_DIR", DATA_DIR / "sample_outputs")

BRONZE_DIR = _path_env("CUSTOMER360_BRONZE_DIR", OUTPUT_DIR / "bronze")
SILVER_DIR = _path_env("CUSTOMER360_SILVER_DIR", OUTPUT_DIR / "silver")
GOLD_DIR = _path_env("CUSTOMER360_GOLD_DIR", OUTPUT_DIR / "gold")
MART_DIR = _path_env("CUSTOMER360_MART_DIR", OUTPUT_DIR / "marts")
AUDIT_DIR = _path_env("CUSTOMER360_AUDIT_DIR", OUTPUT_DIR / "audit")
DQ_DIR = _path_env("CUSTOMER360_DQ_DIR", OUTPUT_DIR / "data_quality")

LAYER_OUTPUT_DIRS = {
    "bronze": BRONZE_DIR,
    "silver": SILVER_DIR,
    "gold": GOLD_DIR,
    "marts": MART_DIR,
    "audit": AUDIT_DIR,
    "data_quality": DQ_DIR,
}

CUSTOMER_COUNT = _int_env("CUSTOMER360_CUSTOMER_COUNT", 5000)
ACCOUNT_COUNT = _int_env("CUSTOMER360_ACCOUNT_COUNT", 8000)
CARD_COUNT = _int_env("CUSTOMER360_CARD_COUNT", 2000)
LOAN_COUNT = _int_env("CUSTOMER360_LOAN_COUNT", 1000)
TRANSACTION_COUNT = _int_env("CUSTOMER360_TRANSACTION_COUNT", 50000)

APP_NAME = os.getenv("CUSTOMER360_APP_NAME", "Customer360AzureBankingPipeline")
SPARK_MASTER = os.getenv("CUSTOMER360_SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = os.getenv("CUSTOMER360_SPARK_SHUFFLE_PARTITIONS", "8")
SPARK_ARROW_ENABLED = os.getenv("CUSTOMER360_SPARK_ARROW_ENABLED", "true")
JAVA_HOME = os.getenv("JAVA_HOME", str(Path(sys.prefix) / "lib" / "jvm"))

RISKY_MERCHANT_CATEGORIES = [
    "crypto",
    "gambling",
    "foreign_cash",
    "high_value_electronics",
]

SOURCE_SYSTEMS = {
    "customers": "azure_sql_customer_master",
    "accounts": "onprem_mysql_account_core",
    "credit_cards": "cassandra_card_platform",
    "loans": "azure_sql_loan_servicing",
    "transactions": "blob_api_transaction_feed",
    "branches": "reference_branch_master",
}

RAW_TABLES = {
    "customers": {"file_name": "customers.csv", "key_cols": ["customer_id"]},
    "accounts": {"file_name": "accounts.csv", "key_cols": ["account_id"]},
    "credit_cards": {"file_name": "credit_cards.csv", "key_cols": ["card_id"]},
    "loans": {"file_name": "loans.csv", "key_cols": ["loan_id"]},
    "transactions": {"file_name": "transactions.csv", "key_cols": ["transaction_id"]},
    "branches": {"file_name": "branches.csv", "key_cols": ["branch_id"]},
}

SCD_OUTPUT_TABLES = {
    "customer_contact_current": "SCD Type 1 current customer contact table",
    "customer_risk_history": "SCD Type 2 customer risk history table",
}

LAYER_DQ_RULES = {
    "bronze": {
        "customers": {"key_cols": ["customer_id"], "not_null_cols": ["customer_id"]},
        "accounts": {"key_cols": ["account_id"], "not_null_cols": ["account_id", "customer_id"]},
        "credit_cards": {"key_cols": ["card_id"], "not_null_cols": ["card_id", "customer_id"]},
        "loans": {"key_cols": ["loan_id"], "not_null_cols": ["loan_id", "customer_id"]},
        "transactions": {"key_cols": ["transaction_id"], "not_null_cols": ["transaction_id", "customer_id"]},
        "branches": {"key_cols": ["branch_id"], "not_null_cols": ["branch_id"]},
    },
    "silver": {
        "customers": {"key_cols": ["customer_id"], "not_null_cols": ["customer_id"]},
        "accounts": {"key_cols": ["account_id"], "not_null_cols": ["account_id", "customer_id"]},
        "credit_cards": {"key_cols": ["card_id"], "not_null_cols": ["card_id", "customer_id"]},
        "loans": {"key_cols": ["loan_id"], "not_null_cols": ["loan_id", "customer_id"]},
        "transactions": {"key_cols": ["transaction_id"], "not_null_cols": ["transaction_id", "customer_id"]},
        "branches": {"key_cols": ["branch_id"], "not_null_cols": ["branch_id"]},
        "customer_contact_current": {"key_cols": ["customer_id"], "not_null_cols": ["customer_id"]},
        "customer_risk_history": {"key_cols": ["customer_id", "effective_start_date"], "not_null_cols": ["customer_id"]},
        "mini_batch_transaction_summary": {"key_cols": ["mini_batch_id"], "not_null_cols": ["mini_batch_id"]},
    },
    "gold": {
        "transactions_scored": {"key_cols": ["transaction_id"], "not_null_cols": ["transaction_id", "customer_id"]},
        "customer_360": {"key_cols": ["customer_id"], "not_null_cols": ["customer_id"]},
        "daily_transaction_summary": {"key_cols": ["transaction_date"], "not_null_cols": ["transaction_date"]},
        "fraud_risk_summary": {"key_cols": ["fraud_rule_band", "risk_tier", "merchant_category"], "not_null_cols": ["fraud_rule_band"]},
        "product_holding_summary": {"key_cols": ["customer_segment", "risk_tier"], "not_null_cols": ["customer_segment", "risk_tier"]},
        "branch_performance": {"key_cols": ["branch_id"], "not_null_cols": ["branch_id"]},
        "mart_customer_360": {"key_cols": ["customer_id"], "not_null_cols": ["customer_id"]},
        "mart_fraud_analytics": {"key_cols": ["fraud_rule_band", "risk_tier", "merchant_category"], "not_null_cols": ["fraud_rule_band"]},
        "mart_product_holding": {"key_cols": ["customer_segment", "risk_tier"], "not_null_cols": ["customer_segment", "risk_tier"]},
        "mart_branch_performance": {"key_cols": ["branch_id"], "not_null_cols": ["branch_id"]},
    },
}
