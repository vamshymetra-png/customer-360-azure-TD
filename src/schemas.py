from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
    TimestampType,
)

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("risk_tier", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("income_band", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("created_date", DateType(), True),
])

account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("open_date", DateType(), True),
    StructField("branch_id", StringType(), True),
])

card_schema = StructType([
    StructField("card_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_status", StringType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("issue_date", DateType(), True),
])

loan_schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("loan_type", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("loan_balance", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("start_date", DateType(), True),
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_channel", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("is_card_present", IntegerType(), True),
    StructField("fraud_label", IntegerType(), True),
])

branch_schema = StructType([
    StructField("branch_id", StringType(), False),
    StructField("branch_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("region", StringType(), True),
])

bronze_metadata_schema = StructType([
    StructField("source_system", StringType(), True),
    StructField("bronze_load_ts", TimestampType(), True),
    StructField("ingestion_mode", StringType(), True),
])

RAW_SCHEMAS = {
    "customers": customer_schema,
    "accounts": account_schema,
    "credit_cards": card_schema,
    "loans": loan_schema,
    "transactions": transaction_schema,
    "branches": branch_schema,
}


def append_fields(schema, fields):
    return StructType([*schema.fields, *fields])


BRONZE_SCHEMAS = {
    name: append_fields(schema, bronze_metadata_schema.fields)
    for name, schema in RAW_SCHEMAS.items()
}

SILVER_SCHEMAS = {
    "customers": append_fields(BRONZE_SCHEMAS["customers"], [StructField("credit_score_band", StringType(), True)]),
    "accounts": BRONZE_SCHEMAS["accounts"],
    "credit_cards": BRONZE_SCHEMAS["credit_cards"],
    "loans": BRONZE_SCHEMAS["loans"],
    "branches": BRONZE_SCHEMAS["branches"],
    "transactions": append_fields(BRONZE_SCHEMAS["transactions"], [
        StructField("transaction_date", DateType(), True),
        StructField("transaction_hour", IntegerType(), True),
        StructField("mini_batch_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("risk_tier", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("account_balance", DoubleType(), True),
        StructField("branch_id", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("credit_limit", DoubleType(), True),
        StructField("card_status", StringType(), True),
    ]),
    "customer_contact_current": StructType([
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("bronze_load_ts", TimestampType(), True),
        StructField("scd_type", StringType(), True),
        StructField("current_record_flag", IntegerType(), True),
    ]),
    "customer_risk_history": StructType([
        StructField("customer_id", StringType(), False),
        StructField("risk_tier", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("employment_status", StringType(), True),
        StructField("income_band", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("credit_score_band", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("record_hash", StringType(), True),
        StructField("effective_start_date", DateType(), True),
        StructField("effective_end_date", DateType(), True),
        StructField("is_current", IntegerType(), True),
        StructField("scd_type", StringType(), True),
    ]),
    "mini_batch_transaction_summary": StructType([
        StructField("mini_batch_id", StringType(), False),
        StructField("transaction_date", DateType(), True),
        StructField("transaction_hour", IntegerType(), True),
        StructField("transaction_count", LongType(), True),
        StructField("total_transaction_amount", DoubleType(), True),
        StructField("confirmed_fraud_count", LongType(), True),
    ]),
}

GOLD_SCHEMAS = {
    "transactions_scored": append_fields(SILVER_SCHEMAS["transactions"], [
        StructField("rule_high_amount", IntegerType(), True),
        StructField("rule_unusual_hour", IntegerType(), True),
        StructField("rule_risky_merchant", IntegerType(), True),
        StructField("rule_high_risk_customer", IntegerType(), True),
        StructField("rule_card_not_present", IntegerType(), True),
        StructField("fraud_rule_score", IntegerType(), True),
        StructField("fraud_rule_band", StringType(), True),
        StructField("risk_tier_idx", DoubleType(), True),
        StructField("merchant_category_idx", DoubleType(), True),
        StructField("transaction_channel_idx", DoubleType(), True),
        StructField("prediction", DoubleType(), True),
        StructField("fraud_ml_probability", DoubleType(), True),
        StructField("fraud_ml_prediction", IntegerType(), True),
    ]),
    "customer_360": append_fields(SILVER_SCHEMAS["customers"], [
        StructField("account_count", LongType(), True),
        StructField("total_account_balance", DoubleType(), True),
        StructField("card_count", LongType(), True),
        StructField("total_credit_limit", DoubleType(), True),
        StructField("loan_count", LongType(), True),
        StructField("total_loan_balance", DoubleType(), True),
        StructField("total_products", LongType(), True),
    ]),
    "daily_transaction_summary": StructType([
        StructField("transaction_date", DateType(), False),
        StructField("transaction_count", LongType(), True),
        StructField("total_transaction_amount", DoubleType(), True),
        StructField("avg_rule_fraud_score", DoubleType(), True),
        StructField("confirmed_fraud_count", LongType(), True),
        StructField("ml_predicted_fraud_count", LongType(), True),
    ]),
    "fraud_risk_summary": StructType([
        StructField("fraud_rule_band", StringType(), False),
        StructField("risk_tier", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("transaction_count", LongType(), True),
        StructField("total_transaction_amount", DoubleType(), True),
        StructField("confirmed_fraud_count", LongType(), True),
        StructField("avg_ml_fraud_probability", DoubleType(), True),
        StructField("avg_rule_score", DoubleType(), True),
    ]),
    "product_holding_summary": StructType([
        StructField("customer_segment", StringType(), False),
        StructField("risk_tier", StringType(), False),
        StructField("customer_count", LongType(), True),
        StructField("avg_products_per_customer", DoubleType(), True),
        StructField("avg_account_balance", DoubleType(), True),
        StructField("avg_loan_balance", DoubleType(), True),
    ]),
    "branch_performance": StructType([
        StructField("branch_id", StringType(), False),
        StructField("branch_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("province", StringType(), True),
        StructField("customer_count", LongType(), True),
        StructField("account_count", LongType(), True),
        StructField("total_deposit_balance", DoubleType(), True),
    ]),
}
GOLD_SCHEMAS.update({
    "mart_customer_360": GOLD_SCHEMAS["customer_360"],
    "mart_fraud_analytics": GOLD_SCHEMAS["fraud_risk_summary"],
    "mart_product_holding": GOLD_SCHEMAS["product_holding_summary"],
    "mart_branch_performance": GOLD_SCHEMAS["branch_performance"],
})


def enforce_schema(df, schema: StructType):
    """Select and cast a DataFrame to the expected schema before writing."""
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])
