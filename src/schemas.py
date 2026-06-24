from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
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

RAW_SCHEMAS = {
    "customers": customer_schema,
    "accounts": account_schema,
    "credit_cards": card_schema,
    "loans": loan_schema,
    "transactions": transaction_schema,
    "branches": branch_schema,
}


def enforce_schema(df, schema: StructType):
    """Select and cast a DataFrame to the expected schema before writing."""
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])
