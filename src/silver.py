from pyspark.sql import functions as F
from data_quality import filter_valid_transactions
from schemas import enforce_schema


def standardize_customers(customers):
    return (
        customers
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("province", F.upper(F.col("province")))
        .withColumn(
            "credit_score_band",
            F.when(F.col("credit_score") >= 760, "Excellent")
             .when(F.col("credit_score") >= 700, "Good")
             .when(F.col("credit_score") >= 640, "Fair")
             .otherwise("Poor")
        )
    )


def standardize_transactions(transactions):
    return (
        transactions
        .withColumn("transaction_date", F.to_date("transaction_timestamp"))
        .withColumn("transaction_hour", F.hour("transaction_timestamp"))
        .withColumn("transaction_amount", F.round(F.col("transaction_amount"), 2))
        .withColumn("mini_batch_id", F.date_format(F.col("transaction_timestamp"), "yyyyMMddHH"))
    )


def write_parquet_outputs(outputs, output_dir, output_schemas):
    output_dir.mkdir(parents=True, exist_ok=True)
    shaped_outputs = {}
    for name, df in outputs.items():
        shaped_df = enforce_schema(df, output_schemas[name])
        shaped_df.write.mode("overwrite").parquet(str(output_dir / name))
        shaped_outputs[name] = shaped_df
    return shaped_outputs


def run_silver(bronze, silver_dir, silver_schemas, customer_contact_scd1_fn, customer_risk_scd2_fn):
    customers = standardize_customers(bronze["customers"])
    accounts = bronze["accounts"]
    cards = bronze["credit_cards"]
    loans = bronze["loans"]
    transactions = filter_valid_transactions(standardize_transactions(bronze["transactions"]))
    branches = bronze["branches"]

    customer_contact_current = customer_contact_scd1_fn(customers)
    customer_risk_history = customer_risk_scd2_fn(customers)

    enriched_transactions = (
        transactions.alias("t")
        .join(customers.select("customer_id", "age", "risk_tier", "credit_score", "customer_segment"), "customer_id", "left")
        .join(accounts.select("account_id", "account_type", "account_balance", "branch_id"), "account_id", "left")
        .join(cards.select("card_id", "card_type", "credit_limit", "card_status"), "card_id", "left")
    )

    mini_batch_transaction_summary = (
        enriched_transactions
        .groupBy("mini_batch_id", "transaction_date", "transaction_hour")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("transaction_amount").alias("total_transaction_amount"),
            F.sum("fraud_label").alias("confirmed_fraud_count")
        )
    )

    outputs = {
        "customers": customers,
        "accounts": accounts,
        "credit_cards": cards,
        "loans": loans,
        "transactions": enriched_transactions,
        "branches": branches,
        "customer_contact_current": customer_contact_current,
        "customer_risk_history": customer_risk_history,
        "mini_batch_transaction_summary": mini_batch_transaction_summary,
    }

    return write_parquet_outputs(outputs, silver_dir, silver_schemas)
