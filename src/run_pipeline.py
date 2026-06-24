import argparse
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from config import (
    RAW_DIR,
    REFERENCE_DIR,
    RAW_TABLES,
    CUSTOMER_COUNT,
    ACCOUNT_COUNT,
    CARD_COUNT,
    LOAN_COUNT,
    TRANSACTION_COUNT,
)
from spark_session import get_spark
from bronze import run_bronze
from silver import run_silver
from gold import run_gold
from audit import write_audit_record
from data_quality import validate_layer

random.seed(42)
np.random.seed(42)


def rand_date(days_back_min=30, days_back_max=3000):
    return (datetime.now() - timedelta(days=random.randint(days_back_min, days_back_max))).date().isoformat()


def generate_sample_data():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    first_names = ["Aarav", "Liam", "Noah", "Olivia", "Emma", "Sophia", "Mia", "Ethan", "Lucas", "Amelia", "Mason", "Ava"]
    last_names = ["Patel", "Smith", "Brown", "Singh", "Kumar", "Wilson", "Martin", "Lee", "Chen", "Taylor", "Anderson", "Thomas"]
    cities = ["Toronto", "Mississauga", "Brampton", "Ottawa", "Vancouver", "Calgary", "Montreal", "Halifax", "Winnipeg", "Edmonton"]
    provinces = ["ON", "QC", "BC", "AB", "MB", "NS"]

    customers = []
    for i in range(1, CUSTOMER_COUNT + 1):
        fn, ln = random.choice(first_names), random.choice(last_names)
        customers.append({
            "customer_id": f"CUST{i:06d}",
            "first_name": fn,
            "last_name": ln,
            "gender": random.choice(["M", "F"]),
            "age": random.randint(18, 80),
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "phone_number": f"416-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "address": f"{random.randint(1,9999)} {random.choice(['King','Queen','Main','Bay','Dundas'])} St",
            "city": random.choice(cities),
            "province": random.choice(provinces),
            "postal_code": "A1A 1A1",
            "customer_segment": random.choice(["Retail", "Mass Affluent", "Small Business", "Student", "Newcomer"]),
            "risk_tier": random.choices(["Low", "Medium", "High"], weights=[70, 23, 7])[0],
            "credit_score": int(np.clip(np.random.normal(710, 65), 450, 850)),
            "income_band": random.choice(["<50K", "50K-100K", "100K-150K", "150K+"]),
            "employment_status": random.choice(["Employed", "Self-Employed", "Student", "Retired", "Unemployed"]),
            "created_date": rand_date(365, 2800),
        })

    customers_df = pd.DataFrame(customers)
    customers_df.to_csv(RAW_DIR / RAW_TABLES["customers"]["file_name"], index=False)

    branches_df = pd.DataFrame([{
        "branch_id": f"BR{i:04d}",
        "branch_name": f"Branch {i}",
        "city": random.choice(cities),
        "province": random.choice(provinces),
        "region": random.choice(["East", "Central", "West", "Atlantic"]),
    } for i in range(1, 101)])
    branches_df.to_csv(RAW_DIR / RAW_TABLES["branches"]["file_name"], index=False)

    customer_ids = customers_df["customer_id"].tolist()
    branch_ids = branches_df["branch_id"].tolist()

    accounts_df = pd.DataFrame([{
        "account_id": f"ACC{i:07d}",
        "customer_id": random.choice(customer_ids),
        "account_type": random.choice(["Chequing", "Savings", "Line of Credit"]),
        "account_status": random.choices(["Active", "Dormant", "Closed"], weights=[90, 7, 3])[0],
        "account_balance": round(float(np.random.gamma(3, 2500)), 2),
        "open_date": rand_date(30, 3000),
        "branch_id": random.choice(branch_ids),
    } for i in range(1, ACCOUNT_COUNT + 1)])
    accounts_df.to_csv(RAW_DIR / RAW_TABLES["accounts"]["file_name"], index=False)

    cards_source = accounts_df.sample(n=CARD_COUNT, random_state=42).reset_index(drop=True)
    cards_df = pd.DataFrame([{
        "card_id": f"CARD{i+1:07d}",
        "customer_id": row["customer_id"],
        "account_id": row["account_id"],
        "card_type": random.choice(["Visa Classic", "Visa Infinite", "Cash Back", "Travel Rewards"]),
        "card_status": random.choices(["Active", "Blocked", "Closed"], weights=[92, 3, 5])[0],
        "credit_limit": float(random.choice([1000, 2500, 5000, 7500, 10000, 15000, 20000])),
        "issue_date": rand_date(30, 1800),
    } for i, row in cards_source.iterrows()])
    cards_df.to_csv(RAW_DIR / RAW_TABLES["credit_cards"]["file_name"], index=False)

    loans_df = pd.DataFrame([{
        "loan_id": f"LOAN{i:07d}",
        "customer_id": random.choice(customer_ids),
        "loan_type": random.choice(["Personal", "Auto", "Mortgage", "Student", "Business"]),
        "loan_status": random.choices(["Active", "Closed", "Delinquent"], weights=[86, 10, 4])[0],
        "loan_amount": float(np.random.choice([5000, 10000, 15000, 25000, 50000, 150000, 300000, 500000])),
        "loan_balance": 0.0,
        "interest_rate": round(random.uniform(2.5, 12.5), 2),
        "start_date": rand_date(60, 2200),
    } for i in range(1, LOAN_COUNT + 1)])
    loans_df["loan_balance"] = (loans_df["loan_amount"] * np.random.uniform(0.15, 0.95, size=len(loans_df))).round(2)
    loans_df.to_csv(RAW_DIR / RAW_TABLES["loans"]["file_name"], index=False)

    merchant_categories = ["grocery", "fuel", "restaurant", "travel", "retail", "utilities", "crypto", "gambling", "foreign_cash", "high_value_electronics"]
    cards_by_customer = cards_df.groupby("customer_id")["card_id"].first().to_dict()
    account_rows = accounts_df.to_dict("records")
    start_dt = datetime.now() - timedelta(days=180)
    transactions = []

    for i in range(1, TRANSACTION_COUNT + 1):
        acc = random.choice(account_rows)
        ts = start_dt + timedelta(days=random.randint(0, 179), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        merchant = random.choices(merchant_categories, weights=[25, 10, 15, 8, 18, 8, 2, 2, 2, 10])[0]
        amount = round(float(np.random.gamma(2.2, 80)), 2)
        is_card_present = random.choices([1, 0], weights=[78, 22])[0]
        fraud_probability = 0.005
        if amount > 1000:
            fraud_probability += 0.04
        if merchant in ["crypto", "gambling", "foreign_cash", "high_value_electronics"]:
            fraud_probability += 0.03
        if ts.hour <= 5 or ts.hour >= 23:
            fraud_probability += 0.02
        if is_card_present == 0:
            fraud_probability += 0.02

        transactions.append({
            "transaction_id": f"TXN{i:09d}",
            "customer_id": acc["customer_id"],
            "account_id": acc["account_id"],
            "card_id": cards_by_customer.get(acc["customer_id"], "") if random.random() < 0.55 else "",
            "transaction_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_amount": amount,
            "merchant_category": merchant,
            "transaction_channel": random.choice(["POS", "Online", "ATM", "Mobile", "Branch"]),
            "transaction_type": random.choice(["Debit", "Credit", "Payment", "Withdrawal", "Transfer"]),
            "is_card_present": is_card_present,
            "fraud_label": 1 if random.random() < fraud_probability else 0,
        })

    pd.DataFrame(transactions).to_csv(RAW_DIR / RAW_TABLES["transactions"]["file_name"], index=False)

    pd.DataFrame({
        "merchant_category": merchant_categories,
        "risk_weight": [5, 5, 5, 10, 5, 5, 20, 20, 20, 20],
    }).to_csv(REFERENCE_DIR / "merchant_risk_reference.csv", index=False)

    pd.DataFrame({
        "risk_tier": ["Low", "Medium", "High"],
        "risk_weight": [0, 10, 20],
    }).to_csv(REFERENCE_DIR / "customer_risk_reference.csv", index=False)


def validate_raw_files():
    missing = [
        table_config["file_name"]
        for table_config in RAW_TABLES.values()
        if not (RAW_DIR / table_config["file_name"]).exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing raw files: {missing}. Run with --generate-data or add files under data/raw."
        )


def audit_layer(spark, layer, tables, message):
    for name, df in tables.items():
        write_audit_record(spark, layer, name, df.count(), "SUCCESS", message)


def print_dq_results(layer, results):
    print(f"{layer.title()} data quality checks:")
    for result in results:
        print(
            f"  {result['table_name']}: {result['status']} "
            f"rows={result['row_count']} duplicates={result['duplicate_key_count']} "
            f"null_failures={result['null_check_failures'] or 'none'}"
        )


def main():
    parser = argparse.ArgumentParser(description="Run Customer 360 Azure banking PySpark pipeline.")
    parser.add_argument("--generate-data", action="store_true", help="Regenerate synthetic raw data before pipeline execution.")
    args = parser.parse_args()

    if args.generate_data:
        generate_sample_data()

    validate_raw_files()

    spark = get_spark()

    bronze = run_bronze(spark)
    bronze_dq = validate_layer(spark, "bronze", bronze)
    print_dq_results("bronze", bronze_dq)
    audit_layer(spark, "bronze", bronze, "Bronze load and data quality checks completed")

    silver = run_silver(bronze)
    silver_dq = validate_layer(spark, "silver", silver)
    print_dq_results("silver", silver_dq)
    audit_layer(
        spark,
        "silver",
        silver,
        "Silver transformation, SCD1 customer contact, SCD2 customer risk, and data quality checks completed",
    )

    gold = run_gold(silver)
    gold_dq = validate_layer(spark, "gold", gold)
    print_dq_results("gold", gold_dq)
    audit_layer(spark, "gold", gold, "Gold, mart, ML scoring, and data quality checks completed")

    print("Customer 360 Azure banking pipeline completed successfully.")
    print("Outputs written under data/sample_outputs.")
    spark.stop()


if __name__ == "__main__":
    main()
