from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from schemas import enforce_schema


def apply_rule_based_fraud_score(transactions, risky_merchant_categories):
    return (
        transactions
        .withColumn("rule_high_amount", F.when(F.col("transaction_amount") >= 1000, 25).otherwise(0))
        .withColumn("rule_unusual_hour", F.when((F.col("transaction_hour") <= 5) | (F.col("transaction_hour") >= 23), 15).otherwise(0))
        .withColumn("rule_risky_merchant", F.when(F.col("merchant_category").isin(risky_merchant_categories), 20).otherwise(0))
        .withColumn("rule_high_risk_customer", F.when(F.col("risk_tier") == "High", 20).otherwise(0))
        .withColumn("rule_card_not_present", F.when(F.col("is_card_present") == 0, 10).otherwise(0))
        .withColumn(
            "fraud_rule_score",
            F.col("rule_high_amount")
            + F.col("rule_unusual_hour")
            + F.col("rule_risky_merchant")
            + F.col("rule_high_risk_customer")
            + F.col("rule_card_not_present")
        )
        .withColumn(
            "fraud_rule_band",
            F.when(F.col("fraud_rule_score") >= 60, "High")
             .when(F.col("fraud_rule_score") >= 30, "Medium")
             .otherwise("Low")
        )
    )


def train_score_fraud_model(transactions):
    model_input = transactions.fillna({
        "risk_tier": "Unknown",
        "merchant_category": "Unknown",
        "transaction_channel": "Unknown",
        "age": 0,
        "credit_score": 0,
        "account_balance": 0.0,
        "transaction_amount": 0.0,
        "transaction_hour": 0,
    })

    risk_indexer = StringIndexer(inputCol="risk_tier", outputCol="risk_tier_idx", handleInvalid="keep")
    merchant_indexer = StringIndexer(inputCol="merchant_category", outputCol="merchant_category_idx", handleInvalid="keep")
    channel_indexer = StringIndexer(inputCol="transaction_channel", outputCol="transaction_channel_idx", handleInvalid="keep")

    assembler = VectorAssembler(
        inputCols=[
            "transaction_amount",
            "transaction_hour",
            "age",
            "credit_score",
            "account_balance",
            "risk_tier_idx",
            "merchant_category_idx",
            "transaction_channel_idx",
        ],
        outputCol="features",
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="fraud_label",
        probabilityCol="fraud_probability_vector",
        maxIter=10,
    )

    pipeline = Pipeline(stages=[risk_indexer, merchant_indexer, channel_indexer, assembler, lr])
    model = pipeline.fit(model_input)
    scored = model.transform(model_input)

    get_prob = F.udf(lambda v: float(v[1]), "double")

    return (
        scored
        .withColumn("fraud_ml_probability", get_prob(F.col("fraud_probability_vector")))
        .withColumn("fraud_ml_prediction", F.col("prediction").cast("int"))
        .drop("features", "rawPrediction", "fraud_probability_vector", "probability")
    )


def write_gold_outputs(gold_outputs, marts, gold_dir, mart_dir, gold_schemas):
    gold_dir.mkdir(parents=True, exist_ok=True)
    mart_dir.mkdir(parents=True, exist_ok=True)
    shaped_outputs = {}

    for name, df in gold_outputs.items():
        shaped_df = enforce_schema(df, gold_schemas[name])
        shaped_df.write.mode("overwrite").parquet(str(gold_dir / name))
        shaped_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(gold_dir / f"{name}_csv"))
        shaped_outputs[name] = shaped_df

    for name, df in marts.items():
        shaped_df = enforce_schema(df, gold_schemas[name])
        shaped_df.write.mode("overwrite").parquet(str(mart_dir / name))
        shaped_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(mart_dir / f"{name}_csv"))
        shaped_outputs[name] = shaped_df

    return shaped_outputs


def run_gold(silver, gold_dir, mart_dir, gold_schemas, risky_merchant_categories):
    transactions_scored = train_score_fraud_model(
        apply_rule_based_fraud_score(silver["transactions"], risky_merchant_categories)
    )

    customer_360 = (
        silver["customers"]
        .join(
            silver["accounts"].groupBy("customer_id")
            .agg(
                F.countDistinct("account_id").alias("account_count"),
                F.sum("account_balance").alias("total_account_balance"),
            ),
            "customer_id",
            "left",
        )
        .join(
            silver["credit_cards"].groupBy("customer_id")
            .agg(
                F.countDistinct("card_id").alias("card_count"),
                F.sum("credit_limit").alias("total_credit_limit"),
            ),
            "customer_id",
            "left",
        )
        .join(
            silver["loans"].groupBy("customer_id")
            .agg(
                F.countDistinct("loan_id").alias("loan_count"),
                F.sum("loan_balance").alias("total_loan_balance"),
            ),
            "customer_id",
            "left",
        )
        .fillna({
            "account_count": 0,
            "total_account_balance": 0.0,
            "card_count": 0,
            "total_credit_limit": 0.0,
            "loan_count": 0,
            "total_loan_balance": 0.0,
        })
        .withColumn("total_products", F.col("account_count") + F.col("card_count") + F.col("loan_count"))
    )

    daily_transaction_summary = (
        transactions_scored
        .groupBy("transaction_date")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("transaction_amount").alias("total_transaction_amount"),
            F.avg("fraud_rule_score").alias("avg_rule_fraud_score"),
            F.sum("fraud_label").alias("confirmed_fraud_count"),
            F.sum("fraud_ml_prediction").alias("ml_predicted_fraud_count"),
        )
    )

    fraud_risk_summary = (
        transactions_scored
        .groupBy("fraud_rule_band", "risk_tier", "merchant_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("transaction_amount").alias("total_transaction_amount"),
            F.sum("fraud_label").alias("confirmed_fraud_count"),
            F.avg("fraud_ml_probability").alias("avg_ml_fraud_probability"),
            F.avg("fraud_rule_score").alias("avg_rule_score"),
        )
    )

    product_holding_summary = (
        customer_360
        .groupBy("customer_segment", "risk_tier")
        .agg(
            F.countDistinct("customer_id").alias("customer_count"),
            F.avg("total_products").alias("avg_products_per_customer"),
            F.avg("total_account_balance").alias("avg_account_balance"),
            F.avg("total_loan_balance").alias("avg_loan_balance"),
        )
    )

    branch_performance = (
        silver["accounts"]
        .join(silver["branches"], "branch_id", "left")
        .groupBy("branch_id", "branch_name", "city", "province")
        .agg(
            F.countDistinct("customer_id").alias("customer_count"),
            F.countDistinct("account_id").alias("account_count"),
            F.sum("account_balance").alias("total_deposit_balance"),
        )
    )

    gold_outputs = {
        "transactions_scored": transactions_scored,
        "customer_360": customer_360,
        "daily_transaction_summary": daily_transaction_summary,
        "fraud_risk_summary": fraud_risk_summary,
        "product_holding_summary": product_holding_summary,
        "branch_performance": branch_performance,
    }

    marts = {
        "mart_customer_360": customer_360,
        "mart_fraud_analytics": fraud_risk_summary,
        "mart_product_holding": product_holding_summary,
        "mart_branch_performance": branch_performance,
    }

    return write_gold_outputs(gold_outputs, marts, gold_dir, mart_dir, gold_schemas)
