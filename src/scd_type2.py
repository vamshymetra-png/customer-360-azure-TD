from pyspark.sql import functions as F

def apply_customer_risk_scd2(customers_df):
    tracked_cols = [
        "risk_tier",
        "customer_segment",
        "employment_status",
        "income_band",
        "credit_score_band"
    ]

    hash_expr = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]),
        256
    )

    return (
        customers_df
        .select(
            "customer_id",
            "risk_tier",
            "customer_segment",
            "employment_status",
            "income_band",
            "credit_score",
            "credit_score_band",
            "created_date"
        )
        .withColumn("record_hash", hash_expr)
        .withColumn("effective_start_date", F.col("created_date"))
        .withColumn("effective_end_date", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(1))
        .withColumn("scd_type", F.lit("SCD2"))
    )
