from datetime import datetime
from pyspark.sql import functions as F


def check_not_null(df, cols):
    return {col_name: df.filter(F.col(col_name).isNull()).count() for col_name in cols}


def check_duplicates(df, key_cols):
    return (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def filter_valid_transactions(transactions_df):
    return transactions_df.filter(
        (F.col("transaction_amount") > 0)
        & F.col("customer_id").isNotNull()
        & F.col("transaction_id").isNotNull()
    )


def dq_summary(df, table_name, key_cols, not_null_cols=None, layer=None):
    not_null_cols = not_null_cols or []
    null_counts = check_not_null(df, not_null_cols)
    duplicate_key_count = check_duplicates(df, key_cols) if key_cols else 0
    failed_null_cols = [col_name for col_name, count in null_counts.items() if count > 0]
    status = "FAIL" if duplicate_key_count > 0 or failed_null_cols else "PASS"

    return {
        "dq_ts": datetime.utcnow().isoformat(),
        "layer": layer,
        "table_name": table_name,
        "row_count": df.count(),
        "key_cols": ",".join(key_cols),
        "not_null_cols": ",".join(not_null_cols),
        "duplicate_key_count": int(duplicate_key_count),
        "null_check_failures": ",".join(failed_null_cols),
        "status": status,
    }


def validate_layer(spark, layer, tables, layer_dq_rules, dq_dir, fail_on_error=True, write_report=True):
    rules = layer_dq_rules.get(layer, {})
    results = []

    for table_name, df in tables.items():
        rule = rules.get(table_name)
        if not rule:
            continue
        results.append(
            dq_summary(
                df=df,
                table_name=table_name,
                key_cols=rule.get("key_cols", []),
                not_null_cols=rule.get("not_null_cols", []),
                layer=layer,
            )
        )

    if not results:
        return []

    if write_report:
        dq_dir.mkdir(parents=True, exist_ok=True)
        spark.createDataFrame(results).coalesce(1).write.mode("append").json(
            str(dq_dir / "pipeline_data_quality")
        )

    failures = [result for result in results if result["status"] != "PASS"]
    if failures and fail_on_error:
        failure_text = "; ".join(
            f"{item['layer']}.{item['table_name']} duplicates={item['duplicate_key_count']} null_failures={item['null_check_failures']}"
            for item in failures
        )
        raise ValueError(f"Data quality validation failed: {failure_text}")

    return results
