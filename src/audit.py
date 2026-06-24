from datetime import datetime
from pyspark.sql import Row


def write_audit_record(spark, audit_dir, layer, table_name, record_count, status, message):
    audit_dir.mkdir(parents=True, exist_ok=True)

    row = Row(
        audit_ts=datetime.utcnow().isoformat(),
        layer=layer,
        table_name=table_name,
        record_count=int(record_count),
        status=status,
        message=message,
    )

    spark.createDataFrame([row]).coalesce(1).write.mode("append").json(
        str(audit_dir / "pipeline_audit")
    )
