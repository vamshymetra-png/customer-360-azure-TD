from datetime import datetime
from pyspark.sql import Row
from config import AUDIT_DIR

def write_audit_record(spark, layer, table_name, record_count, status, message):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    row = Row(
        audit_ts=datetime.utcnow().isoformat(),
        layer=layer,
        table_name=table_name,
        record_count=int(record_count),
        status=status,
        message=message
    )

    spark.createDataFrame([row]).coalesce(1).write.mode("append").json(
        str(AUDIT_DIR / "pipeline_audit")
    )
