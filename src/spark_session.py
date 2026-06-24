from pyspark.sql import SparkSession
from config import APP_NAME, SPARK_ARROW_ENABLED, SPARK_MASTER, SPARK_SHUFFLE_PARTITIONS


def get_spark():
    try:
        return (
            SparkSession.builder
            .appName(APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
            .config("spark.sql.execution.arrow.pyspark.enabled", SPARK_ARROW_ENABLED)
            .getOrCreate()
        )
    except Exception as exc:
        message = str(exc)
        if "JAVA_GATEWAY_EXITED" in message or "Java Runtime" in message:
            raise RuntimeError(
                "PySpark requires a local Java runtime. Install Java/OpenJDK and rerun `python src/run_pipeline.py`."
            ) from exc
        raise
