import os
from pathlib import Path
from pyspark.sql import SparkSession


def configure_java(java_home):
    java_home_path = Path(java_home).expanduser().resolve()
    if java_home_path.exists():
        os.environ["JAVA_HOME"] = str(java_home_path)
        java_bin = str(java_home_path / "bin")
        current_path = os.environ.get("PATH", "")
        if java_bin not in current_path.split(os.pathsep):
            os.environ["PATH"] = java_bin + os.pathsep + current_path


def get_spark(app_name, spark_master, shuffle_partitions, arrow_enabled, java_home):
    configure_java(java_home)
    try:
        return (
            SparkSession.builder
            .appName(app_name)
            .master(spark_master)
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.sql.execution.arrow.pyspark.enabled", arrow_enabled)
            .getOrCreate()
        )
    except Exception as exc:
        message = str(exc)
        if "JAVA_GATEWAY_EXITED" in message or "Java Runtime" in message:
            raise RuntimeError(
                f"PySpark requires a local Java runtime. Tried JAVA_HOME={os.environ.get('JAVA_HOME')}. "
                "Install Java/OpenJDK or set JAVA_HOME, then rerun `python src/run_pipeline.py`."
            ) from exc
        raise
