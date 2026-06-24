from pyspark.sql import functions as F
from config import BRONZE_DIR, RAW_DIR, RAW_TABLES, SOURCE_SYSTEMS
from schemas import RAW_SCHEMAS, enforce_schema


def load_csv(spark, path, schema):
    return spark.read.option("header", True).schema(schema).csv(str(path))


def add_bronze_columns(df, source_system):
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("bronze_load_ts", F.current_timestamp())
        .withColumn("ingestion_mode", F.lit("ADF_COPY_TO_ADLS_RAW"))
    )


def run_bronze(spark):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    bronze = {}
    for name, table_config in RAW_TABLES.items():
        schema = RAW_SCHEMAS[name]
        path = RAW_DIR / table_config["file_name"]
        df = enforce_schema(load_csv(spark, path, schema), schema)
        df = add_bronze_columns(df, SOURCE_SYSTEMS[name])
        df.write.mode("overwrite").parquet(str(BRONZE_DIR / name))
        bronze[name] = df

    return bronze
