from pyspark.sql import functions as F
from schemas import enforce_schema


def load_csv(spark, path, schema):
    return spark.read.option("header", True).schema(schema).csv(str(path))


def add_bronze_columns(df, source_system):
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("bronze_load_ts", F.current_timestamp())
        .withColumn("ingestion_mode", F.lit("ADF_COPY_TO_ADLS_RAW"))
    )


def write_parquet_table(df, output_dir, table_name, schema):
    shaped_df = enforce_schema(df, schema)
    shaped_df.write.mode("overwrite").parquet(str(output_dir / table_name))
    return shaped_df


def run_bronze(spark, raw_dir, bronze_dir, raw_tables, raw_schemas, bronze_schemas, source_systems):
    bronze_dir.mkdir(parents=True, exist_ok=True)

    bronze = {}
    for name, table_config in raw_tables.items():
        raw_schema = raw_schemas[name]
        bronze_schema = bronze_schemas[name]
        path = raw_dir / table_config["file_name"]
        df = load_csv(spark, path, raw_schema)
        df = add_bronze_columns(df, source_systems[name])
        bronze[name] = write_parquet_table(df, bronze_dir, name, bronze_schema)

    return bronze
