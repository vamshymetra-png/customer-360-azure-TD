from pyspark.sql import Window
from pyspark.sql import functions as F

def apply_customer_contact_scd1(customers_df):
    contact_cols = [
        "customer_id", "email", "phone_number", "address",
        "city", "province", "postal_code", "bronze_load_ts"
    ]

    window_spec = Window.partitionBy("customer_id").orderBy(F.col("bronze_load_ts").desc())

    return (
        customers_df.select(*contact_cols)
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("scd_type", F.lit("SCD1"))
        .withColumn("current_record_flag", F.lit(1))
    )
