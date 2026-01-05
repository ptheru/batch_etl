import os
import uuid
from pyspark.sql.functions import col, lower, when
from src.configs.paths import Paths
from src.configs.spark import build_spark
from src.configs.dq import check_not_null, check_allowed_values, enforce_or_fail
from src.configs.log import get_logger

logger = get_logger("silver.transform_events")

ALLOWED_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "login"]
ALLOWED_COUNTRIES = ["US", "CA", "IN", "GB"]

def main():

    p = Paths()
    spark = build_spark("batch_silver_transform")

    bronze_path = p.bronze_events
    silver_path = p.silver_events

    process_date = os.getenv("PROCESS_DATE")  # YYYY-MM-DD
    if process_date:
    # because bronze is partitioned by event_date=yyyy-mm-dd
        df = spark.read.parquet(f"{bronze_path}/dt={process_date}")
    else:
        df = spark.read.parquet(bronze_path)

    # Standardize / clean
    df2 = (
        df.withColumn("event_type", lower(col("event_type")))
          .withColumn("country", upper_or_null(col("country")))
          .withColumn("platform", lower(col("platform")))
          .withColumn("value", when(col("event_type") == "purchase", col("value")).otherwise(None))
    )

    dq = [
        check_not_null(df2, ["event_id", "user_id", "event_type", "event_ts", "event_date"], "silver_not_null"),
        check_allowed_values(df2, "event_type", ALLOWED_EVENT_TYPES, "silver_event_type_allowed"),
        check_allowed_values(df2, "country", ALLOWED_COUNTRIES, "silver_country_allowed"),
    ]
    enforce_or_fail(dq)

    row_count = df2.count()
    logger.info(f"Silver rows: {row_count}")

    # Target ~256MB per output file (tune 128–512MB)
    TARGET_FILE_MB = int(os.getenv("TARGET_FILE_MB", "256"))

    # Rough total input size assumption (50GB here)
    TOTAL_INPUT_GB = float(os.getenv("INPUT_GB", "50"))
    total_partitions = max(32, int((TOTAL_INPUT_GB * 1024) / TARGET_FILE_MB))  

    logger.info(f"Repartitioning for write: total_partitions={total_partitions} by event_date")
    df_to_write = df2.repartition(total_partitions, col("event_date"))


    df_to_write.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)

    spark.stop()
    logger.info("Silver transform complete.")

def upper_or_null(c):
    from pyspark.sql.functions import upper
    return upper(c)

if __name__ == "__main__":
    main()
