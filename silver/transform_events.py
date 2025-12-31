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
    run_id = os.getenv("RUN_ID", str(uuid.uuid4()))
    p = Paths()
    spark = build_spark("batch_silver_transform")

    bronze_path = p.bronze_events
    silver_path = p.silver_events

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

    # Keep same partitioning for downstream joins and pruning
    df2.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)

    spark.stop()
    logger.info("Silver transform complete.")

def upper_or_null(c):
    from pyspark.sql.functions import upper
    return upper(c)

if __name__ == "__main__":
    main()
