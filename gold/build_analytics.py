import os
import uuid
from pyspark.sql.functions import col, count, sum as fsum, to_date
from src.configs.paths import Paths
from src.configs.spark import build_spark
from src.configs.dq import check_not_null, enforce_or_fail
from src.configs.log import get_logger

logger = get_logger("gold.build_analytics")

def main():

    p = Paths()
    spark = build_spark("batch_gold_build")

    silver_path = p.silver_events
    gold_path = p.gold_daily_metrics

    process_date = os.getenv("PROCESS_DATE")  # YYYY-MM-DD
    if process_date:
    # because bronze is partitioned by event_date=yyyy-mm-dd
        df = spark.read.parquet(f"{silver_path}/dt={process_date}")
    else:
        df = spark.read.parquet(silver_path)

    daily = (
        df.withColumn("event_day", to_date(col("event_ts")))
          .groupBy("event_day", "country", "platform")
          .agg(
              count("*").alias("events"),
              count("user_id").alias("rows_with_user_id"),
              fsum((col("event_type") == "purchase").cast("int")).alias("purchases"),
              fsum(col("value")).alias("revenue")
          )
          .withColumnRenamed("event_day", "event_date")
    )

    dq = [check_not_null(daily, ["event_date", "country", "platform", "events"], "gold_not_null")]
    enforce_or_fail(dq)

    row_count = daily.count()
    logger.info(f"Gold daily_metrics rows: {row_count}")

    # Partition by event_date for dashboard reads + incremental refresh
    daily.coalesce(1).write.mode("overwrite").partitionBy("event_date").parquet(gold_path)


    spark.stop()
    logger.info("Gold build complete.")

if __name__ == "__main__":
    main()
