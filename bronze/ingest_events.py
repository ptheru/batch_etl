import os
import uuid
from pyspark.sql.functions import col, to_timestamp, lit, date_format
from src.configs.paths import Paths
from src.configs.spark import build_spark
from src.configs.dq import check_not_null, check_unique, enforce_or_fail
from src.configs.log import get_logger

logger = get_logger("bronze.ingest_events")

def main():
    run_id = os.getenv("RUN_ID", str(uuid.uuid4()))
    p = Paths()
    spark = build_spark("batch_bronze_ingest")

    raw_path = p.raw_events
    bronze_path = p.bronze_events

    logger.info(f"Reading raw jsonl from: {raw_path}")
    df = spark.read.json(raw_path)

    # Normalize timestamps
    df = (
        df.withColumn("event_ts", to_timestamp(col("event_ts")))
          .withColumn("ingest_ts", to_timestamp(col("ingest_ts")))
          .withColumn("event_date", date_format(col("event_ts"), "yyyy-MM-dd"))
          .withColumn("run_id", lit(run_id))
    )

    # DQ checks 
    dq = [
        check_not_null(df, ["event_id", "user_id", "event_type", "event_ts"], "bronze_not_null_core_fields"),
        check_unique(df, ["event_id"], "bronze_event_id_unique"),
    ]
    enforce_or_fail(dq)

    row_count = df.count()
    logger.info(f"Bronze rows: {row_count}")

    # Partitioning justification:
    # - event_date is the most common filter for batch 
    # - keeps partitions stable and prevents tiny files per user/session
    
    # ---- File sizing target ----
    # Target ~256MB per output file (tune 128–512MB)
    TARGET_FILE_MB = int(os.getenv("TARGET_FILE_MB", "256"))

    # Rough total input size assumption (50GB here)
    TOTAL_INPUT_GB = float(os.getenv("INPUT_GB", "50"))
    total_partitions = max(32, int((TOTAL_INPUT_GB * 1024) / TARGET_FILE_MB))  

    logger.info(f"Repartitioning for write: total_partitions={total_partitions} by event_date")
    df_to_write = df.repartition(total_partitions, col("event_date"))

    df_to_write.write.mode("overwrite").partitionBy("event_date").parquet(bronze_path)

    spark.stop()
    logger.info("Bronze ingest complete.")

if __name__ == "__main__":
    main()
