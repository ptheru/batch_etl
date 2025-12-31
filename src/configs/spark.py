from pyspark.sql import SparkSession

def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
            .appName(app_name)

            # ---- Cluster sizing assumptions ----
            # Assume 32 total cores available to this Spark application.
            # Choose ~4 cores/executor => ~8 executors (roughly 32/4).
            # These are usually set at submit-time / cluster manager level:
            # .config("spark.executor.instances", "8")
            # .config("spark.executor.cores", "4")
            # .config("spark.executor.memory", "12g")

            # ---- Shuffle / AQE ----
            # Rule of thumb: shuffle partitions ~ 2x to 3x total cores.
            # For 32 cores => ~64 to 96 shuffle partitions.
            .config("spark.sql.shuffle.partitions", "64")
            .config("spark.sql.adaptive.enabled", "true")
            
            # ---- Input file sizing ----
            # Control how Spark splits input files into partitions.
            # Example: ~50GB input, aiming for ~512MB splits => ~100 partitions.
            .config("spark.sql.files.maxPartitionBytes", 512 * 1024 * 1024)

            # ---- Broadcast threshold (optional) ----
            # If a dimension is <= 50MB, Spark may broadcast it for joins.
            .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)

            .getOrCreate()
    )
    return spark
