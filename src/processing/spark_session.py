"""
src/processing/spark_session.py

Shared factory for creating a SparkSession, used by every Spark job
in this project. Centralizing this in one place means every job gets
the same configuration, and if we ever need to tune something (memory,
partitions, timezone), it's changed in exactly one place.

Configuration choices explained:

- master("local[*]") : runs Spark locally using all available CPU
  cores. This project doesn't have a Spark cluster — the point here is
  to demonstrate Spark's DataFrame API and distributed-processing
  PATTERNS (lazy evaluation, partitioning, transformations), not to
  actually operate at cluster scale. local[*] is the honest, correct
  choice for a student project and is easy to explain as such in an
  interview.

- spark.sql.session.timeZone("UTC") : TLC timestamps have no explicit
  timezone info in the raw files. Without pinning this, Spark uses the
  JVM's local timezone to interpret/display timestamps, which means
  the SAME data could show different hour values depending on which
  machine runs the job. Pinning to UTC makes pickup_hour/derived fields
  reproducible regardless of where the pipeline runs.

- spark.sql.shuffle.partitions(8) : Spark's default is 200 shuffle
  partitions, tuned for genuine cluster workloads. On a single local
  machine with a few million rows, 200 tiny partitions just adds
  scheduling overhead. A small number is more appropriate here. (This
  would be revisited if the project ever moved to a real cluster.)

- spark.driver.memory("4g") : generous default for a laptop/desktop
  processing a few million rows. Adjust down if your machine has less
  RAM available; Spark will fail fast with a clear OOM error if it's
  set too high for your hardware rather than silently misbehaving.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "nyc-taxi-pipeline") -> SparkSession:
    """
    Create (or retrieve, if one already exists in this process) a
    SparkSession configured for local development.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # Reduce log noise: Spark's default INFO logging is very chatty and
    # drowns out our own pipeline logs. WARN still surfaces real problems.
    spark.sparkContext.setLogLevel("WARN")

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Cleanly shut down a SparkSession. Should be called at the end of
    every Spark job (ideally in a try/finally) to release resources.
    """
    spark.stop()