"""
src/processing/clean_trips.py

Bronze -> Silver transformation for NYC Yellow Taxi trip data.

Design, grounded in docs/data_dictionary.md findings:

1. INVALID records (business-rule violations that make a row nonsensical)
   are removed from the main dataset and written to quarantine/ with a
   reason attached. They are never silently dropped without a trace.

   Rules (confirmed against real data during Phase 1 exploration):
   - fare_amount < 0
   - total_amount < 0
   - dropoff_datetime < pickup_datetime
   - pickup_datetime falls outside a plausible window around the
     month being processed (catches anomalies like the 2008 pickup
     timestamp found in the real file, without hardcoding a specific
     year — this makes the rule reusable for any month processed later)

2. SUSPICIOUS records (unusual but not impossible) stay in Silver, but
   get an `is_suspicious` flag column so downstream consumers (dbt,
   Power BI) can choose to include or exclude them, rather than having
   that decision made silently in this layer.

   Heuristics (documented, adjustable):
   - trip_distance > 100 miles
   - fare_amount > $500
   - passenger_count == 0 (NOT null — null passenger_count is expected
     and valid for Flex Fare trips, see point 3)

3. The Flex Fare null cluster (passenger_count, RatecodeID,
   store_and_fwd_flag, congestion_surcharge, Airport_fee all null when
   payment_type == 0) is preserved as TRUE NULLS. This is a confirmed
   source-system behavior, not a data quality defect, so we do not
   fill, flag, or otherwise treat these nulls as suspicious.

4. Column normalization: `Airport_fee` (inconsistent capital-A casing
   in the source) is renamed to `airport_fee` for consistency with
   every other column.

5. Derived fields added: trip_duration_minutes, pickup_date,
   pickup_hour, pickup_weekday, pickup_month.
"""

import logging
from pathlib import Path
from dataclasses import dataclass

from pyspark.sql import functions as F
from src.processing.spark_session import get_spark_session, stop_spark_session


logger = logging.getLogger("processing")


DEFAULT_BRONZE_DIR = Path("data/bronze/taxi")
DEFAULT_SILVER_DIR = Path("data/silver/taxi")
DEFAULT_QUARANTINE_DIR = Path("data/quarantine/taxi")

# Suspicious-record heuristics. Documented assumptions, not hard facts —
# adjust here if analysis later suggests better thresholds.
SUSPICIOUS_TRIP_DISTANCE_THRESHOLD = 100.0   # miles
SUSPICIOUS_FARE_AMOUNT_THRESHOLD = 500.0     # dollars


@dataclass
class CleaningResult:
    year: int
    month: int
    input_rows: int
    valid_rows: int
    quarantined_rows: int
    suspicious_rows: int
    silver_path: Path
    quarantine_path: Path


def _bronze_path(year: int, month: int, bronze_dir: Path) -> Path:
    return bronze_dir / f"year={year:04d}" / f"month={month:02d}"


def _silver_path(year: int, month: int, silver_dir: Path) -> Path:
    return silver_dir / f"year={year:04d}" / f"month={month:02d}"


def _quarantine_path(year: int, month: int, quarantine_dir: Path) -> Path:
    return quarantine_dir / f"year={year:04d}" / f"month={month:02d}"


def clean_trips(
    year: int,
    month: int,
    bronze_dir: Path = DEFAULT_BRONZE_DIR,
    silver_dir: Path = DEFAULT_SILVER_DIR,
    quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
) -> CleaningResult:
    """
    Read one month of Bronze data, apply validation + transformation
    rules, and write the results to Silver (valid) and quarantine
    (invalid) partitions.
    """
    spark = get_spark_session("clean-trips")

    try:
        bronze_path = _bronze_path(year, month, bronze_dir)
        logger.info(f"Reading Bronze data from {bronze_path}")
        df = spark.read.parquet(str(bronze_path))

        input_rows = df.count()
        logger.info(f"Input rows: {input_rows}")

        # --- Normalize column naming ---
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

        # --- Derived fields ---
        df = df.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        )
        df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        df = df.withColumn("pickup_weekday", F.dayofweek("tpep_pickup_datetime"))
        df = df.withColumn("pickup_month", F.month("tpep_pickup_datetime"))

        # --- Plausible date window for this specific month being processed ---
        # A trip can start a day or two before the 1st (rare, but a dropoff
        # just after midnight shouldn't be falsely rejected) and a dropoff
        # can land a few days into the next month. This is intentionally a
        # bit generous around the target month, not a hardcoded year.
        window_start = F.make_date(F.lit(year), F.lit(month), F.lit(1)) - F.expr("INTERVAL 3 DAYS")
        window_end = (
            F.add_months(F.make_date(F.lit(year), F.lit(month), F.lit(1)), 1)
            + F.expr("INTERVAL 3 DAYS")
        )
        pickup_in_plausible_window = (
            (F.col("tpep_pickup_datetime") >= window_start)
            & (F.col("tpep_pickup_datetime") < window_end)
        )

        # --- Invalid record conditions (-> quarantine) ---
        # NOTE on nulls: passenger_count is legitimately NULL for Flex Fare
        # trips (payment_type == 0). In SQL's three-valued logic,
        # `NULL < 0` evaluates to NULL (not False), and NULL OR False OR
        # False... also evaluates to NULL rather than False. A filter on
        # a NULL condition silently DROPS the row entirely (neither
        # matched nor unmatched) -- which would make Flex Fare rows
        # vanish from BOTH Silver and quarantine without a trace. We wrap
        # this comparison in coalesce(..., False) so a null passenger_count
        # is explicitly treated as "not violating this rule", matching
        # our decision to preserve Flex Fare nulls as valid, expected data.
        invalid_condition = (
            (F.col("fare_amount") < 0)
            | (F.col("total_amount") < 0)
            | (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"))
            | (~pickup_in_plausible_window)
            | F.coalesce(F.col("passenger_count") < 0, F.lit(False))
        )

        # Build a human-readable reason string listing every rule a
        # rejected row violated (a row can fail more than one rule).
        reason_array = F.array(
            F.when(F.col("fare_amount") < 0, F.lit("negative fare_amount")),
            F.when(F.col("total_amount") < 0, F.lit("negative total_amount")),
            F.when(
                F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
                F.lit("dropoff before pickup"),
            ),
            F.when(~pickup_in_plausible_window, F.lit("pickup_datetime outside plausible month window")),
            F.when(F.col("passenger_count") < 0, F.lit("negative passenger_count")),
        )
        # array contains nulls for rules that didn't fire; filter them out
        reason_array_clean = F.filter(reason_array, lambda x: x.isNotNull())

        # --- Suspicious record conditions (stay in Silver, flagged) ---
        # Same null-handling note as above: a null passenger_count must
        # coalesce to False here, or is_suspicious itself would be
        # stored as NULL (instead of False) for Flex Fare rows.
        suspicious_condition = (
            (F.col("trip_distance") > SUSPICIOUS_TRIP_DISTANCE_THRESHOLD)
            | (F.col("fare_amount") > SUSPICIOUS_FARE_AMOUNT_THRESHOLD)
            | F.coalesce(F.col("passenger_count") == 0, F.lit(False))  # null (Flex Fare) is NOT suspicious
        )

        # --- Split ---
        quarantine_df = (
            df.filter(invalid_condition)
            .withColumn("rejection_reason", F.concat_ws(", ", reason_array_clean))
            .withColumn("pipeline_timestamp", F.current_timestamp())
        )

        valid_df = (
            df.filter(~invalid_condition)
            .withColumn("is_suspicious", suspicious_condition)
        )

        valid_rows = valid_df.count()
        quarantined_rows = quarantine_df.count()
        suspicious_rows = valid_df.filter(F.col("is_suspicious")).count()

        logger.info(
            f"Valid: {valid_rows} | Quarantined: {quarantined_rows} | "
            f"Suspicious (within valid): {suspicious_rows}"
        )

        # --- Write outputs ---
        silver_out = _silver_path(year, month, silver_dir)
        quarantine_out = _quarantine_path(year, month, quarantine_dir)

        silver_out.parent.mkdir(parents=True, exist_ok=True)
        quarantine_out.parent.mkdir(parents=True, exist_ok=True)

        valid_df.write.mode("overwrite").parquet(str(silver_out))
        quarantine_df.write.mode("overwrite").parquet(str(quarantine_out))

        logger.info(f"Silver written to {silver_out}")
        logger.info(f"Quarantine written to {quarantine_out}")

        return CleaningResult(
            year=year,
            month=month,
            input_rows=input_rows,
            valid_rows=valid_rows,
            quarantined_rows=quarantined_rows,
            suspicious_rows=suspicious_rows,
            silver_path=silver_out,
            quarantine_path=quarantine_out,
        )

    finally:
        stop_spark_session(spark)