"""
src/quality/schema.py

Lightweight validation gate between raw/ and bronze/.

Design notes:
- Uses pyarrow.parquet.ParquetFile, which reads only the file's footer
  (schema + row group metadata) WITHOUT loading any actual row data.
  This makes validation fast even on multi-hundred-MB files.
- This is deliberately shallow: it answers "is this a structurally
  sound file with the columns we expect?", not "is the data itself
  clean?". Deeper business-rule validation (negative fares, bad
  timestamps, etc.) happens later, in the PySpark transformation step
  (Bronze -> Silver), where the full data is being read anyway.
- Column check is intentionally lenient on EXTRA columns (TLC has added
  columns before, e.g. cbd_congestion_fee, request_source) but STRICT
  on missing expected columns, since a missing column usually signals
  either a corrupted download or an upstream schema change serious
  enough to break downstream transformations.
"""

import pyarrow.parquet as pq
from pathlib import Path
from dataclasses import dataclass, field


# Confirmed against the real 2026-06 file and docs/data_dictionary.md.
# If TLC changes the schema again, update this list AND re-run
# Phase 1 exploration on the new file before trusting it blindly.
EXPECTED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "request_source",
}


@dataclass
class SchemaValidationResult:
    success: bool
    row_count: int = 0
    missing_columns: set = field(default_factory=set)
    extra_columns: set = field(default_factory=set)
    error: str | None = None


def validate_schema(file_path: Path) -> SchemaValidationResult:
    """
    Validate a parquet file's structure without loading its row data.

    Checks:
    - file exists and is a readable parquet file
    - all EXPECTED_COLUMNS are present (missing columns = failure)
    - row count > 0 (an empty file = failure)

    Extra/unexpected columns are recorded but do NOT cause failure,
    since TLC has added new columns over time without warning and we
    don't want the pipeline to break just because of an addition.
    """
    if not file_path.exists():
        return SchemaValidationResult(
            success=False, error=f"File does not exist: {file_path}"
        )

    try:
        parquet_file = pq.ParquetFile(file_path)
    except Exception as e:
        return SchemaValidationResult(
            success=False, error=f"File is not a readable parquet file: {e}"
        )

    actual_columns = set(parquet_file.schema_arrow.names)
    row_count = parquet_file.metadata.num_rows

    missing_columns = EXPECTED_COLUMNS - actual_columns
    extra_columns = actual_columns - EXPECTED_COLUMNS

    if missing_columns:
        return SchemaValidationResult(
            success=False,
            row_count=row_count,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
            error=f"Missing expected columns: {sorted(missing_columns)}",
        )

    if row_count == 0:
        return SchemaValidationResult(
            success=False,
            row_count=0,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
            error="File has 0 rows",
        )

    return SchemaValidationResult(
        success=True,
        row_count=row_count,
        missing_columns=missing_columns,
        extra_columns=extra_columns,
    )