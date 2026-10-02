"""
src/ingestion/ingest.py

The orchestrator: wires together download.py, schema.py, and
metadata.py into a single ingest(year, month) call. This is the
function Airflow will eventually call as one task.

Flow:
    1. Check metadata log -> already successfully ingested? Skip.
    2. Download fresh into raw/ (always overwrites, per project design).
    3. Validate schema (lightweight, footer-only check).
    4. If valid -> MOVE the file from raw/ into bronze/, partitioned by
       year/month. (Move, not copy: no reason to keep a duplicate
       multi-hundred-MB file sitting in raw/ once it's trusted.)
    5. If invalid -> leave the file in raw/ untouched. It provides a
       one-cycle debugging window and will be naturally overwritten by
       the next attempt's fresh download (no explicit cleanup needed).
    6. Record the outcome in the metadata log either way.
    7. Log every step for observability.

Deliberately NOT handled here: Spark transformation, Postgres loading,
dbt, Airflow. This function's only responsibility is "get one month
of raw data safely and trustworthily into Bronze." Everything after
Bronze is a separate stage, called separately.
"""

import logging
import shutil
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from src.ingestion.download import download_file, DEFAULT_RAW_DIR
from src.ingestion.metadata import is_ingested, record_attempt, DEFAULT_METADATA_PATH
from src.quality.schema import validate_schema


logger = logging.getLogger("ingestion")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


DEFAULT_BRONZE_DIR = Path("data/bronze/taxi")


@dataclass
class IngestResult:
    year: int
    month: int
    status: str  # "skipped", "success", "failed"
    row_count: Optional[int] = None
    bronze_path: Optional[Path] = None
    error: Optional[str] = None


def _bronze_partition_path(year: int, month: int, bronze_dir: Path) -> Path:
    """
    Build the partitioned Bronze destination path, e.g.:
    data/bronze/taxi/year=2026/month=06/yellow_tripdata_2026-06.parquet
    """
    partition_dir = bronze_dir / f"year={year:04d}" / f"month={month:02d}"
    return partition_dir / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def ingest(
    year: int,
    month: int,
    raw_dir: Path = DEFAULT_RAW_DIR,
    bronze_dir: Path = DEFAULT_BRONZE_DIR,
    metadata_path: Path = DEFAULT_METADATA_PATH,
) -> IngestResult:
    """
    Ingest one month of Yellow Taxi trip data: download, validate,
    promote to Bronze, and record the outcome. Idempotent: calling
    this again for an already-successfully-ingested month is a no-op.
    """
    logger.info(f"Starting ingestion for {year:04d}-{month:02d}")

    # Step 1 — idempotency check
    if is_ingested(year, month, metadata_path):
        logger.info(f"{year:04d}-{month:02d} already ingested. Skipping.")
        return IngestResult(year=year, month=month, status="skipped")

    # Step 2 — download fresh into raw/
    logger.info(f"Downloading {year:04d}-{month:02d} from TLC...")
    download_result = download_file(year, month, raw_dir=raw_dir)

    if not download_result.success:
        logger.error(f"Download failed for {year:04d}-{month:02d}: {download_result.error}")
        record_attempt(year, month, status="failed", error=download_result.error, path=metadata_path)
        return IngestResult(year=year, month=month, status="failed", error=download_result.error)

    logger.info(f"Download complete: {download_result.file_path}")

    # Step 3 — validate schema
    logger.info(f"Validating schema for {download_result.file_path}...")
    validation_result = validate_schema(download_result.file_path)

    if not validation_result.success:
        logger.error(
            f"Validation failed for {year:04d}-{month:02d}: {validation_result.error}. "
            f"File left in {raw_dir} for inspection; will be overwritten on next attempt."
        )
        record_attempt(
            year, month,
            status="failed",
            row_count=validation_result.row_count,
            error=validation_result.error,
            path=metadata_path,
        )
        return IngestResult(
            year=year, month=month, status="failed", error=validation_result.error
        )

    logger.info(
        f"Validation passed: {validation_result.row_count} rows, "
        f"extra columns: {validation_result.extra_columns or 'none'}"
    )

    # Step 4 — promote to Bronze (move, not copy)
    bronze_path = _bronze_partition_path(year, month, bronze_dir)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(download_result.file_path), str(bronze_path))
    logger.info(f"Promoted to Bronze: {bronze_path}")

    # Step 5 — record success
    record_attempt(
        year, month,
        status="success",
        row_count=validation_result.row_count,
        path=metadata_path,
    )

    logger.info(f"Ingestion complete for {year:04d}-{month:02d}: {validation_result.row_count} rows")

    return IngestResult(
        year=year,
        month=month,
        status="success",
        row_count=validation_result.row_count,
        bronze_path=bronze_path,
    )