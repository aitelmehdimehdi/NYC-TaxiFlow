"""
src/warehouse/load.py

Loads one month of Silver data into Postgres, table
staging.stg_taxi_trips.

Design:
- ONE staging table holds every month loaded so far (not one table per
  month), with explicit `batch_year` / `batch_month` columns identifying
  which ingestion batch each row belongs to. This matches the project's
  original design (a single staging.stg_taxi_trips table that dbt reads
  from).
- Idempotency: loading the same month twice does not duplicate rows.
  Each load runs, in a single transaction:
    1. DELETE any existing rows for this batch_year/batch_month
    2. COPY the new rows in
  This means a re-run always leaves exactly one copy of that month's
  data, matching the same idempotency philosophy used in ingestion.
- Uses COPY (via psycopg2's copy_expert), not row-by-row INSERT, since
  COPY is dramatically faster for bulk-loading millions of rows.
- Table creation is idempotent (CREATE SCHEMA/TABLE IF NOT EXISTS), so
  this module can be run against a brand new database with no manual
  setup step required beyond having Postgres itself running.

Known trade-off: this builds the full CSV payload for a month in
memory before COPYing it. For ~3.8M rows this is fine on a normal
dev machine, but if this ever needed to scale to much larger batches,
the right next step would be chunked COPY (e.g. 500k rows at a time)
instead of one giant in-memory buffer.
"""

import io
import logging
from pathlib import Path
from dataclasses import dataclass

import pandas as pd

from src.warehouse.connection import get_connection


logger = logging.getLogger("warehouse")

DEFAULT_SILVER_DIR = Path("data/silver/taxi")

CREATE_STAGING_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_taxi_trips (
    "VendorID" INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    "RatecodeID" DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    "PULocationID" INTEGER,
    "DOLocationID" INTEGER,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION,
    request_source TEXT,
    trip_duration_minutes DOUBLE PRECISION,
    pickup_date DATE,
    pickup_hour INTEGER,
    pickup_weekday INTEGER,
    pickup_month INTEGER,
    is_suspicious BOOLEAN,
    batch_year INTEGER NOT NULL,
    batch_month INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_taxi_trips_batch
    ON staging.stg_taxi_trips (batch_year, batch_month);
"""


@dataclass
class LoadResult:
    year: int
    month: int
    rows_loaded: int


def _ensure_staging_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_STAGING_TABLE_SQL)
    conn.commit()


def load_month_to_staging(
    year: int,
    month: int,
    silver_dir: Path = DEFAULT_SILVER_DIR,
) -> LoadResult:
    """
    Load one month of Silver parquet data into staging.stg_taxi_trips.
    Idempotent: re-running for the same year/month replaces that
    month's rows rather than duplicating them.
    """
    silver_path = silver_dir / f"year={year:04d}" / f"month={month:02d}"
    logger.info(f"Reading Silver data from {silver_path}")

    df = pd.read_parquet(silver_path)
    df["batch_year"] = year
    df["batch_month"] = month

    rows_to_load = len(df)
    logger.info(f"Rows to load: {rows_to_load}")

    conn = get_connection()
    try:
        _ensure_staging_table(conn)

        with conn.cursor() as cur:
            # Step 1: remove any existing rows for this batch (idempotency)
            cur.execute(
                "DELETE FROM staging.stg_taxi_trips WHERE batch_year = %s AND batch_month = %s",
                (year, month),
            )
            deleted = cur.rowcount
            logger.info(f"Deleted {deleted} existing rows for {year:04d}-{month:02d} (if any)")

            # Step 2: bulk-load via COPY
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)

            columns = ", ".join(f'"{c}"' if c[0].isupper() else c for c in df.columns)
            cur.copy_expert(
                f"COPY staging.stg_taxi_trips ({columns}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                buffer,
            )

        conn.commit()
        logger.info(f"Loaded {rows_to_load} rows into staging.stg_taxi_trips")

        return LoadResult(year=year, month=month, rows_loaded=rows_to_load)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()