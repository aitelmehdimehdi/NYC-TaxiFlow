"""
NYC Taxi Data Engineering Pipeline DAG

Orchestrates the complete pipeline:
  1. Ingest raw data from TLC (download + validate + promote to Bronze)
  2. Transform with PySpark (Bronze -> Silver + quarantine)
  3. Load Silver into PostgreSQL staging
  4. dbt seed (zone reference data)
  5. dbt run (staging -> intermediate -> marts / star schema)
  6. dbt test (quality checks)

Parameters (passed via dag_run.conf at trigger time):
  - year: e.g. 2026
  - month: 1-12

Example trigger from the CLI:
  airflow dags trigger taxi_pipeline --conf '{"year": 2026, "month": 6}'

Design notes:
- There is NO separate "validate_raw" task. ingest() already validates
  the downloaded file's schema BEFORE promoting it to Bronze (see
  src/ingestion/ingest.py); a file that fails validation never reaches
  Bronze and ingest() returns status="failed". Re-validating afterward
  would be redundant.
- ingest() does not raise exceptions for ordinary failures (a bad
  download, a schema mismatch) -- it returns an IngestResult with
  status="failed" and an error message instead. Every task below
  explicitly checks `status` and raises AirflowException itself; this
  is what actually fails the Airflow task on a genuine problem.
- year/month are read directly from dag_run.conf inside each task
  function (with an explicit int() cast), rather than passed through
  Jinja-templated op_kwargs. This sidesteps a subtle Airflow pitfall:
  templated op_kwargs can render as strings even with a `| int` Jinja
  filter, which would silently break every f"{year:04d}"-style format
  string in our pipeline code.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from src.ingestion.ingest import ingest
from src.processing.clean_trips import clean_trips
from src.warehouse.load import load_month_to_staging


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze" / "taxi"
SILVER_DIR = DATA_DIR / "silver" / "taxi"
QUARANTINE_DIR = DATA_DIR / "quarantine" / "taxi"
METADATA_PATH = BRONZE_DIR / "_ingestion_metadata.json"

DBT_PROJECT_DIR = "/opt/airflow/dbt/taxi_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt/taxi_analytics"  # profiles.yml lives alongside the project

logger = logging.getLogger(__name__)


def _get_year_month(context) -> tuple[int, int]:
    """Read year/month from dag_run.conf, with explicit int casting."""
    conf = context["dag_run"].conf or {}
    if "year" not in conf or "month" not in conf:
        raise AirflowException(
            "dag_run.conf must include 'year' and 'month', e.g. "
            '{"year": 2026, "month": 6}'
        )
    return int(conf["year"]), int(conf["month"])


# ─────────────────────────────────────────────────────────────────────
# Task Functions
# ─────────────────────────────────────────────────────────────────────


def task_ingest(**context):
    """Download, validate, and promote one month to Bronze."""
    year, month = _get_year_month(context)
    logger.info(f"Starting ingestion for {year:04d}-{month:02d}")

    result = ingest(
        year=year,
        month=month,
        raw_dir=RAW_DIR,
        bronze_dir=BRONZE_DIR,
        metadata_path=METADATA_PATH,
    )

    if result.status == "failed":
        raise AirflowException(f"Ingestion failed for {year:04d}-{month:02d}: {result.error}")

    logger.info(
        f"Ingestion {result.status} for {year:04d}-{month:02d}: "
        f"row_count={result.row_count}, bronze_path={result.bronze_path}"
    )


def task_transform_spark(**context):
    """Bronze -> Silver + quarantine via PySpark."""
    year, month = _get_year_month(context)
    logger.info(f"Starting Spark transformation for {year:04d}-{month:02d}")

    result = clean_trips(
        year=year,
        month=month,
        bronze_dir=BRONZE_DIR,
        silver_dir=SILVER_DIR,
        quarantine_dir=QUARANTINE_DIR,
    )

    logger.info(
        f"Transformation complete for {year:04d}-{month:02d}: "
        f"input={result.input_rows}, valid={result.valid_rows}, "
        f"quarantined={result.quarantined_rows}, suspicious={result.suspicious_rows}"
    )


def task_load_warehouse(**context):
    """Load Silver into PostgreSQL staging.stg_taxi_trips."""
    year, month = _get_year_month(context)
    logger.info(f"Starting warehouse load for {year:04d}-{month:02d}")

    result = load_month_to_staging(
        year=year,
        month=month,
        silver_dir=SILVER_DIR,
    )

    logger.info(f"Loaded {result.rows_loaded} rows for {year:04d}-{month:02d}")


def _run_dbt_command(command: list[str]) -> None:
    """Shared helper for dbt seed/run/test subprocess calls."""
    dbt_env = os.environ.copy()
    full_command = command + [
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    result = subprocess.run(
        full_command,
        env=dbt_env,
        capture_output=True,
        text=True,
        timeout=600,
    )

    # dbt writes most of its meaningful output (including test failure
    # summaries) to stdout, not stderr -- include both so a failure is
    # actually diagnosable from the Airflow task log.
    logger.info(f"dbt stdout:\n{result.stdout}")
    if result.stderr:
        logger.info(f"dbt stderr:\n{result.stderr}")

    if result.returncode != 0:
        raise AirflowException(
            f"Command failed: {' '.join(full_command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def task_dbt_seed(**context):
    """Load the taxi zone reference data (static, rarely changes)."""
    _run_dbt_command(["dbt", "seed"])


def task_dbt_run(**context):
    """Build staging -> intermediate -> marts (the star schema)."""
    _run_dbt_command(["dbt", "run"])


def task_dbt_test(**context):
    """Run all dbt tests against the freshly-built models."""
    _run_dbt_command(["dbt", "test"])


# ─────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="taxi_pipeline",
    default_args=DEFAULT_ARGS,
    description="NYC Taxi end-to-end data pipeline (Bronze -> Silver -> Gold)",
    schedule=None,  # manually triggered with {"year": ..., "month": ...}
    catchup=False,
    tags=["taxi", "data-engineering"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        doc="Download, validate, and promote one month to Bronze",
    )

    transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=task_transform_spark,
        doc="Bronze -> Silver (PySpark cleaning, quarantine invalid rows)",
    )

    load_task = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load_warehouse,
        doc="Load Silver into PostgreSQL staging.stg_taxi_trips",
    )

    dbt_seed_task = PythonOperator(
        task_id="dbt_seed",
        python_callable=task_dbt_seed,
        doc="Load static zone reference data",
    )

    dbt_run_task = PythonOperator(
        task_id="dbt_run",
        python_callable=task_dbt_run,
        doc="Build staging/intermediate/marts (star schema)",
    )

    dbt_test_task = PythonOperator(
        task_id="dbt_test",
        python_callable=task_dbt_test,
        doc="Run dbt tests on the Gold/analytics models",
    )

    ingest_task >> transform_task >> load_task >> dbt_seed_task >> dbt_run_task >> dbt_test_task