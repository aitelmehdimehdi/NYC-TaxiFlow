"""
tests/test_quality.py

Tests for src/quality/schema.py.

Design note: uses pyarrow directly to build small synthetic parquet
files in tmp_path for each test. We don't use the real TLC file here —
that would make tests slow (hundreds of MB) and would only test one
scenario (the happy path). Synthetic files let us cheaply construct
every edge case: missing columns, empty files, corrupted files, etc.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from src.quality.schema import validate_schema, EXPECTED_COLUMNS


def _write_parquet(path: Path, columns: dict) -> None:
    """Helper: write a small synthetic parquet file with given columns."""
    table = pa.table(columns)
    pq.write_table(table, path)


def test_validate_schema_passes_with_full_expected_columns(tmp_path):
    file_path = tmp_path / "good.parquet"
    _write_parquet(file_path, {col: [1, 2, 3] for col in EXPECTED_COLUMNS})

    result = validate_schema(file_path)

    assert result.success is True
    assert result.row_count == 3
    assert result.missing_columns == set()


def test_validate_schema_fails_when_column_missing(tmp_path):
    file_path = tmp_path / "missing_col.parquet"
    columns = {c: [1, 2, 3] for c in EXPECTED_COLUMNS if c != "fare_amount"}
    _write_parquet(file_path, columns)

    result = validate_schema(file_path)

    assert result.success is False
    assert "fare_amount" in result.missing_columns
    assert "fare_amount" in result.error


def test_validate_schema_fails_when_multiple_columns_missing(tmp_path):
    file_path = tmp_path / "missing_multiple.parquet"
    columns = {
        c: [1, 2, 3]
        for c in EXPECTED_COLUMNS
        if c not in {"fare_amount", "tip_amount", "VendorID"}
    }
    _write_parquet(file_path, columns)

    result = validate_schema(file_path)

    assert result.success is False
    assert result.missing_columns == {"fare_amount", "tip_amount", "VendorID"}


def test_validate_schema_passes_with_unexpected_extra_column(tmp_path):
    """
    TLC has added columns before without warning (e.g. cbd_congestion_fee).
    An extra, unrecognized column should be tolerated, not fail the file.
    """
    file_path = tmp_path / "extra_col.parquet"
    columns = {c: [1, 2, 3] for c in EXPECTED_COLUMNS}
    columns["some_future_tlc_field"] = [1, 2, 3]
    _write_parquet(file_path, columns)

    result = validate_schema(file_path)

    assert result.success is True
    assert "some_future_tlc_field" in result.extra_columns


def test_validate_schema_fails_on_empty_file(tmp_path):
    file_path = tmp_path / "empty.parquet"
    _write_parquet(file_path, {col: [] for col in EXPECTED_COLUMNS})

    result = validate_schema(file_path)

    assert result.success is False
    assert result.row_count == 0
    assert "0 rows" in result.error


def test_validate_schema_fails_on_nonexistent_file(tmp_path):
    file_path = tmp_path / "does_not_exist.parquet"

    result = validate_schema(file_path)

    assert result.success is False
    assert "does not exist" in result.error


def test_validate_schema_fails_on_corrupted_file(tmp_path):
    file_path = tmp_path / "corrupt.parquet"
    file_path.write_text("this is definitely not a parquet file")

    result = validate_schema(file_path)

    assert result.success is False
    assert result.error is not None