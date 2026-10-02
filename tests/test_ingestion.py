"""
tests/test_ingestion.py

Tests for src/ingestion/metadata.py and src/ingestion/download.py.

Design notes:
- Every test uses pytest's `tmp_path` fixture, which gives each test
  function its own temporary directory that's automatically cleaned up
  afterward. This guarantees tests never touch your real data/ folder
  and never interfere with each other.
- Download tests use `unittest.mock` to fake the network call entirely.
  We are NOT testing "does the internet work" or "is TLC's server up" —
  we're testing "does OUR code do the right thing given a certain
  response". Hitting the real network in a test suite makes tests slow,
  flaky (fails if TLC is down or you're offline), and non-repeatable.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.ingestion.metadata import (
    is_ingested,
    record_attempt,
    get_ingestion_history,
    load_metadata,
)
from src.ingestion.download import build_url, download_file


# ---------------------------------------------------------------------
# metadata.py tests
# ---------------------------------------------------------------------

def test_is_ingested_false_when_no_history(tmp_path):
    """A month with no recorded attempts should not be considered ingested."""
    meta_path = tmp_path / "metadata.json"
    assert is_ingested(2026, 6, meta_path) is False


def test_is_ingested_false_after_failed_attempt(tmp_path):
    """A failed attempt should NOT mark the month as ingested."""
    meta_path = tmp_path / "metadata.json"
    record_attempt(2026, 6, status="failed", error="schema mismatch", path=meta_path)
    assert is_ingested(2026, 6, meta_path) is False


def test_is_ingested_true_after_successful_attempt(tmp_path):
    """A successful attempt should mark the month as ingested."""
    meta_path = tmp_path / "metadata.json"
    record_attempt(2026, 6, status="success", row_count=100, path=meta_path)
    assert is_ingested(2026, 6, meta_path) is True


def test_is_ingested_uses_most_recent_attempt(tmp_path):
    """
    If a month failed once and then succeeded on retry, is_ingested
    should reflect the MOST RECENT attempt, not the first one.
    """
    meta_path = tmp_path / "metadata.json"
    record_attempt(2026, 6, status="failed", error="network error", path=meta_path)
    record_attempt(2026, 6, status="success", row_count=100, path=meta_path)
    assert is_ingested(2026, 6, meta_path) is True


def test_is_ingested_does_not_leak_across_months(tmp_path):
    """Recording an attempt for one month must not affect another month."""
    meta_path = tmp_path / "metadata.json"
    record_attempt(2026, 6, status="success", row_count=100, path=meta_path)
    assert is_ingested(2026, 7, meta_path) is False


def test_record_attempt_rejects_invalid_status(tmp_path):
    """Status must be exactly 'success' or 'failed' — anything else is a bug."""
    meta_path = tmp_path / "metadata.json"
    with pytest.raises(ValueError):
        record_attempt(2026, 6, status="maybe", path=meta_path)


def test_get_ingestion_history_preserves_all_attempts(tmp_path):
    """History should contain every attempt, in order, not just the latest."""
    meta_path = tmp_path / "metadata.json"
    record_attempt(2026, 6, status="failed", error="attempt 1 failed", path=meta_path)
    record_attempt(2026, 6, status="failed", error="attempt 2 failed", path=meta_path)
    record_attempt(2026, 6, status="success", row_count=100, path=meta_path)

    history = get_ingestion_history(2026, 6, meta_path)
    assert len(history) == 3
    assert history[0]["status"] == "failed"
    assert history[1]["status"] == "failed"
    assert history[2]["status"] == "success"


def test_load_metadata_returns_empty_list_when_file_missing(tmp_path):
    """First-ever run: no metadata file exists yet. Should not crash."""
    meta_path = tmp_path / "does_not_exist.json"
    assert load_metadata(meta_path) == []


# ---------------------------------------------------------------------
# download.py tests
# ---------------------------------------------------------------------

def test_build_url_pads_month_correctly():
    """Single-digit months must be zero-padded to match TLC's naming."""
    assert build_url(2026, 6) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-06.parquet"
    )


def test_build_url_double_digit_month():
    assert build_url(2024, 12) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
    )


def test_build_url_rejects_invalid_month():
    with pytest.raises(ValueError):
        build_url(2026, 13)

    with pytest.raises(ValueError):
        build_url(2026, 0)


def test_download_file_success(tmp_path):
    """
    Simulate a successful download: fake response returns some bytes,
    and we confirm the file lands on disk with that content.
    """
    fake_content = [b"fake parquet bytes " * 100]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = fake_content
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False

    with patch("src.ingestion.download.requests.get", return_value=mock_response):
        result = download_file(2026, 6, raw_dir=tmp_path)

    assert result.success is True
    assert result.file_path.exists()
    assert result.file_path.stat().st_size > 0
    assert result.error is None


def test_download_file_handles_http_error(tmp_path):
    """An HTTP error (e.g. 404) should be captured, not raised."""
    import requests

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False

    with patch("src.ingestion.download.requests.get", return_value=mock_response):
        result = download_file(2026, 6, raw_dir=tmp_path)

    assert result.success is False
    assert "HTTP error" in result.error
    assert result.file_path is None


def test_download_file_handles_connection_error(tmp_path):
    """A network-level failure (DNS, timeout, etc.) should be captured, not raised."""
    import requests

    with patch(
        "src.ingestion.download.requests.get",
        side_effect=requests.exceptions.ConnectionError("connection refused"),
    ):
        result = download_file(2026, 6, raw_dir=tmp_path)

    assert result.success is False
    assert "Request failed" in result.error


def test_download_file_rejects_empty_response(tmp_path):
    """A 200 OK response with zero bytes should still be treated as a failure."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = []  # no chunks at all
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False

    with patch("src.ingestion.download.requests.get", return_value=mock_response):
        result = download_file(2026, 6, raw_dir=tmp_path)

    assert result.success is False
    assert "empty" in result.error.lower()