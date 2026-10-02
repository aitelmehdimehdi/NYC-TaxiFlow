"""
src/ingestion/download.py

Handles the actual download of a TLC Yellow Taxi trip data file for a
given year/month, landing it in the raw/ staging area.

Design notes:
- Uses `requests` with streaming, since these files run into the
  hundreds of MB — loading the whole response into memory at once
  would be wasteful and, on some machines, risky.
- Per the project's idempotency decision: this ALWAYS downloads fresh
  and overwrites whatever's in raw/. raw/ is untrusted staging, not
  Bronze, so overwriting it is safe and keeps retry logic simple.
- Does NOT touch the metadata log or Bronze. This module's only job is
  "get the bytes from TLC onto local disk." Validation and promotion
  to Bronze are separate steps (separate modules), kept deliberately
  decoupled so each piece is independently testable.
"""

import requests
from pathlib import Path
from dataclasses import dataclass


TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_RAW_DIR = Path("data/raw")


@dataclass
class DownloadResult:
    success: bool
    file_path: Path | None
    url: str
    error: str | None = None


def build_url(year: int, month: int) -> str:
    """
    Build the TLC download URL for a given year/month.
    Month is zero-padded to two digits, matching TLC's naming convention.

    Example: build_url(2026, 6) ->
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-06.parquet"
    """
    if not (1 <= month <= 12):
        raise ValueError(f"month must be between 1 and 12, got: {month}")

    return f"{TLC_BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def download_file(
    year: int,
    month: int,
    raw_dir: Path = DEFAULT_RAW_DIR,
    timeout: int = 30,
) -> DownloadResult:
    """
    Download the TLC file for year/month into raw_dir, streaming to
    avoid loading the whole file into memory.

    Always overwrites any existing file at the destination path
    (per the project's "always re-download fresh" idempotency rule).

    Returns a DownloadResult describing what happened. Never raises
    on network/HTTP errors — those are captured in the result so the
    calling code (ingest.py) can decide what to do and log accordingly.
    """
    url = build_url(year, month)
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest_path = raw_dir / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"

    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Sanity check: a 0-byte file means something went wrong even
        # though the HTTP status was OK (rare, but worth catching).
        if dest_path.stat().st_size == 0:
            return DownloadResult(
                success=False,
                file_path=None,
                url=url,
                error="Downloaded file is empty (0 bytes)",
            )

        return DownloadResult(success=True, file_path=dest_path, url=url)

    except requests.exceptions.HTTPError as e:
        return DownloadResult(
            success=False,
            file_path=None,
            url=url,
            error=f"HTTP error: {e}",
        )
    except requests.exceptions.RequestException as e:
        return DownloadResult(
            success=False,
            file_path=None,
            url=url,
            error=f"Request failed: {e}",
        )