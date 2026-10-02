"""
Tracks ingestion history for the NYC Taxi pipeline.

Storage format: a single JSON file containing a list of records, e.g.
[
  {
    "year": 2026,
    "month": 6,
    "status": "success",
    "row_count": 3837248,
    "ingested_at": "2026-09-25T10:15:30",
    "error": null
  },
  ...
]
"""

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

DEFAULT_METADATA_PATH = Path("data/bronze/_ingestion_metadata.json")

def load_metadata(path: Path = DEFAULT_METADATA_PATH) -> list[dict]:
    if not path.exists():
        return []

    with open(path, "r") as f:
        return json.load(f)


def save_metadata(records: list[dict], path: Path = DEFAULT_METADATA_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(records, f, indent=2)


def is_ingested(year: int, month: int, path: Path = DEFAULT_METADATA_PATH) -> bool:
    
    records = load_metadata(path)
    matching = [r for r in records if r["year"] == year and r["month"] == month]
    if not matching:
        return False

    latest = matching[-1]
    return latest["status"] == "success"


def record_attempt(
    year: int,
    month: int,
    status: str,
    row_count: Optional[int] = None,
    error: Optional[str] = None,
    path: Path = DEFAULT_METADATA_PATH,
) -> dict:
    if status not in ("success", "failed"):
        raise ValueError(f"status must be 'success' or 'failed', got: {status}")

    record = {
        "year": year,
        "month": month,
        "status": status,
        "row_count": row_count,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "error": error,
    }

    records = load_metadata(path)
    records.append(record)
    save_metadata(records, path)

    return record


def get_ingestion_history(
    year: int, month: int, path: Path = DEFAULT_METADATA_PATH
) -> list[dict]:
    records = load_metadata(path)
    return [r for r in records if r["year"] == year and r["month"] == month]