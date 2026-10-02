from pathlib import Path
from src.ingestion.metadata import is_ingested, record_attempt, get_ingestion_history

test_path = Path("test_metadata.json")

# 1. Fresh state
print("Before any attempt:", is_ingested(2026, 6, test_path))

# 2. Simulate a failed attempt
record_attempt(2026, 6, status="failed", error="Missing column: request_source", path=test_path)
print("After failed attempt:", is_ingested(2026, 6, test_path))

# 3. Simulate a successful retry
record_attempt(2026, 6, status="success", row_count=3837248, path=test_path)
print("After successful attempt:", is_ingested(2026, 6, test_path))

# 4. Check full history
print("History:", get_ingestion_history(2026, 6, test_path))

# 5. Different month should be unaffected
print("Different month (2026-07):", is_ingested(2026, 7, test_path))

test_path.unlink()  # cleanup