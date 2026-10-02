from src.ingestion.download import download_file
from pathlib import Path

result = download_file(2026, 6, raw_dir=Path("data/raw"))
print(result)