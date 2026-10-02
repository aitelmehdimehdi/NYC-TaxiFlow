"""
src/utils/config.py

Loads database configuration from environment variables, with support
for a local .env file (via python-dotenv) so credentials never need
to be hardcoded or committed to the repo.

Usage: put real values in a .env file at the repo root (gitignored),
matching the keys documented in .env.example.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()  # no-op if no .env file exists; real env vars still work

@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

def get_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "nyc_taxi"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )