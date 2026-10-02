"""
src/warehouse/connection.py

Reusable psycopg2 connection factory for the warehouse layer.
Centralized here so every loader/query script connects the same way,
using credentials from src/utils/config.py (never hardcoded).
"""

import psycopg2
from src.utils.config import get_postgres_config


def get_connection():
    """
    Return a new psycopg2 connection to the project's Postgres database.
    Caller is responsible for closing it (or using it as a context manager).
    """
    config = get_postgres_config()
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
    )