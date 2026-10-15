"""Accès PostgreSQL (psycopg2) : connexion, application du schéma."""

from __future__ import annotations

from pathlib import Path

import psycopg2

from src.utils.config import Settings, get_settings

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


def connect(settings: Settings | None = None):
    s = settings or get_settings()
    return psycopg2.connect(
        host=s.postgres_host,
        port=s.postgres_port,
        dbname=s.postgres_db,
        user=s.postgres_user,
        password=s.postgres_password,
        connect_timeout=10,
    )


def apply_schema(settings: Settings | None = None) -> None:
    """Crée schémas et tables manquants (idempotent)."""
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
