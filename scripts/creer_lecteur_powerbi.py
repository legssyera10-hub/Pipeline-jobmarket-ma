"""Crée (ou met à jour) le rôle PostgreSQL en lecture seule utilisé par Power BI.

  docker compose run --rm --no-deps app python -m scripts.creer_lecteur_powerbi

Power BI ne doit pas se connecter avec le compte administrateur : s'il fuit (fichier
.pbix partagé, identifiants mémorisés), il permet de tout effacer. `powerbi_lecteur`
ne peut que LIRE les schémas gold et bi de la base jobmarket.

Mot de passe : POWERBI_PASSWORD dans .env. S'il est vide, un mot de passe aléatoire
est généré et écrit dans .env (jamais affiché) ; il se saisit une fois dans Power BI.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from psycopg2 import sql

from src.utils.cli import run_cli
from src.utils.config import get_settings
from src.utils.db import apply_schema, connect

ROLE = "powerbi_lecteur"
ENV_FILE = Path(".env")
SCHEMAS = ["gold", "bi"]


def ensure_password() -> str:
    password = os.getenv("POWERBI_PASSWORD", "")
    if password:
        return password
    password = secrets.token_urlsafe(24)
    lines = (
        ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
    )
    lines = [ln for ln in lines if not ln.startswith("POWERBI_PASSWORD=")]
    lines.append(f"POWERBI_PASSWORD={password}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("POWERBI_PASSWORD généré et écrit dans .env (non affiché)")
    return password


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    apply_schema(settings)  # les schémas et vues existent avant les GRANT
    password = ensure_password()
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,))
        verb = "ALTER" if cur.fetchone() else "CREATE"
        cur.execute(
            sql.SQL(verb + " ROLE {} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE CONNECTION LIMIT 5").format(sql.Identifier(ROLE)),
            (password,),
        )  # fmt: skip
        role, db = sql.Identifier(ROLE), sql.Identifier(settings.postgres_db)
        cur.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(db))
        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(db, role))
        for name in SCHEMAS:
            schema = sql.Identifier(name)
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role))
            cur.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                    schema, role
                )
            )
            cur.execute(  # tables futures (nouvelles tables Gold) incluses
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
                ).format(schema, role)
            )
    print(f"Rôle {ROLE} : {verb.lower()} ; lecture seule sur {', '.join(SCHEMAS)}")
    return 0


if __name__ == "__main__":
    run_cli(main)
