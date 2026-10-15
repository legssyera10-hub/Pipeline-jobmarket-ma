"""Configuration centralisée, lue depuis les variables d'environnement (.env en local).

Aucun identifiant n'est écrit en dur dans le code : tout passe par ici.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import find_dotenv, load_dotenv


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Variable d'environnement manquante : {name} (voir .env.example)"
        )
    return value


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    spark_master: str
    spark_driver_memory: str = "1536m"

    @classmethod
    def from_env(cls) -> Settings:
        # .env cherché depuis le dossier courant (racine du projet).
        # override=False : une variable déjà définie (docker compose) gagne sur .env
        load_dotenv(find_dotenv(usecwd=True), override=False)
        return cls(
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=_require("MINIO_ROOT_USER"),
            minio_secret_key=_require("MINIO_ROOT_PASSWORD"),
            minio_bucket=os.getenv("MINIO_BUCKET", "jobmarket"),
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "jobmarket"),
            postgres_user=_require("POSTGRES_USER"),
            postgres_password=_require("POSTGRES_PASSWORD"),
            spark_master=os.getenv("SPARK_MASTER", "local[4]"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1536m"),
        )

    @property
    def storage_options(self) -> dict:
        """Options s3fs pour pandas (argument storage_options de read/to_parquet)."""
        return {
            "key": self.minio_access_key,
            "secret": self.minio_secret_key,
            "client_kwargs": {"endpoint_url": self.minio_endpoint},
        }

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/"
            f"{self.postgres_db}"
        )

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
            # Réseau Docker local, PostgreSQL sans TLS : on évite la négociation SSL,
            # qui a provoqué un timeout au démarrage à froid. À revoir si BDD distante.
            "sslmode": "disable",
            "connectTimeout": "30",
        }

    def s3_path(self, *parts: str, scheme: str = "s3") -> str:
        """Chemin dans le bucket. scheme="s3" pour pandas/s3fs, "s3a" pour Spark."""
        key = "/".join(p.strip("/") for p in parts)
        return f"{scheme}://{self.minio_bucket}/{key}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
