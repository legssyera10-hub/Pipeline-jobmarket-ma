"""Écriture de la couche Bronze : Parquet brut, immuable, partitionné par date.

Chemin : bronze/source=<source>/date=<YYYY-MM-DD>/data.parquet

Immuabilité garantie par le stockage, pas par une vérification préalable :
PutObject avec `If-None-Match: *` est refusé (HTTP 412) si l'objet existe déjà.
Pas de fenêtre « je vérifie puis j'écris » où deux runs concurrents pourraient
s'écraser. Relancer une date déjà ingérée est donc sans effet (idempotent).
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.utils.config import Settings, get_settings
from src.utils.storage import s3_client

TECH_COLUMNS = ["_ingested_at", "_source", "_run_date"]


class EmptyBatchError(ValueError):
    """La source n'a rien renvoyé pour cette date."""


@dataclass(frozen=True)
class BronzeResult:
    source: str
    run_date: date
    key: str
    rows: int
    status: str  # "written" | "skipped"


def bronze_key(source: str, run_date: date) -> str:
    return f"bronze/source={source}/date={run_date.isoformat()}/data.parquet"


def add_technical_columns(
    df: pd.DataFrame, source: str, run_date: date, ingested_at: pd.Timestamp
) -> pd.DataFrame:
    clash = set(TECH_COLUMNS) & set(df.columns)
    if clash:
        raise ValueError(f"colonnes réservées déjà présentes dans la source : {clash}")
    out = df.copy()
    out["_ingested_at"] = ingested_at
    out["_source"] = source
    out["_run_date"] = run_date
    return out


def bronze_schema(columns: list[str]) -> pa.Schema:
    """Schéma imposé, jamais deviné : colonnes source en texte, techniques typées.

    Sans lui, une colonne entièrement vide dans une petite partition est inférée en
    type « null » (écrit INT32), et Spark refuse de la fusionner avec les fichiers où
    elle est en string.
    """
    tech = {
        "_ingested_at": pa.timestamp("us", tz="UTC"),
        "_source": pa.string(),
        "_run_date": pa.date32(),
    }
    return pa.schema([pa.field(c, tech.get(c, pa.string())) for c in columns])


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(
        df, schema=bronze_schema(list(df.columns)), preserve_index=False
    )
    buf = io.BytesIO()
    # Spark 3.5 ne lit pas les timestamps Parquet en nanosecondes (défaut pandas) :
    # on les écrit en microsecondes.
    pq.write_table(
        table,
        buf,
        compression="snappy",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    return buf.getvalue()


def _is_already_exists(err: ClientError) -> bool:
    code = err.response.get("Error", {}).get("Code", "")
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"PreconditionFailed", "ConditionalRequestConflict"} or status in {
        409,
        412,
    }


def write_bronze(
    df: pd.DataFrame,
    source: str,
    run_date: date,
    settings: Settings | None = None,
    client=None,
    ingested_at: pd.Timestamp | None = None,
) -> BronzeResult:
    if df.empty:
        raise EmptyBatchError(f"aucune offre pour {source} / {run_date}")

    s = settings or get_settings()
    client = client or s3_client(s)
    key = bronze_key(source, run_date)
    ingested_at = ingested_at or pd.Timestamp.now(tz="UTC")

    body = to_parquet_bytes(add_technical_columns(df, source, run_date, ingested_at))
    try:
        client.put_object(Bucket=s.minio_bucket, Key=key, Body=body, IfNoneMatch="*")
    except ClientError as err:
        if _is_already_exists(err):
            return BronzeResult(source, run_date, key, len(df), "skipped")
        raise
    return BronzeResult(source, run_date, key, len(df), "written")
