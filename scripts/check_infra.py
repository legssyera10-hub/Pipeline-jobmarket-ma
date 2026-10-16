"""Vérification de bout en bout du socle (jour 1).

Usage :  docker compose run --rm app python -m scripts.check_infra

Contrôles :
  1. le bucket MinIO existe (boto3)
  2. pandas écrit puis relit du Parquet partitionné dans MinIO (s3fs)
  3. PostgreSQL répond et les schémas gold/ops existent (psycopg2)
  4. Spark relit le Parquet de pandas via s3a://
  5. Spark écrit deux fois la même partition en overwrite dynamique -> pas de doublons
  6. Spark écrit et relit une table PostgreSQL via JDBC
Code de sortie 0 si tout passe, 1 sinon.
"""

from __future__ import annotations

import sys
import time
import traceback
from datetime import date

import pandas as pd
import psycopg2

from src.utils.cli import run_cli
from src.utils.config import get_settings
from src.utils.spark import get_spark
from src.utils.storage import s3_client

PREFIX = "_healthcheck"
RUN_DATE = date(2026, 1, 1)  # date fixe : le contrôle lui-même est idempotent


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "offre_id": ["a1", "a2", "a3"],
            "titre": ["Data Engineer", "Data Analyst", "ML Engineer"],
            "ville": ["Casablanca", "Rabat", "Tanger"],
        }
    )


def check_bucket(s) -> str:
    s3_client(s).head_bucket(Bucket=s.minio_bucket)
    return f"bucket '{s.minio_bucket}' accessible sur {s.minio_endpoint}"


def check_pandas_parquet(s) -> str:
    path = s.s3_path(PREFIX, "pandas", f"date={RUN_DATE}", "data.parquet")
    _sample().to_parquet(path, storage_options=s.storage_options, index=False)
    back = pd.read_parquet(path, storage_options=s.storage_options)
    assert len(back) == 3, f"attendu 3 lignes, lu {len(back)}"
    return f"{path} ({len(back)} lignes relues)"


def check_postgres(s) -> str:
    with (
        psycopg2.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password,
            connect_timeout=5,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("SHOW server_version")
        version = cur.fetchone()[0]
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('gold', 'ops') ORDER BY 1"
        )
        schemas = [r[0] for r in cur.fetchall()]
    assert schemas == ["gold", "ops"], f"schémas manquants : {schemas}"
    return f"PostgreSQL {version}, schémas {schemas}"


def check_spark_read(spark, s) -> str:
    path = s.s3_path(PREFIX, "pandas", scheme="s3a")
    df = spark.read.parquet(path)
    n = df.count()
    assert n == 3, f"attendu 3 lignes, lu {n}"
    assert "date" in df.columns, "colonne de partition 'date' non découverte"
    return f"{path} : {n} lignes, partition 'date' découverte"


def check_spark_idempotent_write(spark, s) -> str:
    from pyspark.sql import functions as F

    path = s.s3_path(PREFIX, "spark", scheme="s3a")
    df = spark.createDataFrame(_sample()).withColumn("date", F.lit(str(RUN_DATE)))
    for _ in range(2):  # deux exécutions du "même run"
        df.write.mode("overwrite").partitionBy("date").parquet(path)
    n = spark.read.parquet(path).filter(F.col("date") == str(RUN_DATE)).count()
    assert n == 3, f"idempotence KO : {n} lignes après 2 écritures (attendu 3)"
    return f"2 écritures de la même partition -> {n} lignes, pas de doublon"


def check_spark_jdbc(spark, s) -> str:
    table = "ops._healthcheck"
    df = spark.createDataFrame(_sample())
    df.write.jdbc(s.jdbc_url, table, mode="overwrite", properties=s.jdbc_properties)
    n = spark.read.jdbc(s.jdbc_url, table, properties=s.jdbc_properties).count()
    assert n == 3, f"attendu 3 lignes, lu {n}"
    return f"écrit/relit {table} : {n} lignes"


def main() -> int:
    s = get_settings()
    results: list[tuple[str, bool, str]] = []

    def run(name, fn, *args):
        t0 = time.perf_counter()
        try:
            msg = fn(*args)
            results.append((name, True, f"{msg} [{time.perf_counter() - t0:.1f}s]"))
        except Exception as exc:  # rapport complet plutôt qu'arrêt au 1er échec
            traceback.print_exc()
            results.append((name, False, f"{type(exc).__name__}: {exc}"))

    run("MinIO bucket", check_bucket, s)
    run("pandas <-> MinIO", check_pandas_parquet, s)
    run("PostgreSQL", check_postgres, s)

    spark = get_spark("check_infra", s)
    try:
        run("Spark lecture s3a", check_spark_read, spark, s)
        run("Spark écriture idempotente", check_spark_idempotent_write, spark, s)
        run("Spark JDBC -> PostgreSQL", check_spark_jdbc, spark, s)
        spark_version = spark.version
    finally:
        spark.stop()

    py = sys.version.split()[0]
    print(f"\n=== Vérification du socle (Spark {spark_version}, Python {py}) ===")
    for name, ok, msg in results:
        print(f"[{'OK' if ok else 'KO'}] {name:<28} {msg}")
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} contrôles OK")
    return 1 if failed else 0


if __name__ == "__main__":
    run_cli(main)
