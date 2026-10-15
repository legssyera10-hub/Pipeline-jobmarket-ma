"""Contrôle de la couche Bronze d'une source.

Usage :
  docker compose run --rm app python -m scripts.check_bronze [source]

Vérifie : relecture pandas d'une partition, relecture Spark de toute la source,
schéma (tout en texte + colonnes techniques), cohérence partition/_run_date,
unicité de job_id, zéros initiaux préservés, horodatage d'ingestion lisible.
"""

from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql import functions as F

from src.ingestion.bronze import TECH_COLUMNS
from src.utils.config import get_settings
from src.utils.spark import get_spark
from src.utils.storage import list_keys


def main(source: str) -> int:
    s = get_settings()
    prefix = f"bronze/source={source}/"
    keys = list_keys(prefix, s)
    print(f"{len(keys)} fichiers sous s3://{s.minio_bucket}/{prefix}")

    # 1. pandas relit une partition (critère « fini quand » du jour 2)
    sample = pd.read_parquet(s.s3_path(keys[-1]), storage_options=s.storage_options)
    print(f"pandas relit {keys[-1]} : {len(sample)} lignes, {sample.shape[1]} colonnes")

    # 2. Spark relit toute la source ; la partition `date` vient du chemin
    spark = get_spark("check_bronze", s)
    try:
        df = spark.read.parquet(s.s3_path(prefix, scheme="s3a"))
        types = dict(df.dtypes)
        raw_cols = [c for c in df.columns if c not in [*TECH_COLUMNS, "date"]]
        stats = df.agg(
            F.count("*").alias("lignes"),
            F.countDistinct("job_id").alias("job_id_distincts"),
            F.countDistinct("date").alias("partitions"),
            F.sum((F.col("date") != F.col("_run_date")).cast("int")).alias(
                "date_ne_run_date"
            ),
            F.sum(F.col("zip_code").startswith("0").cast("int")).alias("zip_zero"),
            F.countDistinct("_ingested_at").alias("n_ingested_at"),
            F.min("_ingested_at").alias("ingested_at"),
        ).first()
        top = df.groupBy("date").count().orderBy(F.desc("count")).limit(5).collect()
    finally:
        spark.stop()

    checks = {
        "colonnes brutes toutes en texte": all(types[c] == "string" for c in raw_cols),
        "colonnes techniques typées": types["_ingested_at"] == "timestamp"
        and types["_run_date"] == "date"
        and types["_source"] == "string",
        "job_id unique": stats["lignes"] == stats["job_id_distincts"],
        "partition date == _run_date": stats["date_ne_run_date"] == 0,
        "zéros initiaux de zip_code conservés": stats["zip_zero"] > 0,
    }
    print(
        f"\nSpark : {stats['lignes']:,} lignes, {stats['partitions']} partitions, "
        f"{len(raw_cols)} colonnes brutes + {TECH_COLUMNS}"
    )
    print(
        f"zip_code commençant par 0 : {stats['zip_zero']:,} ; "
        f"_ingested_at : {stats['n_ingested_at']} valeur(s), {stats['ingested_at']}"
    )
    print("partitions les plus volumineuses :")
    for r in top:
        print(f"  {r['date']}  {r['count']:>6,}")
    print()
    for name, ok in checks.items():
        print(f"[{'OK' if ok else 'KO'}] {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "kaggle_linkedin"))
