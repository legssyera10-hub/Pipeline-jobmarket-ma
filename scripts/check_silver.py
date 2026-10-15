"""Contrôle et diagnostic de la couche Silver d'une source.

Usage :
  docker compose run --rm app python -m scripts.check_silver [source]

Vérifie le schéma, l'unicité de offre_id et la cohérence des colonnes, puis affiche
des diagnostics qui servent à juger les règles (republications, expérience, nulls).
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from src.utils.config import get_settings
from src.utils.spark import get_spark

EXPECTED_TYPES = {
    "offre_id": "string",
    "date_collecte": "date",
    "date_publication": "date",
    "experience_min_annees": "int",
    "salaire_min_annuel": "int",
    "technologies": "array<string>",
    "est_republication": "boolean",
    "_ingested_at": "timestamp",
}


def main(source: str) -> int:
    s = get_settings()
    spark = get_spark("check_silver", s)
    try:
        df = spark.read.parquet(s.s3_path("silver/offres", scheme="s3a")).where(
            F.col("source") == source
        )
        df.printSchema()
        types = dict(df.dtypes)
        n = df.count()
        n_ids = df.select("offre_id").distinct().count()

        print("\n--- Taux de valeurs nulles (colonnes clés)")
        cols = [
            "date_publication", "titre_normalise", "entreprise_normalisee",
            "ville_normalisee", "etat_code", "type_contrat", "niveau_experience",
            "experience_min_annees", "salaire_min_annuel",
        ]  # fmt: skip
        nulls = df.select(
            [F.avg(F.col(c).isNull().cast("int")).alias(c) for c in cols]
        ).first()
        for c in cols:
            print(f"  {c:<24} {nulls[c]:6.1%}")

        print("\n--- Republications : même jour vs jours différents")
        df.where("est_republication").select(
            F.when(
                F.col("date_publication") == F.col("date_occurrence_precedente"),
                "meme_jour",
            )
            .otherwise("jours_differents")
            .alias("type")
        ).groupBy("type").count().show()
        print("Clés les plus republiées :")
        (
            df.where("est_republication")
            .groupBy("entreprise_normalisee", "titre_normalise", "ville_normalisee")
            .count()
            .orderBy(F.desc("count"))
            .show(8, truncate=40)
        )

        print("--- Expérience extraite selon le niveau déclaré (cohérence)")
        (
            df.groupBy("niveau_experience")
            .agg(
                F.count("*").alias("offres"),
                F.avg(F.col("experience_min_annees").isNotNull().cast("int")).alias(
                    "couverture"
                ),
                F.percentile_approx("experience_min_annees", 0.5).alias("mediane_min"),
            )
            .orderBy("mediane_min")
            .show()
        )

        print("--- Technologies par famille de poste")
        (
            df.groupBy("famille_poste")
            .agg(
                F.count("*").alias("offres"),
                F.avg(F.size("technologies")).alias("nb_moyen_technos"),
            )
            .orderBy(F.desc("offres"))
            .show()
        )
        incoherent_dates = df.where(
            F.col("date_expiration") < F.col("date_publication")
        ).count()
    finally:
        spark.stop()

    checks = {
        "types attendus": all(types.get(c) == t for c, t in EXPECTED_TYPES.items()),
        "offre_id unique": n == n_ids,
        "aucune expiration avant publication": incoherent_dates == 0,
    }
    print(f"\n{n:,} lignes")
    for name, ok in checks.items():
        print(f"[{'OK' if ok else 'KO'}] {name}")
    if incoherent_dates:
        print(f"     ({incoherent_dates} offres expirent avant leur publication)")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "kaggle_linkedin"))
