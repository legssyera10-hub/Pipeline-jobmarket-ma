"""Démonstration : des données volontairement corrompues sont arrêtées.

Critère « fini quand » du jour 5 : une donnée corrompue déclenche un échec de
validation ET atterrit en quarantaine. Le test se fait sur une source isolée
(`demo_corruption`) : les vraies données ne sont jamais touchées.

  docker compose run --rm app python -m scripts.demo_corruption [--keep]

1. copie 200 vraies offres Bronze du 2024-04-09 dans bronze/source=demo_corruption ;
2. en corrompt une partie (voir CORRUPTIONS) ;
3. lance le job Silver sur cette source ;
4. vérifie : rejets en quarantaine avec le bon motif, suite critique en échec,
   rien publié dans silver/offres ; puis nettoie (sauf --keep).
Code de sortie 0 si la corruption a bien été arrêtée.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime

import pandas as pd
from pyspark.sql import functions as F

from src.ingestion.bronze import TECH_COLUMNS, write_bronze
from src.transform.silver import print_report, run
from src.utils.cli import run_cli
from src.utils.config import get_settings
from src.utils.db import apply_schema
from src.utils.spark import get_spark
from src.utils.storage import delete_prefix, list_keys

SOURCE = "demo_corruption"
RUN_DATE = date(2024, 4, 9)
N = 200
AFTER_COLLECTION = str(
    datetime(2024, 5, 1, tzinfo=UTC).timestamp() * 1000
)  # "1714521600000.0", même format que la source

# (lignes, colonne, valeur, effet attendu)
CORRUPTIONS = [
    (range(0, 20), "title", None, "rejet : titre_manquant"),
    (range(20, 40), "original_listed_time", AFTER_COLLECTION,
     "rejet : publication_apres_collecte"),
    (range(40, 50), "original_listed_time", None, "rejet : date_publication_manquante"),
    (range(60, 160), "location", "United States",
     "valide, mais 2/3 des villes vides : dérive détectée par la suite critique"),
]  # fmt: skip
SALARY_ROWS = range(50, 60)  # min > max : anomalie signalée, offre conservée
EXPECTED_REJECTS = {
    "titre_manquant": 20,
    "publication_apres_collecte": 20,
    "date_publication_manquante": 10,
}

PREFIXES = [
    f"bronze/source={SOURCE}/",
    f"silver/offres/source={SOURCE}/",
    f"silver/rejets/source={SOURCE}/",
    f"silver/_staging/{SOURCE}/",
]


def corrupted_batch(settings) -> pd.DataFrame:
    path = settings.s3_path(
        f"bronze/source=kaggle_linkedin/date={RUN_DATE}/data.parquet"
    )
    df = pd.read_parquet(path, storage_options=settings.storage_options)
    df = df.drop(columns=TECH_COLUMNS).head(N).reset_index(drop=True)
    for rows, column, value, _ in CORRUPTIONS:
        df.loc[list(rows), column] = value
    salary_cols = ["min_salary", "max_salary", "pay_period", "currency"]
    df.loc[list(SALARY_ROWS), salary_cols] = ["50000.0", "40000.0", "YEARLY", "USD"]
    return df


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--keep", action="store_true", help="ne pas nettoyer à la fin")
    keep = p.parse_args().keep
    s = get_settings()
    apply_schema(s)
    for prefix in PREFIXES:  # repartir de zéro
        delete_prefix(prefix, s)

    print(f"[démo] {N} offres réelles copiées vers la source '{SOURCE}', corruptions :")
    for rows, column, value, effect in CORRUPTIONS:
        print(
            f"  lignes {rows.start}-{rows.stop - 1}: {column} = {value!r} -> {effect}"
        )
    print(f"  lignes {SALARY_ROWS.start}-{SALARY_ROWS.stop - 1}: salaire min > max "
          "-> anomalie signalée, offre conservée")  # fmt: skip
    write_bronze(corrupted_batch(s), SOURCE, RUN_DATE, s)

    spark = get_spark("demo_corruption", s)
    try:
        metrics, report = run(spark, s, SOURCE, RUN_DATE, RUN_DATE)
        rejets = spark.read.parquet(s.s3_path("silver/rejets", scheme="s3a")).where(
            F.col("source") == SOURCE
        )
        motifs = {
            r["m"]: r["count"]
            for r in rejets.select(F.explode("_motifs_rejet").alias("m"))
            .groupBy("m")
            .count()
            .collect()
        }
    finally:
        spark.stop()

    print("\n[démo] contrôles qualité :")
    print_report(report)
    failed = {r["controle"] + (f" ({r['colonne']})" if r["colonne"] else "")
              for r in report.failures("critique")}  # fmt: skip
    checks = {
        "les 50 lignes corrompues sont en quarantaine avec le bon motif": motifs
        == EXPECTED_REJECTS,
        "la suite critique échoue (taux de rejet)": "taux_de_rejet_maximal" in failed,
        "la suite critique échoue (dérive des villes)": any(
            "ville_normalisee" in f for f in failed
        ),
        "rien n'est publié dans silver/offres": not metrics["publie"]
        and not list_keys(f"silver/offres/source={SOURCE}/", s),
        "les offres au salaire incohérent ne sont pas rejetées": "salaire"
        not in str(motifs),
    }
    print("\n[démo] vérifications :")
    for name, ok in checks.items():
        print(f"  [{'OK' if ok else 'KO'}] {name}")
    print(f"  motifs observés : {motifs}")

    if not keep:
        removed = sum(delete_prefix(prefix, s) for prefix in PREFIXES)
        print(f"[démo] nettoyage : {removed} fichier(s) supprimé(s) "
              "(les résultats restent dans ops.controles_qualite)")  # fmt: skip
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    run_cli(main)
