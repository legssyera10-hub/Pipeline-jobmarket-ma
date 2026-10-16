"""Couche Gold : agrégats analytiques de silver/offres -> PostgreSQL (schéma gold).

  python -m src.transform.gold [--source kaggle_linkedin]

Recalcul complet à chaque exécution (quelques milliers de lignes : l'incrémental
n'apporterait rien), donc idempotent par construction. Publication en deux temps :
1. Spark écrit chaque table dans gold_staging (JDBC) ;
2. une transaction PostgreSQL remplace les lignes de la source dans gold.* :
   un lecteur (Power BI) voit l'ancienne version ou la nouvelle, jamais un état
   intermédiaire. Les clés primaires de gold.* refusent tout doublon.

Comptages : `nb_offres` = offres uniques (est_republication = false, voir D12),
`nb_annonces` = toutes les annonces.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.cli import run_cli
from src.utils.config import Settings, get_settings
from src.utils.db import apply_schema, connect
from src.utils.spark import get_spark

TABLES = [
    "tech_par_mois",
    "offres_par_ville",
    "experience_demandee",
    "cooccurrence_tech",
]

# (borne basse incluse, libellé, ordre) ; NULL -> "Non précisé"
TRANCHES_EXPERIENCE = [
    (0, "0-1 an", 1),
    (2, "2-3 ans", 2),
    (4, "4-5 ans", 3),
    (6, "6-9 ans", 4),
    (10, "10 ans et +", 5),
]


def _unique():
    """1 si offre unique (hors republication). Fonction : F.col exige Spark actif."""
    return (~F.col("est_republication")).cast("int")


def _with_month(df: DataFrame) -> DataFrame:
    return df.withColumn("mois", F.trunc("date_publication", "month"))


def tech_par_mois(silver: DataFrame) -> DataFrame:
    base = _with_month(silver)
    scopes = {
        "toutes": base,
        "tech": base.where(F.col("famille_poste") != "AUTRE"),
    }
    parts = []
    for scope, df in scopes.items():
        totals = df.groupBy("source", "mois").agg(F.sum(_unique()).alias("_total"))
        counts = (
            df.select(
                "source",
                "mois",
                F.explode("technologies").alias("technologie"),
                "est_republication",
            )  # fmt: skip
            .groupBy("source", "mois", "technologie")
            .agg(
                F.sum(_unique()).alias("nb_offres"),
                F.count("*").alias("nb_annonces"),
            )
        )
        parts.append(
            counts.join(totals, ["source", "mois"])
            .withColumn("perimetre", F.lit(scope))
            .withColumn(
                "part_relative",
                F.when(
                    F.col("_total") > 0, F.col("nb_offres") / F.col("_total")
                ).otherwise(F.lit(0.0)),
            )
            .drop("_total")
        )
    out = parts[0].unionByName(parts[1])
    return out.select(
        "source", "perimetre", "mois", "technologie", "nb_offres", "nb_annonces",
        "part_relative",
    )  # fmt: skip


def offres_par_ville(silver: DataFrame) -> DataFrame:
    return (
        _with_month(silver)
        .groupBy(
            "source",
            "mois",
            F.coalesce("ville_normalisee", F.lit("Non précisée")).alias("ville"),
            F.coalesce("etat_code", F.lit("NA")).alias("etat_code"),
            "famille_poste",
        )
        .agg(F.sum(_unique()).alias("nb_offres"), F.count("*").alias("nb_annonces"))
    )


def _tranche(col: str):
    label, order = F.lit("Non précisé"), F.lit(0)
    for low, name, rank in TRANCHES_EXPERIENCE:  # bornes croissantes
        label = F.when(F.col(col) >= low, F.lit(name)).otherwise(label)
        order = F.when(F.col(col) >= low, F.lit(rank)).otherwise(order)
    return label, order


def experience_demandee(silver: DataFrame) -> DataFrame:
    label, order = _tranche("experience_min_annees")
    counts = (
        silver.withColumn("tranche", label)
        .withColumn("ordre_tranche", order)
        .groupBy("source", "famille_poste", "tranche", "ordre_tranche")
        .agg(F.sum(_unique()).alias("nb_offres"), F.count("*").alias("nb_annonces"))
    )
    totals = counts.groupBy("source", "famille_poste").agg(
        F.sum("nb_offres").alias("_total")
    )
    return (
        counts.join(totals, ["source", "famille_poste"])
        .withColumn(
            "part_famille",
            F.when(F.col("_total") > 0, F.col("nb_offres") / F.col("_total")).otherwise(
                F.lit(0.0)
            ),
        )
        .drop("_total")
    )


def cooccurrence_tech(silver: DataFrame) -> DataFrame:
    """Paires de technologies citées dans la même offre unique."""
    offers = silver.where(~F.col("est_republication")).select(
        "source", "offre_id", F.array_distinct("technologies").alias("t")
    )
    exploded = offers.select("source", "offre_id", F.explode("t").alias("tech"))
    support = exploded.groupBy("source", "tech").agg(F.count("*").alias("n"))
    n_offers = offers.groupBy("source").agg(F.count("*").alias("_n_total"))
    a, b = exploded.alias("a"), exploded.alias("b")
    pairs = (
        a.join(
            b,
            (F.col("a.source") == F.col("b.source"))
            & (F.col("a.offre_id") == F.col("b.offre_id"))
            & (F.col("a.tech") < F.col("b.tech")),
        )
        .groupBy(F.col("a.source").alias("source"),
                 F.col("a.tech").alias("tech_a"), F.col("b.tech").alias("tech_b"))
        .agg(F.count("*").alias("nb_offres_communes"))
    )  # fmt: skip
    sa = support.select("source", F.col("tech").alias("tech_a"),
                        F.col("n").alias("nb_offres_a"))  # fmt: skip
    sb = support.select("source", F.col("tech").alias("tech_b"),
                        F.col("n").alias("nb_offres_b"))  # fmt: skip
    common, na, nb = (
        F.col("nb_offres_communes"),
        F.col("nb_offres_a"),
        F.col("nb_offres_b"),
    )
    return (
        pairs.join(sa, ["source", "tech_a"])
        .join(sb, ["source", "tech_b"])
        .join(n_offers, "source")
        .withColumn("jaccard", common / (na + nb - common))
        .withColumn("lift", common * F.col("_n_total") / (na * nb))
        .select(
            "source", "tech_a", "tech_b", "nb_offres_communes", "nb_offres_a",
            "nb_offres_b", "jaccard", "lift",
        )  # fmt: skip
    )


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    return {
        "tech_par_mois": tech_par_mois(silver),
        "offres_par_ville": offres_par_ville(silver),
        "experience_demandee": experience_demandee(silver),
        "cooccurrence_tech": cooccurrence_tech(silver),
    }


def publish(tables: dict[str, DataFrame], source: str, settings: Settings) -> dict:
    """gold_staging (Spark/JDBC) puis bascule transactionnelle dans gold.*"""
    counts = {}
    for name, df in tables.items():
        df.write.jdbc(
            settings.jdbc_url,
            f"gold_staging.{name}",
            mode="overwrite",
            properties=settings.jdbc_properties,
        )
        counts[name] = df.count()
    with connect(settings) as conn, conn.cursor() as cur:  # une seule transaction
        for name, df in tables.items():
            cols = ", ".join(df.columns)
            cur.execute(f"DELETE FROM gold.{name} WHERE source = %s", (source,))
            cur.execute(
                f"INSERT INTO gold.{name} ({cols}) "
                f"SELECT {cols} FROM gold_staging.{name} WHERE source = %s",
                (source,),
            )
    return counts


def run(spark: SparkSession, settings: Settings, source: str) -> dict:
    silver = (
        spark.read.parquet(settings.s3_path("silver/offres", scheme="s3a"))
        .where(F.col("source") == source)
        .select(
            "source",
            "offre_id",
            "date_publication",
            "famille_poste",
            "ville_normalisee",
            "etat_code",
            "experience_min_annees",
            "technologies",
            "est_republication",
        )  # fmt: skip
        .cache()  # petite projection (sans description) relue par les 4 tables
    )
    try:
        if silver.limit(1).count() == 0:
            raise RuntimeError(f"silver/offres vide pour la source {source}")
        return publish(build_gold(silver), source, settings)
    finally:
        silver.unpersist()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Job Gold")
    p.add_argument("--source", default="kaggle_linkedin")
    args = p.parse_args(argv)
    settings = get_settings()
    apply_schema(settings)
    t0 = time.perf_counter()
    spark = get_spark("gold", settings)
    try:
        counts = run(spark, settings, args.source)
    finally:
        spark.stop()
    print(f"[gold] {args.source} publié en {time.perf_counter() - t0:.0f}s")
    for name, n in counts.items():
        print(f"  gold.{name:<20} {n:>7,} lignes")
    return 0


if __name__ == "__main__":
    run_cli(main)
