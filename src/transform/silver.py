"""Couche Silver : Bronze (texte brut) -> offres typées, normalisées, enrichies.

  python -m src.transform.silver --date 2024-04-09        # un jour (usage Airflow)
  python -m src.transform.silver --start 2024-04-01 --end 2024-04-20
  python -m src.transform.silver --all [--explain]

Sorties (Parquet, partitionnées source=<source>/date_collecte=<YYYY-MM-DD>/) :
  silver/offres  : offres valides, publiées seulement si la suite critique passe ;
  silver/rejets  : offres en quarantaine, avec tous leurs motifs (_motifs_rejet).
Idempotent : seules les partitions traitées sont remplacées (y compris les partitions
devenues vides), et aucun calcul ne dépend de la date du jour (pas de current_date()).
Code de sortie 1 si un contrôle critique échoue : Silver n'est alors pas publié et la
version précédente reste en place.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.quality.expectations import QualityReport, log_report, validate
from src.quality.rules import apply_quality_rules, split_quarantine
from src.transform.experience import parse_experience
from src.transform.maroc import find_city
from src.transform.technologies import extract_technologies, load_patterns
from src.transform.text import (
    classify_family,
    clean_url,
    normalize_company,
    normalize_title,
    parse_location,
)
from src.utils.config import Settings, get_settings
from src.utils.db import apply_schema
from src.utils.spark import get_spark
from src.utils.storage import copy_prefix, delete_prefix, list_keys

REFERENCE_DIR = Path(__file__).resolve().parents[2] / "reference"
REPUBLICATION_WINDOW_DAYS = 30
DEDUP_KEY = ["entreprise_normalisee", "titre_normalise", "_ville_cle"]

# Multiplicateurs pour annualiser un salaire (base 40 h x 52 semaines)
PAY_PERIOD_TO_YEAR = {
    "YEARLY": 1,
    "MONTHLY": 12,
    "BIWEEKLY": 26,
    "WEEKLY": 52,
    "HOURLY": 2080,
}
CONTRACT_TYPES = [
    "FULL_TIME", "PART_TIME", "CONTRACT", "TEMPORARY", "INTERNSHIP", "VOLUNTEER",
]  # fmt: skip
EXPERIENCE_LEVELS = {
    "Internship": "STAGE",
    "Entry level": "DEBUTANT",
    "Associate": "ASSOCIE",
    "Mid-Senior level": "CONFIRME",
    "Director": "DIRECTEUR",
    "Executive": "DIRIGEANT",
}

# --- Enrichissement Python (une seule UDF vectorisée, un seul passage) -----------

ENRICH_SCHEMA = T.StructType(
    [
        T.StructField("url", T.StringType()),
        T.StructField("titre_normalise", T.StringType()),
        T.StructField("famille_poste", T.StringType()),
        T.StructField("entreprise_normalisee", T.StringType()),
        T.StructField("ville_parsee", T.StringType()),
        T.StructField("etat_brut", T.StringType()),
        T.StructField("pays", T.StringType()),
        T.StructField("region_parsee", T.StringType()),
        T.StructField("experience_min_annees", T.IntegerType()),
        T.StructField("experience_max_annees", T.IntegerType()),
        T.StructField("technologies", T.ArrayType(T.StringType())),
    ]
)


def _locate(location: str | None, pays_code: str | None):
    """(ville, état brut, pays, région) selon le pays déclaré par la source."""
    if pays_code == "MA":
        found = find_city(location)
        ville, region = found if found else (None, None)
        return ville, None, "MA", region
    ville, etat, pays = parse_location(location)  # sources américaines
    return ville, etat, pays, None


def enrich_records(
    urls, titles, companies, locations, descriptions, skills, pays_codes=None
) -> pd.DataFrame:
    """Version pandas pure de l'enrichissement (testable sans Spark)."""
    patterns = load_patterns()
    if pays_codes is None:
        pays_codes = [None] * len(urls)
    rows = []
    for url, title, company, location, desc, skill, pays_code in zip(
        urls, titles, companies, locations, descriptions, skills, pays_codes,
        strict=True,
    ):  # fmt: skip
        ville, etat, pays, region = _locate(location, pays_code)
        exp_min, exp_max = parse_experience(desc)
        text = "\n".join(x for x in (title, desc, skill) if x)
        rows.append(
            (
                clean_url(url),
                normalize_title(title),
                classify_family(title),
                normalize_company(company),
                ville,
                etat,
                pays,
                region,
                exp_min,
                exp_max,
                extract_technologies(text, patterns),
            )
        )
    df = pd.DataFrame(rows, columns=ENRICH_SCHEMA.fieldNames())
    for col in ("experience_min_annees", "experience_max_annees"):
        df[col] = df[col].astype("Int64")
    return df


@F.pandas_udf(ENRICH_SCHEMA)
def _enrich_udf(
    url: pd.Series,
    title: pd.Series,
    company: pd.Series,
    location: pd.Series,
    description: pd.Series,
    skills: pd.Series,
    pays_code: pd.Series,
) -> pd.DataFrame:
    return enrich_records(url, title, company, location, description, skills, pays_code)


# --- Référentiels (petits : diffusés à chaque exécuteur par broadcast join) -------


def load_references(spark: SparkSession, ref_dir: Path = REFERENCE_DIR) -> dict:
    states = pd.read_csv(ref_dir / "us_states.csv", dtype=str)
    # Clé de jointure : le code ("TX") OU le nom ("TEXAS"), les deux formes existent
    lookup = pd.concat(
        [
            states.assign(_cle=states["etat_code"].str.upper()),
            states.assign(_cle=states["etat_nom"].str.upper()),
        ]
    )
    metros = pd.read_csv(ref_dir / "us_metros.csv", dtype=str).rename(
        columns={"ville": "_metro_ville", "etat_code": "_metro_etat"}
    )
    return {
        "states": spark.createDataFrame(lookup),
        "metros": spark.createDataFrame(metros),
    }


# --- Transformations Spark -------------------------------------------------------


# Contrat Bronze -> Silver : colonnes lues par build_silver. Une source qui n'en fournit
# pas certaines (l'ATS n'a pas de skills_desc, Kaggle n'a pas de pays_code) les reçoit
# vides : une seule logique Silver pour toutes les sources.
BRONZE_CONTRACT = [
    "job_id", "job_posting_url", "title", "company_name", "location", "description",
    "skills_desc", "pays_code", "original_listed_time", "listed_time", "expiry",
    "work_type", "workplace_type", "remote_allowed", "formatted_experience_level",
    "min_salary", "med_salary", "max_salary", "currency", "pay_period",
    "normalized_salary",
]  # fmt: skip


def conform_bronze(bronze: DataFrame) -> DataFrame:
    missing = [c for c in BRONZE_CONTRACT if c not in bronze.columns]
    return bronze.select("*", *[F.lit(None).cast("string").alias(c) for c in missing])


def _to_date(col: str) -> F.Column:
    """Epoch en millisecondes ("1713397508000.0", Kaggle et Lever) ou ISO 8601 avec
    fuseau ("2026-10-07T16:41:02-04:00", Greenhouse) -> date UTC."""
    c = F.col(col)
    return F.when(
        c.rlike(r"^\d+(\.\d+)?$"),
        F.to_date(F.timestamp_millis(c.cast("double").cast("long"))),
    ).otherwise(F.to_date(c.cast("timestamp")))


def _annual_salary(amount: F.Column) -> F.Column:
    mult = F.create_map(*[F.lit(x) for kv in PAY_PERIOD_TO_YEAR.items() for x in kv])
    return F.round(amount.cast("double") * mult[F.upper("pay_period")]).cast("int")


def build_silver(bronze: DataFrame, refs: dict) -> DataFrame:
    """Bronze -> Silver, une ligne par offre. Pas de déduplication ici."""
    e = _enrich_udf(
        "job_posting_url", "title", "company_name", "location", "description",
        "skills_desc", "pays_code",
    )  # fmt: skip
    df = (
        conform_bronze(bronze)
        .withColumn("_e", e)
        .select("*", "_e.*")
        .drop("_e")
        # Référentiel des zones métropolitaines ("Greater Houston" -> Houston, TX)
        .join(
            F.broadcast(refs["metros"]),
            F.trim(F.col("location")) == F.col("lieu_brut"),
            "left",
        )
        .withColumn("ville_normalisee", F.coalesce("_metro_ville", "ville_parsee"))
        .withColumn("_etat_cle", F.upper(F.coalesce("_metro_etat", "etat_brut")))
        # Référentiel des états : code ou nom -> code, nom, région
        .join(
            F.broadcast(refs["states"]),
            F.col("_etat_cle") == F.col("_cle"),
            "left",
        )
    )

    remote_title = F.lower("title")
    return df.select(
        F.sha2(
            F.concat_ws("|", F.col("_source"), F.coalesce("url", "job_id")), 256
        ).alias("offre_id"),
        F.col("_source").alias("source"),
        F.col("job_id").alias("id_source"),
        "url",
        F.col("_run_date").alias("date_collecte"),
        _to_date("original_listed_time").alias("date_publication"),
        _to_date("listed_time").alias("date_derniere_publication"),
        _to_date("expiry").alias("date_expiration"),
        F.col("title").alias("titre_brut"),
        "titre_normalise",
        "famille_poste",
        F.col("company_name").alias("entreprise"),
        "entreprise_normalisee",
        F.col("location").alias("lieu_brut"),
        "ville_normalisee",
        "etat_code",
        "etat_nom",
        # Région du recensement américain, ou région administrative marocaine
        F.coalesce("region", "region_parsee").alias("region"),
        "pays",
        F.when(F.upper("work_type").isin(CONTRACT_TYPES), F.upper("work_type"))
        .when(F.col("work_type").isNotNull(), F.lit("AUTRE"))
        .alias("type_contrat"),
        # remote_allowed ne vaut que 1 ou vide : vide ne veut PAS dire « sur site ».
        # workplace_type (Lever) : remote | hybrid | onsite | unspecified
        F.when(
            remote_title.contains("hybrid") | (F.lower("workplace_type") == "hybrid"),
            "HYBRIDE",
        )
        .when(
            (F.col("remote_allowed").cast("double") == 1)
            | remote_title.contains("remote")
            | (F.lower("workplace_type") == "remote"),
            "TELETRAVAIL",
        )
        .when(F.lower("workplace_type") == "onsite", "SUR_SITE")
        .otherwise("NON_PRECISE")
        .alias("teletravail"),
        F.create_map(*[F.lit(x) for kv in EXPERIENCE_LEVELS.items() for x in kv])[
            F.col("formatted_experience_level")
        ].alias("niveau_experience"),
        "experience_min_annees",
        "experience_max_annees",
        _annual_salary(F.coalesce("min_salary", "med_salary")).alias(
            "salaire_min_annuel"
        ),
        _annual_salary(F.coalesce("max_salary", "med_salary")).alias(
            "salaire_max_annuel"
        ),
        F.upper("currency").alias("devise"),
        F.upper("pay_period").alias("salaire_periode_source"),
        F.col("normalized_salary").cast("double").alias("salaire_normalise_source"),
        "technologies",
        F.col("description"),
        F.sha2(
            F.concat_ws(
                "|",
                "titre_normalise",
                "entreprise_normalisee",
                F.lower("ville_normalisee"),
                F.regexp_replace(F.lower("description"), r"\s+", " "),
            ),
            256,
        ).alias("hash_contenu"),
        "_ingested_at",
    )


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Même offre_id vu plusieurs fois (recollecte) : on garde la 1re collecte."""
    w = Window.partitionBy("offre_id").orderBy("date_collecte", "_ingested_at")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _published_ids(
    spark: SparkSession, settings: Settings, source: str, before: date
) -> DataFrame | None:
    """offre_id déjà publiés dans silver/offres pour cette source, avant `before`."""
    if not list_keys(f"silver/offres/source={source}/", settings):
        return None
    return (
        spark.read.parquet(
            settings.s3_path(f"silver/offres/source={source}", scheme="s3a")
        )
        .where(F.col("date_collecte") < F.lit(before))
        .select("offre_id")
    )


def exclude_published(df: DataFrame, published: DataFrame | None) -> DataFrame:
    """Retire les offres déjà publiées un jour précédent.

    Indispensable pour une source d'instantanés : une offre ouverte 45 jours apparaît
    dans 45 collectes. La fenêtre relue (30 jours) ne suffit pas à la reconnaître, la
    comparaison porte donc sur tout ce qui est déjà publié (une seule colonne lue).
    """
    if published is None:
        return df
    return df.join(F.broadcast(published.distinct()), "offre_id", "left_anti")


def mark_republications(df: DataFrame, days: int = REPUBLICATION_WINDOW_DAYS):
    """Fenêtre glissante : une offre est une REPUBLICATION si la même clé
    (entreprise, titre, ville) a été publiée dans les `days` jours précédents.

    Rien n'est supprimé : on marque (est_republication) et on garde la date de
    l'occurrence précédente. Gold compte les offres où est_republication = false.
    Une clé revue après plus de `days` jours redevient une nouvelle offre : un poste
    rouvert est un signal réel qu'on veut conserver.
    Déterministe : ne dépend que des dates de publication, pas de la date du jour.
    """
    keyed = df.withColumn(
        "_ville_cle", F.coalesce(F.lower("ville_normalisee"), F.lit(""))
    )
    w = Window.partitionBy(*DEDUP_KEY).orderBy("date_publication", "offre_id")
    prev = F.lag("date_publication").over(w)
    key_ok = (
        F.col("entreprise_normalisee").isNotNull()
        & F.col("titre_normalise").isNotNull()
        & F.col("date_publication").isNotNull()
    )
    is_repub = (
        key_ok & prev.isNotNull() & (F.datediff("date_publication", prev) <= days)
    )
    return (
        keyed.withColumn("est_republication", F.coalesce(is_repub, F.lit(False)))
        .withColumn(
            "date_occurrence_precedente",
            F.when(F.col("est_republication"), prev),
        )
        .drop("_ville_cle")
    )


# --- Job ---------------------------------------------------------------------------


def bronze_dates(source: str, settings: Settings) -> list[date]:
    keys = list_keys(f"bronze/source={source}/", settings)
    found = {re.search(r"date=(\d{4}-\d{2}-\d{2})", k) for k in keys}
    return sorted(date.fromisoformat(m.group(1)) for m in found if m)


def _partition_prefix(table: str, source: str, d: date) -> str:
    return f"silver/{table}/source={source}/date_collecte={d}/"


def _write_partitions(
    df: DataFrame,
    table: str,
    source: str,
    target_dates: list[date],
    settings: Settings,
) -> int:
    """Remplace les partitions `target_dates` de silver/<table> pour la source.

    L'overwrite dynamique ne remplace que les partitions présentes dans `df` : une
    date qui n'a plus aucune ligne (tout rejeté, ou plus aucun rejet) garderait sa
    version précédente. On supprime donc explicitement ces partitions-là.
    """
    present = {r[0] for r in df.select("date_collecte").distinct().collect()}
    for d in sorted(set(target_dates) - present):
        delete_prefix(_partition_prefix(table, source, d), settings)
    if present:
        (
            df.repartition("date_collecte")  # 1 fichier par partition
            .write.mode("overwrite")
            .partitionBy("source", "date_collecte")
            .parquet(settings.s3_path(f"silver/{table}", scheme="s3a"))
        )
    return len(present)


def _publish_staging(
    staging: str, source: str, target_dates: list[date], settings: Settings
) -> None:
    """Copie côté serveur staging -> silver/offres, partition par partition."""
    for d in target_dates:
        final = _partition_prefix("offres", source, d)
        delete_prefix(final, settings)
        copy_prefix(f"{staging}source={source}/date_collecte={d}/", final, settings)
    delete_prefix(staging, settings)


class _Chrono:
    """Durée de chaque étape, affichée dans les logs (repérer une régression)."""

    def __init__(self):
        self.last = time.perf_counter()
        self.etapes: dict[str, float] = {}

    def __call__(self, etape: str) -> None:
        now = time.perf_counter()
        self.etapes[etape] = now - self.last
        print(f"[silver] étape {etape:<34} {now - self.last:6.0f} s", flush=True)
        self.last = now


def run(
    spark: SparkSession,
    settings: Settings,
    source: str,
    start: date,
    end: date,
    lookback_days: int = REPUBLICATION_WINDOW_DAYS,
    explain_path: Path | None = None,
    log_to_db: bool = True,
) -> tuple[dict, QualityReport]:
    """Write-audit-publish.

    1. WRITE   : lot valide -> silver/_staging/<source>/<start>_<end>/ ; rejets ->
                 silver/rejets (toujours écrits, pour diagnostiquer) ;
    2. AUDIT   : Great Expectations relit le Parquet de transit (chaque contrôle ne
                 lit que ses colonnes : jamais les descriptions en mémoire) ;
    3. PUBLISH : si la suite critique passe, copie côté serveur vers silver/offres ;
                 sinon le lot refusé reste en transit et silver/offres est inchangé.
    """
    t0 = time.perf_counter()
    chrono = _Chrono()
    bronze_path = settings.s3_path(f"bronze/source={source}", scheme="s3a")
    staging = f"silver/_staging/{source}/{start}_{end}/"
    in_range = F.col("date_collecte").between(F.lit(start), F.lit(end))

    # Les jours précédents sont relus pour que la fenêtre de republication voie les
    # occurrences antérieures ; seules les dates [start, end] sont écrites.
    bronze = spark.read.parquet(bronze_path).where(
        F.col("date").between(F.lit(start - timedelta(days=lookback_days)), F.lit(end))
    )
    # Cache sur disque : l'UDF n'est calculée qu'une fois pour toutes les écritures,
    # et le heap JVM (1,5 Go) n'héberge pas 124 000 descriptions
    built = build_silver(bronze, load_references(spark)).persist(StorageLevel.DISK_ONLY)
    try:
        base = built.where(in_range)
        n_base = base.count()
        chrono("construction + cache (UDF)")
        target_dates = [r[0] for r in base.select("date_collecte").distinct().collect()]
        # Toutes les observations (une offre vue tel jour), avant dédoublonnage :
        # une API d'ATS renvoie chaque jour les offres encore ouvertes, ce journal
        # permettra de mesurer combien de temps une offre reste en ligne.
        _write_partitions(
            base.select("offre_id", "source", "date_collecte").distinct(),
            "observations", source, target_dates, settings,
        )  # fmt: skip
        chrono("observations")

        # Second cache, APRÈS la fenêtre de dédoublonnage : sans lui, chaque action en
        # aval (motifs, comptages, deux écritures) refaisait ce brassage complet des
        # descriptions (job passé de 350 s à 915 s au jour 6)
        checked = apply_quality_rules(
            exclude_published(
                drop_exact_duplicates(built),
                _published_ids(spark, settings, source, before=start),
            )
        ).persist(StorageLevel.DISK_ONLY)
        checked.count()  # matérialise le cache avant de libérer le précédent
        chrono("dédoublonnage + règles + cache")
    finally:
        built.unpersist()
    try:
        valid_all, rejected_all = split_quarantine(checked)
        rejected = rejected_all.where(in_range)
        # Les rejets n'influencent pas le marquage des republications
        valid = mark_republications(valid_all).where(in_range)
        if explain_path:
            explain_path.parent.mkdir(parents=True, exist_ok=True)
            plan = valid._jdf.queryExecution().explainString(
                spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(
                    "formatted"
                )
            )
            explain_path.write_text(plan, encoding="utf-8")

        motifs = {
            r["motif"]: r["count"]
            for r in rejected.select(F.explode("_motifs_rejet").alias("motif"))
            .groupBy("motif")
            .count()
            .collect()
        }
        n_rejets = rejected.count()
        _write_partitions(rejected, "rejets", source, target_dates, settings)
        chrono("rejets (motifs, comptage, écriture)")

        delete_prefix(staging, settings)  # reste d'une exécution précédente
        # Écrit d'abord, compte ensuite en relisant le transit : compter `valid`
        # directement referait la fenêtre de republication une fois de plus
        (
            valid.repartition("date_collecte")
            .write.mode("overwrite")
            .partitionBy("source", "date_collecte")
            .parquet(settings.s3_path(staging, scheme="s3a"))
        )
        if any(k.endswith(".parquet") for k in list_keys(staging, settings)):
            staged = spark.read.parquet(settings.s3_path(staging, scheme="s3a"))
            n_valid = staged.count()
        else:
            # Aucune nouvelle offre (normal pour une source d'instantanés) : le
            # transit est vide, on valide le lot vide tel quel
            staged, n_valid = valid, 0
        chrono("republications + écriture transit")
        report = validate(staged, n_rejets, motifs, source)
        chrono("audit Great Expectations")
    finally:
        checked.unpersist()

    if log_to_db:
        log_report(report, source, start, end, settings)
    published = report.critical_ok
    if published:
        _publish_staging(staging, source, target_dates, settings)
        chrono("publication (copie serveur)")
        result = (
            spark.read.parquet(
                settings.s3_path(f"silver/offres/source={source}", scheme="s3a")
            ).where(in_range)
            if n_valid
            else None
        )
    else:
        result = staged if n_valid else None  # lot refusé, gardé pour diagnostic
    metrics = silver_metrics(result) if result is not None else {"lignes_silver": 0}

    n_bronze = (
        spark.read.parquet(bronze_path)
        .where(F.col("date").between(F.lit(start), F.lit(end)))
        .count()
    )
    metrics.update(
        duree_s=time.perf_counter() - t0,
        lignes_bronze=n_bronze,
        lignes_construites=n_base,
        lignes_deja_publiees=n_base - n_valid - n_rejets,
        lignes_rejetees=n_rejets,
        motifs_rejet=motifs,
        publie=published,
        transit=None if published else staging,
    )
    return metrics, report


def silver_metrics(df: DataFrame) -> dict:
    """Indicateurs de couverture, affichés après chaque run et notés dans le journal."""
    tech = F.col("famille_poste") != "AUTRE"
    has_salary = F.col("salaire_min_annuel").isNotNull()
    mid = (F.col("salaire_min_annuel") + F.col("salaire_max_annuel")) / 2
    agg = (
        df.agg(
            F.count("*").alias("lignes_silver"),
            F.countDistinct("date_collecte").alias("partitions"),
            F.avg(F.col("est_republication").cast("int")).alias("taux_republication"),
            F.avg(tech.cast("int")).alias("part_offres_tech"),
            F.avg((F.size("technologies") > 0).cast("int")).alias("couv_technologies"),
            F.avg(F.when(tech, (F.size("technologies") > 0).cast("int"))).alias(
                "couv_technologies_offres_tech"
            ),
            F.avg(F.col("experience_min_annees").isNotNull().cast("int")).alias(
                "couv_experience"
            ),
            F.avg(has_salary.cast("int")).alias("couv_salaire"),
            F.avg(
                F.when(
                    has_salary & F.col("salaire_normalise_source").isNotNull(),
                    (
                        F.abs(mid - F.col("salaire_normalise_source"))
                        <= 0.01 * F.col("salaire_normalise_source")
                    ).cast("int"),
                )
            ).alias("salaire_coherent_avec_source"),
            F.avg(F.col("ville_normalisee").isNotNull().cast("int")).alias(
                "couv_ville"
            ),
            F.avg(F.col("etat_code").isNotNull().cast("int")).alias("couv_etat"),
            F.countDistinct("offre_id").alias("offre_id_distincts"),
        )
        .first()
        .asDict()
    )
    agg["familles"] = {
        r["famille_poste"]: r["count"]
        for r in df.groupBy("famille_poste").count().collect()
    }
    agg["top_technologies"] = [
        (r["t"], r["count"])
        for r in df.select(F.explode("technologies").alias("t"))
        .groupBy("t")
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    ]
    return agg


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job Silver")
    p.add_argument("--source", default="kaggle_linkedin")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat)
    group.add_argument("--start", type=date.fromisoformat)
    group.add_argument("--all", action="store_true")
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--lookback-days", type=int, default=REPUBLICATION_WINDOW_DAYS)
    p.add_argument("--explain", action="store_true", help="écrit le plan physique")
    args = p.parse_args(argv)
    if args.start and not args.end:
        p.error("--start exige --end")
    return args


def print_report(report: QualityReport) -> None:
    for r in report.results:
        mark = "OK" if r["succes"] else ("KO" if r["suite"] == "critique" else "!!")
        detail = r["valeur_observee"] or (
            f"{r['nb_inattendus']} inattendus" if r["nb_inattendus"] else ""
        )
        print(f"  [{mark}] {r['suite']:<13} {r['controle']:<52} "
              f"{r['colonne'] or '':<34} {detail}")  # fmt: skip
        if not r["succes"] and r["exemples"]:
            print(f"        exemples : {r['exemples'][:150]}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    if args.date:
        start = end = args.date
    elif args.all:
        dates = bronze_dates(args.source, settings)
        if not dates:
            print(f"[silver] aucune partition Bronze pour {args.source}")
            return 1
        start, end = dates[0], dates[-1]
    else:
        start, end = args.start, args.end

    apply_schema(settings)
    spark = get_spark("silver", settings)
    try:
        m, report = run(
            spark,
            settings,
            args.source,
            start,
            end,
            args.lookback_days,
            Path("docs/plans/silver_plan.txt") if args.explain else None,
        )
    finally:
        spark.stop()

    print(f"\n[silver] {args.source} {start} -> {end} en {m.pop('duree_s'):.0f}s")
    print("[qualité] contrôles :")
    print_report(report)
    for k in ("familles", "top_technologies", "motifs_rejet"):
        if k in m:
            print(f"  {k}: {m.pop(k)}")
    published = m.pop("publie")
    transit = m.pop("transit")
    for k, v in m.items():
        if v is None:  # ex. concordance des salaires quand aucune offre n'en a
            print(f"  {k}: sans objet")
        else:
            print(f"  {k}: {v:.1%}" if isinstance(v, float) else f"  {k}: {v:,}")

    if not published:
        failed = [r["controle"] for r in report.failures("critique")]
        print(
            f"[silver] NON PUBLIÉ : {len(failed)} contrôle(s) critique(s) en échec "
            f"{failed} ; la version précédente de silver/offres est conservée."
        )
        print(f"[silver] lot refusé conservé pour diagnostic : {transit}")
        return 1
    accounted = m["lignes_silver"] + m["lignes_rejetees"] + m["lignes_deja_publiees"]
    ok = m["lignes_construites"] == m["lignes_bronze"] == accounted and m[
        "lignes_silver"
    ] == m.get("offre_id_distincts", 0)
    print(
        f"[silver] {'OK' if ok else 'ÉCART'} : Bronze ({m['lignes_bronze']:,}) = "
        f"nouvelles valides ({m['lignes_silver']:,}) + rejets "
        f"({m['lignes_rejetees']:,}) + déjà publiées ({m['lignes_deja_publiees']:,})"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
