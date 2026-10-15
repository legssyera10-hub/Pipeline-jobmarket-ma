"""Job Silver (SparkSession locale) : transformations, broadcast, republications."""

import hashlib
from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.transform.silver import (
    build_silver,
    drop_exact_duplicates,
    exclude_published,
    load_references,
    mark_republications,
)

pytestmark = pytest.mark.spark

BRONZE_COLS = [
    "job_id", "company_name", "title", "description", "max_salary", "pay_period",
    "location", "med_salary", "min_salary", "original_listed_time", "remote_allowed",
    "job_posting_url", "expiry", "formatted_experience_level", "skills_desc",
    "listed_time", "work_type", "currency", "normalized_salary",
]  # fmt: skip
D = "1712620800000.0"  # 2024-04-09 00:00 UTC


def bronze_row(**over):
    row = dict.fromkeys(BRONZE_COLS)
    row.update(
        job_id="1",
        company_name="Acme, Inc.",
        title="Sr. Data Engineer",
        description="5+ years of experience with Python, SQL and Spark.",
        location="Austin, TX",
        original_listed_time=D,
        listed_time=D,
        expiry="1715212800000.0",
        job_posting_url="https://www.linkedin.com/jobs/view/1/?trk=x",
        work_type="FULL_TIME",
        formatted_experience_level="Mid-Senior level",
    )
    row.update(over)
    return row


@pytest.fixture(scope="module")
def silver_of(spark):
    refs = load_references(spark)

    def _build(*rows):
        # Schéma explicite : comme en Bronze, tout est texte (et des colonnes vides
        # partout empêcheraient l'inférence)
        schema = ", ".join(f"{c} string" for c in BRONZE_COLS)
        data = [[r[c] for c in BRONZE_COLS] for r in rows]
        bronze = spark.createDataFrame(data, schema).select(
            *BRONZE_COLS,
            F.lit("2026-10-12 00:00:00").cast("timestamp").alias("_ingested_at"),
            F.lit("kaggle_linkedin").alias("_source"),
            F.lit(date(2024, 4, 9)).alias("_run_date"),
        )
        return build_silver(bronze, refs)

    return _build


def test_typed_and_normalized_fields(silver_of):
    r = silver_of(bronze_row()).first()
    url = "https://www.linkedin.com/jobs/view/1"
    assert r.url == url
    assert r.offre_id == hashlib.sha256(f"kaggle_linkedin|{url}".encode()).hexdigest()
    assert r.date_publication == date(2024, 4, 9)
    assert r.date_expiration == date(2024, 5, 9)
    assert r.titre_normalise == "senior data engineer"
    assert r.famille_poste == "DATA_ENG"
    assert r.entreprise_normalisee == "acme"
    assert (r.ville_normalisee, r.etat_code, r.etat_nom, r.region) == (
        "Austin", "TX", "Texas", "South",
    )  # fmt: skip
    assert r.technologies == ["python", "spark", "sql"]
    assert (r.experience_min_annees, r.experience_max_annees) == (5, None)
    assert r.type_contrat == "FULL_TIME"
    assert r.niveau_experience == "CONFIRME"
    assert r.teletravail == "NON_PRECISE"  # remote_allowed vide != sur site


def test_offre_id_ignores_tracking_parameters(silver_of):
    a, b = silver_of(
        bronze_row(job_posting_url="https://x/jobs/view/9/?trk=a"),
        bronze_row(job_id="2", job_posting_url="https://x/jobs/view/9/?trk=b"),
    ).collect()
    assert a.offre_id == b.offre_id


@pytest.mark.parametrize(
    "location, expected",
    [
        ("Greater Houston", ("Houston", "TX", "South")),  # référentiel métropoles
        ("Portland, Oregon Metropolitan Area", ("Portland", "OR", "West")),
        ("Los Angeles, California, United States", ("Los Angeles", "CA", "West")),
        ("United States", (None, None, None)),
    ],
)
def test_location_resolution(silver_of, location, expected):
    r = silver_of(bronze_row(location=location)).first()
    assert (r.ville_normalisee, r.etat_code, r.region) == expected


@pytest.mark.parametrize(
    "salary, expected",
    [
        ({"min_salary": "25.0", "max_salary": "30.0", "pay_period": "HOURLY"},
         (52000, 62400)),
        ({"med_salary": "5000.0", "pay_period": "MONTHLY"}, (60000, 60000)),
        ({"min_salary": "90000.0", "max_salary": "120000.0", "pay_period": "YEARLY"},
         (90000, 120000)),
        ({}, (None, None)),
    ],
)  # fmt: skip
def test_salary_is_annualized(silver_of, salary, expected):
    r = silver_of(bronze_row(**salary)).first()
    assert (r.salaire_min_annuel, r.salaire_max_annuel) == expected


def test_remote_and_hybrid(silver_of):
    rows = silver_of(
        bronze_row(job_id="1", remote_allowed="1.0"),
        bronze_row(job_id="2", title="Data Analyst - Hybrid", job_posting_url="u2"),
    ).orderBy("id_source")
    assert [r.teletravail for r in rows.collect()] == ["TELETRAVAIL", "HYBRIDE"]


def test_reference_joins_are_broadcast(silver_of):
    plan = silver_of(bronze_row())._jdf.queryExecution().executedPlan().toString()
    assert plan.count("BroadcastHashJoin") == 2
    assert "SortMergeJoin" not in plan


# --- Republications (fenêtre glissante) --------------------------------------------


@pytest.fixture
def offers(spark):
    def _make(*rows):
        return spark.createDataFrame(
            [
                (oid, ent, "data engineer", ville, date.fromisoformat(d))
                for oid, ent, ville, d in rows
            ],
            "offre_id string, entreprise_normalisee string, titre_normalise string,"
            " ville_normalisee string, date_publication date",
        )

    return _make


def flags(df):
    return {
        r.offre_id: (r.est_republication, r.date_occurrence_precedente)
        for r in mark_republications(df).collect()
    }


def test_republication_within_window(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-03-01"), ("b", "acme", "Austin", "2024-03-20")
        )
    )
    assert f == {"a": (False, None), "b": (True, date(2024, 3, 1))}


def test_reopened_after_window_is_a_new_offer(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-01-01"), ("b", "acme", "Austin", "2024-03-01")
        )
    )
    assert f["b"] == (False, None)


def test_window_boundary_is_inclusive(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-03-01"), ("b", "acme", "Austin", "2024-03-31")
        )
    )
    assert f["b"][0] is True  # exactement 30 jours


def test_window_slides_from_previous_occurrence(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-01-01"),
            ("b", "acme", "Austin", "2024-01-25"),
            ("c", "acme", "Austin", "2024-02-20"),  # 56 j après a, 26 j après b
        )
    )
    assert [f[k][0] for k in "abc"] == [False, True, True]


def test_same_day_duplicates_keep_one_original(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-03-01"), ("b", "acme", "Austin", "2024-03-01")
        )
    )
    assert sorted(v[0] for v in f.values()) == [False, True]


def test_other_city_or_company_is_not_a_republication(offers):
    f = flags(
        offers(
            ("a", "acme", "Austin", "2024-03-01"),
            ("b", "acme", "Dallas", "2024-03-02"),
            ("c", "globex", "Austin", "2024-03-02"),
        )
    )
    assert not any(v[0] for v in f.values())


def test_incomplete_key_is_never_flagged(offers):
    f = flags(
        offers(("a", None, "Austin", "2024-03-01"), ("b", None, "Austin", "2024-03-02"))
    )
    assert not any(v[0] for v in f.values())


def test_does_not_depend_on_today(offers):
    # Données de 2020 : un filtre current_date() - 30 j les aurait toutes supprimées
    df = offers(
        ("a", "acme", "Austin", "2020-01-01"), ("b", "acme", "Austin", "2020-01-05")
    )
    out = mark_republications(df)
    assert out.count() == 2


def test_exact_duplicates_keep_first_collection(spark):
    df = spark.createDataFrame(
        [
            Row(
                offre_id="x", date_collecte=date(2024, 4, 10), _ingested_at=1, v="late"
            ),
            Row(
                offre_id="x", date_collecte=date(2024, 4, 9), _ingested_at=1, v="first"
            ),
        ]
    )
    assert [r.v for r in drop_exact_duplicates(df).collect()] == ["first"]


# --- Source ATS marocaine (jour 6) -------------------------------------------------


@pytest.fixture(scope="module")
def ats_silver(spark):
    refs = load_references(spark)
    cols = ["job_id", "company_name", "title", "description", "location",
            "pays_code", "job_posting_url", "original_listed_time", "listed_time",
            "work_type", "workplace_type"]  # fmt: skip

    def _build(**over):
        row = dict(
            job_id="greenhouse:acme:1", company_name="Acme",
            title="Architecte Data & IA (H/F)",
            description="5 ans d'expérience en Python et Spark sur Azure.",
            location="Tour Atlas, Casablanca, Maroc", pays_code="MA",
            job_posting_url="https://job-boards.greenhouse.io/acme/jobs/1",
            original_listed_time="2026-09-30T23:30:00-04:00",
            listed_time="1772536023579", work_type="FULL_TIME",
            workplace_type="hybrid",
        )  # fmt: skip
        row.update(over)
        bronze = spark.createDataFrame(
            [[row[c] for c in cols]], ", ".join(f"{c} string" for c in cols)
        ).select(
            *cols,
            F.lit("2026-10-15 06:00:00").cast("timestamp").alias("_ingested_at"),
            F.lit("ats_maroc").alias("_source"),
            F.lit(date(2026, 10, 15)).alias("_run_date"),
        )
        return build_silver(bronze, refs).first()

    return _build


def test_ats_row_is_enriched_with_moroccan_geography(ats_silver):
    r = ats_silver()
    assert (r.ville_normalisee, r.region, r.pays, r.etat_code) == (
        "Casablanca", "Casablanca-Settat", "MA", None,
    )  # fmt: skip
    assert r.famille_poste == "AI_ML"
    assert r.teletravail == "HYBRIDE"
    assert r.technologies == ["azure", "python", "spark"]
    assert r.experience_min_annees == 5


def test_iso_dates_with_offset_and_epoch_ms_are_both_parsed(ats_silver):
    r = ats_silver()
    # 2026-09-30 23:30 à UTC-4 = 2026-10-01 03:30 UTC
    assert r.date_publication == date(2026, 10, 1)
    assert r.date_derniere_publication == date(2026, 3, 3)  # epoch ms (Lever)


def test_morocco_without_city(ats_silver):
    r = ats_silver(location="Remote - Morocco", workplace_type="remote")
    assert (r.ville_normalisee, r.pays, r.teletravail) == (None, "MA", "TELETRAVAIL")


def test_already_published_offers_are_excluded(spark):
    df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], "offre_id string, v int")
    published = spark.createDataFrame([("b",), ("b",)], "offre_id string")
    kept = {r.offre_id for r in exclude_published(df, published).collect()}
    assert kept == {"a", "c"}
    assert exclude_published(df, None).count() == 3  # première exécution
