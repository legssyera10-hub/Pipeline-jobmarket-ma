"""Suites Great Expectations : un lot propre passe, chaque corruption est détectée."""

from datetime import date

import pytest

from src.quality.expectations import validate

pytestmark = pytest.mark.spark

SCHEMA = (
    "offre_id string, source string, titre_normalise string, date_publication date,"
    " date_collecte date, date_expiration date, famille_poste string,"
    " type_contrat string, teletravail string, niveau_experience string,"
    " etat_code string, experience_min_annees int, ville_normalisee string,"
    " entreprise_normalisee string, salaire_min_annuel int, devise string"
)
D = date(2024, 4, 9)
N = 20


def clean_rows(n=N):
    return [
        {
            "offre_id": f"id{i}",
            "source": "test",
            "titre_normalise": "data engineer",
            "date_publication": D,
            "date_collecte": D,
            "date_expiration": date(2024, 5, 9),
            "famille_poste": "DATA_ENG",
            "type_contrat": "FULL_TIME",
            "teletravail": "NON_PRECISE",
            "niveau_experience": "CONFIRME",
            "etat_code": "TX",
            "experience_min_annees": 3,
            "ville_normalisee": "Austin",
            "entreprise_normalisee": "acme",
            "salaire_min_annuel": 90_000,
            "devise": "USD",
        }  # fmt: skip
        for i in range(n)
    ]


@pytest.fixture
def check(spark):
    def _check(rows, n_rejets=0):
        df = spark.createDataFrame([tuple(r.values()) for r in rows], SCHEMA)
        return validate(df, n_rejets, {})

    return _check


def failed(report, suite="critique"):
    return {(r["controle"], r["colonne"]) for r in report.failures(suite)}


def test_clean_batch_passes(check):
    report = check(clean_rows())
    assert report.critical_ok, failed(report)
    assert not failed(report, "avertissement")


def test_duplicate_ids_block(check):
    rows = clean_rows()
    rows[1]["offre_id"] = rows[0]["offre_id"]
    report = check(rows)
    assert not report.critical_ok
    assert ("expect_column_values_to_be_unique", "offre_id") in failed(report)


def test_unknown_value_blocks(check):
    rows = clean_rows()
    rows[0]["famille_poste"] = "MARKETING"
    rows[1]["etat_code"] = "ZZ"
    assert {c for _, c in failed(check(rows))} >= {"famille_poste", "etat_code"}


def test_fill_rate_drift_blocks(check):
    # Chaque ligne reste valide, mais la moitié des villes ont disparu : format changé
    rows = clean_rows()
    for r in rows[: N // 2]:
        r["ville_normalisee"] = None
    assert ("expect_column_values_to_not_be_null", "ville_normalisee") in failed(
        check(rows)
    )


def test_publication_after_collection_blocks(check):
    rows = clean_rows()
    rows[0]["date_publication"] = date(2024, 4, 10)
    assert any("date_collecte >= date_publication" == c for _, c in failed(check(rows)))


def test_rejection_rate_threshold(check):
    report = check(clean_rows(), n_rejets=5)  # 5 / 25 = 20 % > 5 %
    assert ("taux_de_rejet_maximal", None) in failed(report)


def test_warnings_do_not_block(check):
    rows = clean_rows()
    for r in rows:
        r["salaire_min_annuel"] = 15  # aberrant partout
    report = check(rows)
    assert report.critical_ok
    assert ("expect_column_values_to_be_between", "salaire_min_annuel") in failed(
        report, "avertissement"
    )


def test_ats_profile_accepts_empty_batch_and_no_us_state(spark):
    # Source d'instantanés : un jour sans NOUVELLE offre est normal
    empty = spark.createDataFrame([], SCHEMA)
    assert validate(empty, 0, {}, "ats_maroc").critical_ok
    rows = clean_rows()
    for r in rows:
        r["etat_code"] = None  # pas d'état au Maroc
    assert validate(
        spark.createDataFrame([tuple(r.values()) for r in rows], SCHEMA),
        0, {}, "ats_maroc",
    ).critical_ok  # fmt: skip
