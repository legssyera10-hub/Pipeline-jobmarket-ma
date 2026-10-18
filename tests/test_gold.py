"""Tables Gold calculées sur un petit Silver dont on connaît les réponses."""

from datetime import date

import pytest

from src.transform.gold import (
    cooccurrence_tech,
    experience_demandee,
    offres_par_ville,
    salaires_par_famille,
    tech_par_mois,
)

pytestmark = pytest.mark.spark

SCHEMA = (
    "source string, offre_id string, date_publication date, famille_poste string,"
    " ville_normalisee string, etat_code string, experience_min_annees int,"
    " technologies array<string>, est_republication boolean"
)
APR = date(2024, 4, 9)


@pytest.fixture
def silver(spark):
    rows = [
        # offre_id, famille, ville, etat, exp, technos, republication
        ("o1", "DATA_ENG", "Austin", "TX", 3, ["python", "spark", "sql"], False),
        ("o2", "DATA_ENG", "Austin", "TX", 5, ["python", "sql"], False),
        ("o3", "AUTRE", None, None, None, ["excel"], False),
        ("o4", "DATA_ENG", "Austin", "TX", 3, ["python", "spark", "sql"], True),
        (
            "o5",
            "BI",
            "Dallas",
            "TX",
            12,
            ["sql", "sql"],
            False,
        ),  # doublon dans le tableau
        ("o6", "DEV", "Dallas", "TX", 0, [], False),
    ]
    return spark.createDataFrame(
        [("test", o, APR, f, v, e, x, t, r) for o, f, v, e, x, t, r in rows], SCHEMA
    )


def as_dict(df, keys, value):
    return {tuple(r[k] for k in keys): r[value] for r in df.collect()}


def test_tech_par_mois_counts_unique_offers_and_announcements(silver):
    df = tech_par_mois(silver)
    toutes = df.where("perimetre = 'toutes'")
    offres = as_dict(toutes, ["technologie"], "nb_offres")
    annonces = as_dict(toutes, ["technologie"], "nb_annonces")
    assert offres[("python",)] == 2 and annonces[("python",)] == 3  # o4 = republication
    # part_relative = offres citant python / offres uniques du mois (5 : o1,o2,o3,o5,o6)
    parts = as_dict(toutes, ["technologie"], "part_relative")
    assert parts[("python",)] == pytest.approx(2 / 5)


def test_tech_scope_excludes_non_tech_offers(silver):
    tech = tech_par_mois(silver).where("perimetre = 'tech'")
    assert "excel" not in {r.technologie for r in tech.collect()}
    parts = as_dict(tech, ["technologie"], "part_relative")
    assert parts[("python",)] == pytest.approx(2 / 4)  # o1,o2,o5,o6 sont tech


def test_unknown_city_gets_explicit_label(silver):
    df = offres_par_ville(silver)
    assert ("Non précisée", "NA") in {(r.ville, r.etat_code) for r in df.collect()}


@pytest.mark.parametrize(
    "offre, tranche",
    [("o6", "0-1 an"), ("o1", "2-3 ans"), ("o2", "4-5 ans"), ("o5", "10 ans et +"),
     ("o3", "Non précisé")],
)  # fmt: skip
def test_experience_brackets(spark, silver, offre, tranche):
    one = silver.where(f"offre_id = '{offre}'")
    assert [r.tranche for r in experience_demandee(one).collect()] == [tranche]


def test_experience_shares_sum_to_one_per_family(silver):
    df = experience_demandee(silver)
    for fam, total in as_dict(
        df.groupBy("famille_poste").sum("part_famille"),
        ["famille_poste"],
        "sum(part_famille)",
    ).items():
        assert total == pytest.approx(1.0), fam


def test_cooccurrence_pairs(silver):
    df = cooccurrence_tech(silver)
    pairs = {(r.tech_a, r.tech_b): r for r in df.collect()}
    # Chaque paire une seule fois (a < b), pas de paire d'une techno avec elle-même
    assert all(a < b for a, b in pairs)
    p = pairs[("python", "sql")]
    assert p.nb_offres_communes == 2  # o1, o2 (o4 est une republication)
    assert (p.nb_offres_a, p.nb_offres_b) == (2, 3)  # sql : o1, o2, o5
    assert p.jaccard == pytest.approx(2 / (2 + 3 - 2))
    assert p.lift == pytest.approx(2 * 5 / (2 * 3))  # 5 offres uniques au total
    assert ("sql", "sql") not in pairs


SAL_SCHEMA = (
    "source string, offre_id string, famille_poste string, niveau_experience string,"
    " salaire_min_annuel double, salaire_max_annuel double, devise string,"
    " _anomalies array<string>, est_republication boolean"
)


def test_salaires_couverture_et_quartiles(spark):
    rows = [
        ("o1", "DATA_ENG", "Senior", 100.0, 120.0, "USD", [], False),  # milieu 110
        ("o2", "DATA_ENG", "Senior", 80.0, 100.0, "USD", None, False),  # 90
        ("o3", "DATA_ENG", "Senior", None, None, None, [], False),  # pas de salaire
        ("o4", "DATA_ENG", "Senior", 5.0, 6.0, "USD", ["salaire_hors_bornes"], False),
        ("o5", "DATA_ENG", "Senior", 100.0, 120.0, "USD", [], True),  # republication
        ("o6", "DATA_ENG", None, 50.0, 70.0, "EUR", [], False),  # devise non USD
    ]
    df = spark.createDataFrame([("t", *r) for r in rows], SAL_SCHEMA)
    out = {r["niveau_experience"]: r for r in salaires_par_famille(df).collect()}

    senior = out["Senior"]
    assert senior["nb_offres"] == 4  # o5 (republication) exclue
    assert senior["nb_avec_salaire"] == 2  # o4 en anomalie, o3 sans salaire
    assert senior["taux_couverture"] == pytest.approx(0.5)
    assert senior["nb_salaires_usd"] == 2
    assert senior["salaire_median_usd"] in (90.0, 110.0)  # percentile approché
    assert senior["salaire_p25_usd"] <= senior["salaire_p75_usd"]

    inconnu = out["Non précisé"]  # niveau absent : libellé explicite
    assert inconnu["nb_avec_salaire"] == 1 and inconnu["nb_salaires_usd"] == 0
    assert inconnu["salaire_median_usd"] is None  # aucune médiane inventée
