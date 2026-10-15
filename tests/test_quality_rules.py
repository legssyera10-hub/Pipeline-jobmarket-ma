"""Quarantaine : aucune perte, motifs cumulés, anomalies non bloquantes."""

from datetime import date

import pytest

from src.quality.rules import apply_quality_rules, split_quarantine

pytestmark = pytest.mark.spark

SCHEMA = (
    "offre_id string, titre_normalise string, date_publication date,"
    " date_collecte date, date_expiration date, salaire_min_annuel int,"
    " salaire_max_annuel int, devise string, experience_min_annees int,"
    " experience_max_annees int"
)
D = date(2024, 4, 9)


def row(oid, **over):
    r = dict(
        offre_id=oid, titre_normalise="data engineer", date_publication=D,
        date_collecte=D, date_expiration=date(2024, 5, 9), salaire_min_annuel=None,
        salaire_max_annuel=None, devise=None, experience_min_annees=None,
        experience_max_annees=None,
    )  # fmt: skip
    r.update(over)
    return tuple(r.values())


@pytest.fixture
def checked(spark):
    def _make(*rows):
        valid, rejected = split_quarantine(
            apply_quality_rules(spark.createDataFrame(list(rows), SCHEMA))
        )
        return (
            {r.offre_id: r for r in valid.collect()},
            {r.offre_id: r for r in rejected.collect()},
        )

    return _make


def test_null_date_goes_to_quarantine_instead_of_vanishing(checked):
    # Bug du plan : filter(cond) et filter(~cond) écartaient TOUS DEUX cette ligne
    valid, rejected = checked(row("ok"), row("sans_date", date_publication=None))
    assert set(valid) == {"ok"}
    assert rejected["sans_date"]._motifs_rejet == ["date_publication_manquante"]


def test_every_row_lands_in_exactly_one_side(checked):
    rows = [
        row("a"),
        row("b", titre_normalise=None),
        row("c", date_publication=None, date_collecte=None),
        row("d", salaire_min_annuel=5),
    ]
    valid, rejected = checked(*rows)
    assert set(valid) | set(rejected) == {"a", "b", "c", "d"}
    assert not set(valid) & set(rejected)


def test_all_reasons_are_kept(checked):
    _, rejected = checked(row("x", titre_normalise=None, date_publication=None))
    assert rejected["x"]._motifs_rejet == [
        "titre_manquant",
        "date_publication_manquante",
    ]


def test_publication_after_collection_is_rejected(checked):
    _, rejected = checked(row("x", date_publication=date(2024, 4, 10)))
    assert rejected["x"]._motifs_rejet == ["publication_apres_collecte"]


def test_does_not_depend_on_today(checked):
    # 2030 n'est « dans le futur » que par rapport à aujourd'hui, pas à la collecte
    future = row(
        "x",
        date_publication=date(2030, 1, 1),
        date_collecte=date(2030, 1, 2),
        date_expiration=None,
    )
    valid, _ = checked(future)
    assert "x" in valid


@pytest.mark.parametrize(
    "over, anomaly",
    [
        ({"salaire_min_annuel": 15, "salaire_max_annuel": 20, "devise": "USD"},
         "salaire_hors_bornes"),
        ({"salaire_min_annuel": 50_000, "salaire_max_annuel": 41_533_440,
          "devise": "USD"}, "salaire_hors_bornes"),
        ({"salaire_min_annuel": 90_000, "salaire_max_annuel": 80_000, "devise": "USD"},
         "salaire_min_superieur_max"),
        ({"salaire_min_annuel": 60_000, "salaire_max_annuel": 70_000, "devise": "EUR"},
         "salaire_devise_non_usd"),
        ({"date_expiration": date(2024, 4, 1)}, "expiration_avant_publication"),
    ],
)  # fmt: skip
def test_anomalies_flag_but_do_not_reject(checked, over, anomaly):
    valid, rejected = checked(row("x", **over))
    assert not rejected
    assert anomaly in valid["x"]._anomalies


def test_clean_row_has_empty_arrays(checked):
    valid, _ = checked(
        row("x", salaire_min_annuel=60_000, salaire_max_annuel=80_000, devise="USD")
    )
    assert valid["x"]._anomalies == []
    assert "_motifs_rejet" not in valid["x"].asDict()
