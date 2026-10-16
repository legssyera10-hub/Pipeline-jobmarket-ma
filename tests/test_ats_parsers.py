"""Parseurs Greenhouse / Lever sur des réponses synthétiques (structure réelle)."""

import json
from pathlib import Path

import pytest

from src.ingestion.ats.parsers import (
    BRONZE_COLUMNS,
    SchemaDriftError,
    html_to_text,
    map_work_type,
    parse_greenhouse,
    parse_lever,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ats"


@pytest.fixture
def greenhouse():
    rows = parse_greenhouse(
        json.loads((FIXTURES / "greenhouse.json").read_text(encoding="utf-8")),
        "Acme",
        "acme",
    )
    return {r["job_id"]: r for r in rows}


@pytest.fixture
def lever():
    rows = parse_lever(
        json.loads((FIXTURES / "lever.json").read_text(encoding="utf-8")),
        "Acme Maghreb",
        "acme",
    )
    return {r["job_id"]: r for r in rows}


def test_greenhouse_keeps_only_morocco(greenhouse):
    # 102 (Paris) exclue ; 103 retenue grâce à son bureau de Rabat
    assert set(greenhouse) == {"greenhouse:acme:101", "greenhouse:acme:103"}


def test_greenhouse_fields(greenhouse):
    r = greenhouse["greenhouse:acme:101"]
    assert r["company_name"] == "Acme"
    assert r["location"] == "Tour Atlas, Boulevard Zerktouni, Casablanca, Maroc"
    assert r["pays_code"] == "MA"
    assert r["original_listed_time"] == "2026-09-30T23:30:00-04:00"  # first_published
    assert r["work_type"] == "FULL_TIME"
    assert r["seniorite_source"] == "Junior"
    assert r["departement"] == "Data Engineering"


def test_greenhouse_html_is_unescaped_then_stripped(greenhouse):
    desc = greenhouse["greenhouse:acme:101"]["description"]
    assert desc == "Missions\nPipelines Spark et Airflow\n2 ans d'expérience en Python"
    assert "<" not in desc and "&" not in desc


def test_greenhouse_office_used_when_location_is_vague(greenhouse):
    r = greenhouse["greenhouse:acme:103"]
    assert r["location"] == "Rabat"
    assert r["work_type"] == "INTERNSHIP"


def test_lever_morocco_in_secondary_locations(lever):
    # Lieu principal Alger, mais Casablanca parmi allLocations : retenue
    r = lever["lever:acme:aaaa-1111"]
    assert r["location"] == "Casablanca, Morocco"
    assert r["toutes_localisations"] == (
        "Algiers, Algeria | Casablanca, Morocco | Tunis, Tunisia"
    )
    assert r["workplace_type"] == "hybrid"
    assert r["original_listed_time"] == "1772536023579"


def test_lever_excludes_offers_without_morocco(lever):
    assert "lever:acme:bbbb-2222" not in lever


def test_lever_country_code_alone_is_enough(lever):
    r = lever["lever:acme:cccc-3333"]
    assert r["location"] == "Morocco"
    assert r["remote_allowed"] == "1.0"
    assert (r["min_salary"], r["max_salary"], r["pay_period"], r["currency"]) == (
        "10", "15", "HOURLY", "USD",
    )  # fmt: skip


def test_lever_description_combines_all_sections(lever):
    desc = lever["lever:acme:aaaa-1111"]["description"]
    assert desc.startswith("Acme Maghreb est une super-app.")
    # Le gras au milieu d'une phrase ne la coupe pas ; chaque <li> est une ligne
    assert "Responsabilités" in desc and "Développer en Kotlin\n" in desc
    assert desc.endswith("Nous valorisons la diversité.")


def test_every_record_has_the_fixed_bronze_columns(greenhouse, lever):
    for r in [*greenhouse.values(), *lever.values()]:
        assert list(r) == BRONZE_COLUMNS
        assert all(v is None or isinstance(v, str) for v in r.values())


@pytest.mark.parametrize(
    "raw, code",
    [
        ("Full-time", "FULL_TIME"), ("Fulltime", "FULL_TIME"), ("CDI", "FULL_TIME"),
        ("Part-time", "PART_TIME"), ("Contractor", "CONTRACT"),
        ("Freelancer (Hourly Rate)", "CONTRACT"), ("CDD", "CONTRACT"),
        ("Stage", "INTERNSHIP"), ("Internship", "INTERNSHIP"),
        ("Alternance", "INTERNSHIP"), ("Bénévolat", "OTHER"), (None, None),
    ],
)  # fmt: skip
def test_work_type_mapping(raw, code):
    assert map_work_type(raw) == code


def test_html_to_text_empty():
    assert html_to_text(None) is None and html_to_text("<p> </p>") is None


# --- Changement de format de l'API (jour 7) ------------------------------------------


def test_lever_renamed_field_raises_explicit_error():
    body = json.loads((FIXTURES / "lever.json").read_text(encoding="utf-8"))
    for post in body:
        post["title"] = post.pop("text")  # l'API « renomme » le champ
    with pytest.raises(SchemaDriftError, match=r"'text' absent de 3/3 offres"):
        parse_lever(body, "Acme", "acme")


def test_greenhouse_without_jobs_key_raises():
    with pytest.raises(SchemaDriftError, match="'jobs'"):
        parse_greenhouse({"postings": []}, "Acme", "acme")


def test_empty_answer_is_not_a_schema_drift():
    assert parse_lever([], "Acme", "acme") == []
    assert parse_greenhouse({"jobs": []}, "Acme", "acme") == []
