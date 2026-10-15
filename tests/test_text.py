import pytest

from src.transform.text import (
    classify_family,
    clean_url,
    normalize_company,
    normalize_title,
    parse_location,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Sr. Data Engineer (Remote)", "senior data engineer"),
        ("Data Engineer - H/F", "data engineer"),
        ("Ingénieur Données [CDI]", "ingenieur donnees"),
        ("C++ Developer", "c++ developer"),
        ("  Jr   Data  Analyst ", "junior data analyst"),
        ("(Remote)", None),
        (None, None),
        # Écritures non latines : ne doivent pas devenir vides (bug trouvé au jour 5
        # grâce à la quarantaine ; la source marocaine aura des titres en arabe)
        ("مهندس بيانات", "مهندس بيانات"),
        ("مُهَنْدِس", "مهندس"),  # voyelles courtes (diacritiques) retirées
        ("海外社媒运营", "海外社媒运营"),
        ("מחסנאי.ת", "מחסנאי ת"),
        ("지역 영업 관리자", "지역 영업 관리자"),  # syllabes recomposées (NFC)
        ("![", None),
    ],
)
def test_normalize_title(raw, expected):
    assert normalize_title(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("COMPA Industries, Inc.", "compa industries"),
        ("Marina Sirras & Associates LLC", "marina sirras and associates"),
        ("The Home Depot", "home depot"),
        ("Capital One", "capital one"),
        ("Société Générale S.A.", "societe generale"),
        ("Inc", "inc"),  # on ne vide jamais complètement le nom
        ("المكتب الشريف للفوسفاط", "المكتب الشريف للفوسفاط"),
    ],
)
def test_normalize_company(raw, expected):
    assert normalize_company(raw) == expected


def test_clean_url_removes_tracking():
    assert (
        clean_url("https://www.linkedin.com/jobs/view/3778723781/?trk=jobs_biz")
        == "https://www.linkedin.com/jobs/view/3778723781"
    )
    assert clean_url(None) is None


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Austin, TX", ("Austin", "TX", "US")),
        ("Portland, Oregon Metropolitan Area", ("Portland", "Oregon", "US")),
        ("Mobile, Alabama Area", ("Mobile", "Alabama", "US")),
        ("Los Angeles, California, United States", ("Los Angeles", "California", "US")),
        ("Chicago, United States", ("Chicago", None, "US")),
        ("United States", (None, None, "US")),
        ("NAMER", (None, None, None)),
        ("Greater Houston", ("Houston", None, "US")),
        ("San Francisco Bay Area", ("San Francisco", None, "US")),
        ("Dallas-Fort Worth Metroplex", ("Dallas", None, "US")),
        ("Greensboro--Winston-Salem--High Point Area", ("Greensboro", None, "US")),
        ("Winston-Salem, NC", ("Winston-Salem", "NC", "US")),
        (None, (None, None, None)),
    ],
)
def test_parse_location(raw, expected):
    assert parse_location(raw) == expected


@pytest.mark.parametrize(
    "title, family",
    [
        ("Senior Data Engineer", "DATA_ENG"),
        ("ETL Developer", "DATA_ENG"),
        ("Data Scientist II", "DATA_SCI"),
        ("Software Engineer, Machine Learning", "AI_ML"),
        ("AI Engineer", "AI_ML"),
        ("Business Intelligence Analyst", "BI"),
        ("Data Analyst", "BI"),
        ("Power BI Developer", "BI"),
        ("Full Stack Developer", "DEV"),
        ("DevOps Engineer", "DEV"),
        ("Business Development Representative", "AUTRE"),
        ("Software Sales Executive", "AUTRE"),
        ("Real Estate Developer", "AUTRE"),
        ("Registered Nurse", "AUTRE"),
        ("Maintenance Technician", "AUTRE"),
        ("Ingénieur Data", "DATA_ENG"),
        (None, "AUTRE"),
        # Intitulés français des offres marocaines (jour 6)
        ("Architecte Data & IA (H/F)", "AI_ML"),
        ("Ingénieur IA Générative", "AI_ML"),
        ("Architecte de données", "DATA_ENG"),
        ("Consultant Décisionnel", "BI"),
        ("Ingénieur Cloud AWS Anglophone", "DEV"),
        ("Ingénieur Études et Développement Java", "DEV"),
        ("Tech Lead Web", "DEV"),
        ("Registered Nurse - Des Moines, IA", "AUTRE"),  # IA = Iowa, pas l'IA
    ],
)
def test_classify_family(title, family):
    assert classify_family(title) == family
