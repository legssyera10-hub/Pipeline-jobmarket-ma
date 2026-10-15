"""Extraction des technologies : pièges de délimitation et d'homonymie."""

import pytest

from src.transform.technologies import extract_technologies, load_patterns


def techs(text):
    return extract_technologies(text)


def test_reference_has_about_150_entries():
    assert len(load_patterns()) >= 150


@pytest.mark.parametrize(
    "text, present, absent",
    [
        # Délimitation
        ("Python, SQL and Spark", {"python", "sql", "spark"}, set()),
        ("a sparkling personality", set(), {"spark"}),
        ("Pythonic thinking", set(), {"python"}),
        ("Strong C++ and C# skills", {"cpp", "csharp"}, {"c"}),
        ("Experience with C/C++", {"c", "cpp"}, set()),
        ("ASP.NET and .NET Core", {"dotnet"}, set()),
        ("Node.js backend", {"node_js"}, {"javascript"}),
        # Java vs JavaScript
        ("JavaScript and TypeScript", {"javascript", "typescript"}, {"java"}),
        ("Java, Spring Boot", {"java", "spring"}, set()),
        ("JAVASCRIPT developer", {"javascript"}, {"java"}),
        # SQL : pas dans NoSQL / MySQL / PostgreSQL
        ("NoSQL databases", {"nosql"}, {"sql"}),
        ("MySQL and PostgreSQL", {"mysql", "postgresql"}, {"sql"}),
        # Texte collé par le retrait du HTML
        ("skills in Rust and PythonLinux systems", {"rust", "python", "linux"}, set()),
        ("relevant experienceSQL Server", {"sql_server"}, set()),
        ("ALL CAPS: SPARKS FLY, LAWS APPLY", set(), {"spark", "aws"}),
        # R
        ("Python, R, SQL", {"r", "python", "sql"}, set()),
        ("Statistical tools (R programming)", {"r"}, set()),
        ("Our R&D team", set(), {"r"}),
        ("Contact R. Smith", set(), {"r"}),
        # Go
        ("Python, Go, Rust", {"go", "python", "rust"}, set()),
        ("Golang microservices", {"go"}, set()),
        ("Go to market strategy; ready to go", set(), {"go"}),
        # Homonymes du langage courant
        ("Ability to excel in a fast-paced environment", set(), {"excel"}),
        ("Advanced Excel and Power BI", {"excel", "power_bi"}, set()),
        ("monitor airflow in HVAC systems", set(), {"airflow"}),
        ("Apache Airflow DAGs", {"airflow"}, set()),
        ("treat rust on equipment", set(), {"rust"}),
        ("respond with a swift reaction; react quickly", set(), {"swift", "react"}),
        ("React and Swift apps", {"react", "swift"}, set()),
        ("DBT therapy for patients", set(), {"dbt"}),
        ("models built with dbt", {"dbt"}, set()),
        ("cutting torch and welding", set(), {"pytorch"}),
        ("an 8-hour workday", set(), {"workday"}),
        ("Grade C certification, C-level executives", set(), {"c"}),
    ],
)
def test_extraction(text, present, absent):
    found = set(techs(text))
    assert present <= found, f"manquants : {present - found}"
    assert not (absent & found), f"faux positifs : {absent & found}"


def test_output_is_sorted_and_unique():
    assert techs("SQL sql Sql python") == ["python", "sql"]


def test_empty_text():
    assert techs(None) == [] and techs("") == []
