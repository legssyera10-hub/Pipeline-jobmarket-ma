import pytest

from src.utils.config import Settings

ENV = {
    "MINIO_ROOT_USER": "user",
    "MINIO_ROOT_PASSWORD": "secret",
    "POSTGRES_USER": "pg",
    "POSTGRES_PASSWORD": "pgpass",
}


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    # Se placer dans un dossier vide pour que load_dotenv ne lise pas le vrai .env
    monkeypatch.chdir(tmp_path)
    for key in [*ENV, "MINIO_ENDPOINT", "POSTGRES_HOST", "POSTGRES_PORT"]:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_from_env_reads_variables(clean_env):
    for k, v in ENV.items():
        clean_env.setenv(k, v)
    clean_env.setenv("POSTGRES_PORT", "6543")

    s = Settings.from_env()

    assert s.minio_access_key == "user"
    assert s.postgres_port == 6543
    assert s.minio_endpoint == "http://localhost:9000"  # valeur par défaut


def test_from_env_fails_loudly_on_missing_secret(clean_env):
    for k, v in ENV.items():
        if k != "MINIO_ROOT_PASSWORD":
            clean_env.setenv(k, v)

    with pytest.raises(RuntimeError, match="MINIO_ROOT_PASSWORD"):
        Settings.from_env()


def test_s3_path_schemes(settings):
    assert settings.s3_path("bronze", "source=x/") == "s3://jobmarket/bronze/source=x"
    assert (
        settings.s3_path("/silver/", "date=2026-01-01", scheme="s3a")
        == "s3a://jobmarket/silver/date=2026-01-01"
    )


def test_storage_options_point_to_minio(settings):
    opts = settings.storage_options
    assert opts["client_kwargs"]["endpoint_url"] == "http://minio.test:9000"
    assert opts["key"] == "test-key"


def test_jdbc_url(settings):
    assert settings.jdbc_url == "jdbc:postgresql://pg.test:5432/jobmarket"
    assert settings.jdbc_properties["driver"] == "org.postgresql.Driver"
