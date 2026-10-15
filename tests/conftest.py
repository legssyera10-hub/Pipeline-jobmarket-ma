import pytest

from src.utils.config import Settings


@pytest.fixture
def settings() -> Settings:
    """Settings factices : aucun test unitaire ne dépend du .env ni des services."""
    return Settings(
        minio_endpoint="http://minio.test:9000",
        minio_access_key="test-key",
        minio_secret_key="test-secret",
        minio_bucket="jobmarket",
        postgres_host="pg.test",
        postgres_port=5432,
        postgres_db="jobmarket",
        postgres_user="u",
        postgres_password="p",
        spark_master="local[2]",
    )


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale sans connecteur S3, partagée par tous les tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
