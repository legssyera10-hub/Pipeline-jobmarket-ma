"""Fabrique de SparkSession configurée pour MinIO (s3a://) et PostgreSQL (JDBC)."""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession

from src.utils.config import Settings, get_settings

# Utilisé seulement hors Docker : Spark télécharge les jars depuis Maven au démarrage.
# Versions alignées sur le Hadoop 3.3.4 embarqué dans PySpark 3.5.
_MAVEN_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.4",
    ]
)


def get_spark(
    app_name: str = "jobmarket", settings: Settings | None = None
) -> SparkSession:
    s = settings or get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(s.spark_master)
        # --- MinIO via le connecteur S3A de Hadoop ---
        .config("spark.hadoop.fs.s3a.endpoint", s.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s.minio_secret_key)
        # MinIO sert les buckets en "path style" (http://host/bucket/key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(s.minio_endpoint.startswith("https")).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # --- Reproductibilité / idempotence ---
        .config("spark.sql.session.timeZone", "UTC")
        # overwrite ne remplace QUE les partitions présentes dans le DataFrame écrit
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Petit volume en local : les 200 partitions de shuffle par défaut sont de trop
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        # En mode local, le driver est aussi l'exécuteur. Budget pour 4 Go de Docker :
        # heap JVM + 1 worker Python par coeur + MinIO + PostgreSQL.
        .config("spark.driver.memory", s.spark_driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Lots Arrow de 2 000 lignes (10 000 par défaut) : une description fait ~3,6 Ko,
        # un lot de 10 000 occupe ~150 Mo par worker Python une fois en pandas.
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2000")
        # Cache en colonnes : lots de 2 000 lignes (10 000 par défaut). Chaque lot est
        # construit en mémoire avant d'aller sur disque ; avec les descriptions, un lot
        # de 10 000 a provoqué un OutOfMemoryError (jour 5).
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "2000")
    )

    jars_dir = os.getenv("SPARK_JARS_DIR")
    jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar"))) if jars_dir else []
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        builder = builder.config("spark.jars.packages", _MAVEN_PACKAGES)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
