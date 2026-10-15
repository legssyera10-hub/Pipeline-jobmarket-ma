"""Accès bas niveau à MinIO via boto3 (existence du bucket, listage des clés)."""

from __future__ import annotations

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from src.utils.config import Settings, get_settings


def s3_client(settings: Settings | None = None) -> BaseClient:
    s = settings or get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_access_key,
        aws_secret_access_key=s.minio_secret_key,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        region_name="us-east-1",
    )


def list_keys(prefix: str, settings: Settings | None = None) -> list[str]:
    s = settings or get_settings()
    paginator = s3_client(s).get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=s.minio_bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def delete_prefix(prefix: str, settings: Settings | None = None) -> int:
    """Supprime tous les objets sous `prefix`. Retourne le nombre supprimé."""
    if not prefix or not prefix.endswith("/"):
        raise ValueError(f"préfixe refusé (doit finir par '/') : {prefix!r}")
    s = settings or get_settings()
    keys = list_keys(prefix, s)
    client = s3_client(s)
    for i in range(0, len(keys), 1000):  # DeleteObjects : 1000 clés max par appel
        client.delete_objects(
            Bucket=s.minio_bucket,
            Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
        )
    return len(keys)


def copy_prefix(src: str, dst: str, settings: Settings | None = None) -> int:
    """Copie côté serveur (sans transfert par le client) de src/* vers dst/*."""
    if not (src.endswith("/") and dst.endswith("/")):
        raise ValueError("les préfixes doivent finir par '/'")
    s = settings or get_settings()
    client = s3_client(s)
    keys = list_keys(src, s)
    for key in keys:
        client.copy_object(
            Bucket=s.minio_bucket,
            Key=dst + key[len(src) :],
            CopySource={"Bucket": s.minio_bucket, "Key": key},
        )
    return len(keys)
