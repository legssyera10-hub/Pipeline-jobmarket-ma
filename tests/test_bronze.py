"""Écriture Bronze, avec un faux client S3 qui respecte If-None-Match."""

import io
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from src.ingestion.bronze import (
    EmptyBatchError,
    add_technical_columns,
    bronze_key,
    write_bronze,
)

RUN = date(2024, 4, 9)
NOW = pd.Timestamp("2026-10-04 12:00:00", tz="UTC")


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None):
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[(Bucket, Key)] = Body


@pytest.fixture
def raw():
    return pd.DataFrame({"job_id": ["1", "2"], "zip_code": ["08540", None]})


def test_key_layout():
    assert (
        bronze_key("kaggle_linkedin", RUN)
        == "bronze/source=kaggle_linkedin/date=2024-04-09/data.parquet"
    )


def test_technical_columns_added_without_touching_raw(raw):
    out = add_technical_columns(raw, "kaggle_linkedin", RUN, NOW)
    assert list(out.columns) == [
        "job_id",
        "zip_code",
        *["_ingested_at", "_source", "_run_date"],
    ]
    assert out["_source"].unique().tolist() == ["kaggle_linkedin"]
    assert "_source" not in raw.columns  # l'entrée n'est pas modifiée


def test_reserved_column_in_source_is_rejected(raw):
    with pytest.raises(ValueError, match="réservées"):
        add_technical_columns(raw.assign(_source="x"), "s", RUN, NOW)


def test_written_parquet_content_and_types(raw, settings):
    s3 = FakeS3()
    res = write_bronze(raw, "kaggle_linkedin", RUN, settings, s3, NOW)

    assert res.status == "written" and res.rows == 2
    body = s3.objects[("jobmarket", res.key)]
    table = pq.read_table(io.BytesIO(body))
    schema = {f.name: str(f.type) for f in table.schema}
    assert schema == {
        "job_id": "string",
        "zip_code": "string",
        "_ingested_at": "timestamp[us, tz=UTC]",  # lisible par Spark (pas de ns)
        "_source": "string",
        "_run_date": "date32[day]",
    }
    assert table.column("zip_code").to_pylist() == ["08540", None]


def test_all_null_column_is_still_written_as_string(settings):
    # Cas réel : petite partition où zip_code est vide partout -> type "null" inféré
    df = pd.DataFrame({"job_id": ["1"], "zip_code": [None]})
    s3 = FakeS3()
    res = write_bronze(df, "kaggle_linkedin", RUN, settings, s3, NOW)
    table = pq.read_table(io.BytesIO(s3.objects[("jobmarket", res.key)]))
    assert str(table.schema.field("zip_code").type) == "string"


def test_second_write_is_skipped_and_original_kept(raw, settings):
    s3 = FakeS3()
    first = write_bronze(raw, "kaggle_linkedin", RUN, settings, s3, NOW)
    original = s3.objects[("jobmarket", first.key)]

    again = write_bronze(raw.head(1), "kaggle_linkedin", RUN, settings, s3, NOW)

    assert again.status == "skipped"
    assert s3.objects[("jobmarket", first.key)] == original


def test_empty_batch_is_an_error(settings):
    with pytest.raises(EmptyBatchError):
        write_bronze(pd.DataFrame(), "kaggle_linkedin", RUN, settings, FakeS3(), NOW)


def test_other_s3_errors_propagate(raw, settings):
    class Broken(FakeS3):
        def put_object(self, **kwargs):
            raise ClientError(
                {
                    "Error": {"Code": "AccessDenied"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "PutObject",
            )

    with pytest.raises(ClientError):
        write_bronze(raw, "kaggle_linkedin", RUN, settings, Broken(), NOW)
