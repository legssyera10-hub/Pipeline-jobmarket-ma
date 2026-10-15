"""Collecteur Kaggle : CSV synthétique, mêmes pièges que le vrai fichier."""

import shutil
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from src.ingestion.kaggle_linkedin import KaggleLinkedInCollector, epoch_ms_to_date

FIXTURE = Path(__file__).parent / "fixtures" / "postings_sample.csv"
D1, D2 = date(2024, 4, 9), date(2024, 4, 10)


@pytest.fixture
def collector(tmp_path):
    csv_path = tmp_path / "postings.csv"
    shutil.copy(FIXTURE, csv_path)
    return KaggleLinkedInCollector(csv_path, tmp_path / "staging" / "postings")


def test_epoch_ms_string_to_utc_date():
    arr = pa.array(["1712620800000.0", "1712707199000.0", None])
    assert epoch_ms_to_date(arr).to_pylist() == [D1, D1, None]


def test_fetch_returns_only_rows_of_the_date_sorted(collector):
    df = collector.fetch(D1)
    assert df["job_id"].tolist() == ["1", "3"]
    assert "pub_date" not in df.columns


def test_all_columns_kept_as_raw_text(collector):
    df = collector.fetch(D1).set_index("job_id")
    assert df.loc["3", "zip_code"] == "08540"  # zéro initial préservé
    assert df.loc["3", "max_salary"] == "120000.0"
    assert df.loc["3", "description"] == "Ligne 1\nLigne 2, avec virgule"
    assert df.loc["1", "description"] == 'Description "citée"'
    assert set(df.dtypes.astype(str)) == {"object"}


def test_empty_fields_become_null(collector):
    df = collector.fetch(D2).set_index("job_id")
    assert df.loc["2", "zip_code"] is None


def test_rows_per_date_counts_missing_dates_separately(collector):
    assert collector.rows_per_date() == {D1: 2, D2: 1, None: 1}
    assert collector.available_dates() == [D1, D2]


def test_fetch_unknown_date_is_empty(collector):
    assert collector.fetch(date(2020, 1, 1)).empty


def test_staging_is_rebuilt_when_csv_changes(collector):
    collector.ensure_staging()
    with open(collector.csv_path, "a", encoding="utf-8") as f:
        f.write("5,Hooli,Data Scientist,x,,1712620800000.0,1712620800000.0,\n")
    future = collector.staging_dir.stat().st_mtime + 10
    import os

    os.utime(collector.csv_path, (future, future))
    assert collector.fetch(D1)["job_id"].tolist() == ["1", "3", "5"]
