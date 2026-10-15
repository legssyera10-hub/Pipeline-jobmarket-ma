"""Source « Kaggle LinkedIn Job Postings 2023-2024 » (data/raw/postings.csv).

Le CSV n'est pas découpé par jour : chaque `fetch(run_date)` devrait relire 493 Mo.
On le convertit donc UNE fois, en streaming, en un cache local Parquet partitionné
par date de publication (data/staging/...). Ce cache simule « ce que la source publie
chaque jour » ; ce n'est pas la couche Bronze, qui reste dans MinIO.

Toutes les colonnes sont lues en texte : laisser deviner les types détruit des
valeurs (zip_code "08540" -> 8540.0). Le typage est le travail de la couche Silver.
"""

from __future__ import annotations

import csv
import shutil
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import csv as pacsv

from src.ingestion.base import Collector

SOURCE = "kaggle_linkedin"
# Première mise en ligne de l'offre (epoch ms UTC, ex. "1713397508000.0").
# listed_time est la date de la dernière republication : on la garde en colonne brute.
PUBLICATION_COL = "original_listed_time"
# Pas de "_" en tête : pyarrow ignore les dossiers "_xxx=..." à la lecture
PARTITION_COL = "pub_date"
NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def epoch_ms_to_date(values: pa.Array) -> pa.Array:
    """'1713397508000.0' -> date UTC 2024-04-17. Les valeurs nulles restent nulles."""
    millis = pc.cast(pc.cast(values, pa.float64()), pa.int64())
    return pc.cast(pc.cast(millis, pa.timestamp("ms", tz="UTC")), pa.date32())


def _read_header(csv_path: Path) -> list[str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def _csv_batches(csv_path: Path, columns: list[str]) -> pacsv.CSVStreamingReader:
    return pacsv.open_csv(
        csv_path,
        read_options=pacsv.ReadOptions(block_size=32 << 20),
        # Les descriptions contiennent des retours à la ligne entre guillemets
        parse_options=pacsv.ParseOptions(newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in columns},
            strings_can_be_null=True,
        ),
    )


def build_staging(csv_path: Path, staging_dir: Path) -> int:
    """CSV -> Parquet partitionné par date de publication. Retourne le nb de lignes."""
    columns = _read_header(csv_path)
    if PUBLICATION_COL not in columns:
        raise ValueError(f"colonne {PUBLICATION_COL} absente de {csv_path}")

    schema = pa.schema(
        [pa.field(c, pa.string()) for c in columns]
        + [pa.field(PARTITION_COL, pa.date32())]
    )
    n_rows = 0

    def batches() -> Iterator[pa.RecordBatch]:
        nonlocal n_rows
        for batch in _csv_batches(csv_path, columns):
            n_rows += batch.num_rows
            pub = epoch_ms_to_date(batch.column(PUBLICATION_COL))
            yield pa.RecordBatch.from_arrays([*batch.columns, pub], schema=schema)

    tmp_dir = staging_dir.with_name(staging_dir.name + ".tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    ds.write_dataset(
        batches(),
        tmp_dir,
        schema=schema,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([(PARTITION_COL, pa.date32())]), flavor="hive"
        ),
        existing_data_behavior="error",
    )
    # Remplacement en fin de traitement : un cache à moitié écrit n'est jamais visible
    shutil.rmtree(staging_dir, ignore_errors=True)
    tmp_dir.rename(staging_dir)
    return n_rows


class KaggleLinkedInCollector(Collector):
    name = SOURCE

    def __init__(
        self,
        csv_path: str | Path = "data/raw/postings.csv",
        staging_dir: str | Path = "data/staging/kaggle_linkedin/postings",
    ):
        self.csv_path = Path(csv_path)
        self.staging_dir = Path(staging_dir)

    def ensure_staging(self) -> None:
        """Construit le cache s'il est absent ou plus ancien que le CSV."""
        if (
            self.staging_dir.exists()
            and self.staging_dir.stat().st_mtime >= self.csv_path.stat().st_mtime
        ):
            return
        n = build_staging(self.csv_path, self.staging_dir)
        print(f"[staging] {n:,} lignes de {self.csv_path} -> {self.staging_dir}")

    def _dataset(self) -> ds.Dataset:
        self.ensure_staging()
        return ds.dataset(
            self.staging_dir,
            format="parquet",
            partitioning=ds.partitioning(
                pa.schema([(PARTITION_COL, pa.date32())]), flavor="hive"
            ),
        )

    def fetch(self, run_date: date) -> pd.DataFrame:
        dataset = self._dataset()
        columns = [c for c in dataset.schema.names if c != PARTITION_COL]
        table = dataset.to_table(
            columns=columns, filter=ds.field(PARTITION_COL) == pa.scalar(run_date)
        )
        # Ordre stable : relancer la même date produit le même fichier
        return table.sort_by("job_id").to_pandas()

    def available_dates(self) -> list[date]:
        counts = self.rows_per_date()
        return sorted(d for d in counts if d is not None)

    def rows_per_date(self) -> dict[date | None, int]:
        table = self._dataset().to_table(columns=[PARTITION_COL])
        result = table.group_by(PARTITION_COL).aggregate([([], "count_all")])
        return dict(
            zip(
                result.column(PARTITION_COL).to_pylist(),
                result.column("count_all").to_pylist(),
                strict=True,
            )
        )
