"""Collecteur Bronze de la source `ats_maroc` : parse les réponses brutes stockées.

La collecte réseau (src/ingestion/ats/collecte.py) et le parsing sont deux étapes :
  1. collecte : API -> raw/source=ats_maroc/date=D/*.json.gz (immuable) ;
  2. ce collecteur : raw du jour D -> lignes Bronze (via src.ingestion.run).
Rejouer l'étape 2 ne refait aucun appel réseau.
"""

from __future__ import annotations

import gzip
import json
import re
from datetime import date

import pandas as pd

from src.ingestion.ats.collecte import SOURCE
from src.ingestion.ats.parsers import PARSERS
from src.ingestion.base import Collector
from src.utils.config import Settings, get_settings
from src.utils.storage import list_keys, s3_client

DEMO_SOURCE = f"{SOURCE}_demo"  # source isolée pour provoquer des incidents (jour 7)


class AtsMarocCollector(Collector):
    name = SOURCE

    def __init__(self, settings: Settings | None = None, source: str = SOURCE):
        self.settings = settings or get_settings()
        self.name = source

    def _keys(self, run_date: date | None = None) -> list[str]:
        prefix = f"raw/source={self.name}/"
        if run_date:
            prefix += f"date={run_date.isoformat()}/"
        return sorted(list_keys(prefix, self.settings))

    def fetch(self, run_date: date) -> pd.DataFrame:
        client = s3_client(self.settings)
        records = []
        for key in self._keys(run_date):
            obj = client.get_object(Bucket=self.settings.minio_bucket, Key=key)
            envelope = json.loads(gzip.decompress(obj["Body"].read()))
            parse = PARSERS[envelope["ats"]]
            records.extend(
                parse(envelope["corps"], envelope["employeur"], envelope["slug"])
            )
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records).astype(object)
        return df.where(df.notna(), None).sort_values("job_id").reset_index(drop=True)

    def available_dates(self) -> list[date]:
        found = {re.search(r"date=(\d{4}-\d{2}-\d{2})/", k) for k in self._keys()}
        return sorted(date.fromisoformat(m.group(1)) for m in found if m)
