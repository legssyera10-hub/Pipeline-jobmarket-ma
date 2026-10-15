"""Point d'entrée de l'ingestion Bronze.

  python -m src.ingestion.run --date 2024-04-09        # un jour (usage Airflow)
  python -m src.ingestion.run --start 2024-04-01 --end 2024-04-20
  python -m src.ingestion.run --all                    # backfill complet

Une date déjà présente en Bronze est ignorée (« skipped ») : Bronze est immuable.
Code de sortie 1 si une date échoue, ou si la date unique demandée est vide.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

import pandas as pd

from src.ingestion.ats_maroc import AtsMarocCollector
from src.ingestion.base import Collector
from src.ingestion.bronze import EmptyBatchError, write_bronze
from src.ingestion.kaggle_linkedin import KaggleLinkedInCollector
from src.utils.config import get_settings
from src.utils.storage import s3_client

COLLECTORS: dict[str, type[Collector]] = {
    KaggleLinkedInCollector.name: KaggleLinkedInCollector,
    AtsMarocCollector.name: AtsMarocCollector,
}


def _date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingestion Bronze")
    p.add_argument("--source", default="kaggle_linkedin", choices=sorted(COLLECTORS))
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat)
    group.add_argument("--start", type=date.fromisoformat)
    group.add_argument("--all", action="store_true")
    p.add_argument("--end", type=date.fromisoformat)
    args = p.parse_args(argv)
    if args.start and not args.end:
        p.error("--start exige --end")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    collector = COLLECTORS[args.source]()
    settings = get_settings()
    client = s3_client(settings)
    # Un seul horodatage par exécution : toutes les partitions d'un run le partagent
    ingested_at = pd.Timestamp.now(tz="UTC")

    if args.date:
        dates = [args.date]
    elif args.all:
        dates = collector.available_dates()
    else:
        dates = _date_range(args.start, args.end)

    counts = {"written": 0, "skipped": 0, "empty": 0, "failed": 0}
    rows_written = 0
    t0 = time.perf_counter()
    for d in dates:
        try:
            res = write_bronze(
                collector.fetch(d), collector.name, d, settings, client, ingested_at
            )
        except EmptyBatchError as exc:
            counts["empty"] += 1
            print(f"[bronze] {d} vide : {exc}")
            continue
        except Exception as exc:
            counts["failed"] += 1
            print(f"[bronze] {d} ÉCHEC : {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        counts[res.status] += 1
        if res.status == "written":
            rows_written += res.rows
        print(f"[bronze] {d} {res.status:<7} {res.rows:>6} lignes -> {res.key}")

    print(
        f"\n[bronze] {len(dates)} date(s) en {time.perf_counter() - t0:.0f}s : "
        + ", ".join(f"{k}={v}" for k, v in counts.items())
        + f" ; {rows_written:,} lignes écrites"
    )
    single_date_empty = bool(args.date) and counts["empty"] == 1
    return 1 if counts["failed"] or single_date_empty else 0


if __name__ == "__main__":
    sys.exit(main())
