"""Purge d'une source Bronze. Opération d'administration, jamais lancée par le DAG.

Bronze est immuable : une écriture fautive ne peut pas être « corrigée » par une
nouvelle ingestion. La seule voie est de supprimer explicitement les fichiers puis
de réingérer. On exige de retaper le nom de la source pour éviter tout accident.

  docker compose run --rm app python -m scripts.purge_bronze kaggle_linkedin \
      --confirm kaggle_linkedin [--date 2024-04-09]
"""

from __future__ import annotations

import argparse
import sys

from src.utils.config import get_settings
from src.utils.storage import list_keys, s3_client


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("source")
    p.add_argument("--confirm", required=True, help="retaper le nom de la source")
    p.add_argument("--date", help="limiter la purge à une partition (YYYY-MM-DD)")
    args = p.parse_args()
    if args.confirm != args.source:
        p.error("--confirm doit être identique au nom de la source")

    s = get_settings()
    prefix = f"bronze/source={args.source}/"
    if args.date:
        prefix += f"date={args.date}/"
    keys = list_keys(prefix, s)
    if not keys:
        print(f"rien à purger sous {prefix}")
        return 0
    client = s3_client(s)
    for i in range(0, len(keys), 1000):  # DeleteObjects : 1000 clés max par appel
        client.delete_objects(
            Bucket=s.minio_bucket,
            Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
        )
    print(f"{len(keys)} fichier(s) supprimé(s) sous s3://{s.minio_bucket}/{prefix}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
