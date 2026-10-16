"""Prépare une panne provoquée sur la source isolée `ats_maroc_demo` (jour 7).

  docker compose run --rm --no-deps app python -m scripts.preparer_incident schema
  docker compose run --rm --no-deps app python -m scripts.preparer_incident qualite
  docker compose run --rm --no-deps app python -m scripts.preparer_incident nettoyer

Copie les VRAIES réponses brutes d'un jour de `ats_maroc` vers `ats_maroc_demo`, en
les altérant, puis on déclenche le DAG avec {"source": "ats_maroc_demo",
"collecter": false}. Les données réelles ne sont jamais touchées.

- schema  : l'API Lever « renomme » le champ `text` (titre) en `title`
            -> la tâche bronze doit échouer avec SchemaDriftError, et alerter ;
- qualite : tous les lieux deviennent « Morocco » (plus aucune ville)
            -> la tâche silver doit échouer sur la suite critique, rien publié ;
- copie   : copie saine, pour tester le DAG de bout en bout ;
- nettoyer: supprime tout ce qui concerne `ats_maroc_demo` (MinIO et Gold).
"""

from __future__ import annotations

import gzip
import json
import re
import sys

from src.ingestion.ats.collecte import SOURCE
from src.ingestion.ats_maroc import DEMO_SOURCE
from src.utils.config import get_settings
from src.utils.storage import delete_prefix, list_keys, s3_client


def _corrupt_schema(ats: str, body):
    if ats == "lever":
        for post in body:
            post["title"] = post.pop("text", None)
    return body


def _corrupt_locations(ats: str, body):
    if ats == "greenhouse":
        for job in body["jobs"]:
            job["location"] = {"name": "Morocco"}
            for office in job.get("offices") or []:
                office["location"], office["name"] = "Morocco", "Morocco"
    else:
        for post in body:
            cat = post.setdefault("categories", {})
            cat["location"], cat["allLocations"] = "Morocco", ["Morocco"]
            post["country"] = "MA"
    return body


MUTATIONS = {
    "copie": lambda ats, body: body,  # copie saine : tester le DAG de bout en bout
    "schema": _corrupt_schema,
    "qualite": _corrupt_locations,
}
PREFIXES = [
    f"raw/source={DEMO_SOURCE}/",
    f"bronze/source={DEMO_SOURCE}/",
    f"silver/offres/source={DEMO_SOURCE}/",
    f"silver/rejets/source={DEMO_SOURCE}/",
    f"silver/observations/source={DEMO_SOURCE}/",
    f"silver/_staging/{DEMO_SOURCE}/",
]


def clean(settings) -> int:
    from src.transform.gold import TABLES
    from src.utils.db import connect

    with connect(settings) as conn, conn.cursor() as cur:  # lignes Gold de la démo
        for table in TABLES:
            cur.execute(f"DELETE FROM gold.{table} WHERE source = %s", (DEMO_SOURCE,))
    return sum(delete_prefix(p, settings) for p in PREFIXES)


def prepare(mode: str, settings) -> str:
    keys = list_keys(f"raw/source={SOURCE}/", settings)
    dates = sorted({re.search(r"date=([\d-]+)/", k).group(1) for k in keys})
    if not dates:
        sys.exit("aucune collecte réelle à copier : lancer d'abord la collecte")
    day = dates[-1]
    clean(settings)  # Bronze est immuable : repartir d'une source démo vide
    client = s3_client(settings)
    for key in [k for k in keys if f"date={day}/" in k]:
        env = json.loads(
            gzip.decompress(
                client.get_object(Bucket=settings.minio_bucket, Key=key)["Body"].read()
            )
        )
        env["corps"] = MUTATIONS[mode](env["ats"], env["corps"])
        env["incident_simule"] = mode
        client.put_object(
            Bucket=settings.minio_bucket,
            Key=key.replace(f"source={SOURCE}/", f"source={DEMO_SOURCE}/"),
            Body=gzip.compress(json.dumps(env, ensure_ascii=False).encode("utf-8")),
        )
    return day


def main(argv: list[str]) -> int:
    mode = argv[0] if argv else ""
    settings = get_settings()
    if mode == "nettoyer":
        print(f"[incident] {clean(settings)} objet(s) supprimé(s) pour {DEMO_SOURCE}")
        return 0
    if mode not in MUTATIONS:
        print(__doc__)
        return 2
    day = prepare(mode, settings)
    print(
        f"[incident] '{mode}' préparé : raw/source={DEMO_SOURCE}/date={day}/\n"
        f'[incident] déclencher le DAG avec {{"source": "{DEMO_SOURCE}", '
        f'"collecter": false, "date": "{day}"}}'
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
