"""Collecte : appelle les API des employeurs et stocke les réponses BRUTES, immuables.

  python -m src.ingestion.ats.collecte              # instantané du jour (UTC)

raw/source=ats_maroc/date=<YYYY-MM-DD>/<ats>__<slug>.json.gz : la réponse JSON
complète, enveloppée de métadonnées (URL, heure, User-Agent).

Ces API donnent l'état du MOMENT : on ne peut pas recollecter le 12 octobre le
15 octobre. La collecte n'accepte donc que la date du jour. Ce qui se rejoue, c'est
le parsing (raw -> Bronze), à partir des réponses stockées.
Un employeur déjà collecté ce jour est ignoré : relancer ne refait aucun appel.
Un employeur en échec n'empêche pas les autres ; code de sortie 1 s'il y en a un.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import requests
import yaml
from botocore.exceptions import ClientError

from src.ingestion.ats.http import PoliteClient, RobotsDenied, TransientHTTPError
from src.ingestion.ats.parsers import API_URLS, PARSERS
from src.utils.config import Settings, get_settings
from src.utils.storage import s3_client

SOURCE = "ats_maroc"
EMPLOYEURS = Path(__file__).resolve().parents[3] / "reference" / "employeurs_ats.yaml"


@dataclass(frozen=True)
class Employeur:
    nom: str
    ats: str
    slug: str


def load_employeurs(path: Path = EMPLOYEURS) -> list[Employeur]:
    with open(path, encoding="utf-8") as f:
        rows = yaml.safe_load(f)["employeurs"]
    out = [Employeur(r["nom"], r["ats"], r["slug"]) for r in rows]
    unknown = {e.ats for e in out} - set(PARSERS)
    if unknown:
        raise ValueError(f"ATS sans parseur : {unknown}")
    return out


def user_agent() -> str:
    contact = os.getenv("COLLECTE_CONTACT")
    base = "JobMarketMA/0.1 (projet etudiant data engineering; ~10 requetes/jour"
    return f"{base}; contact: {contact})" if contact else f"{base})"


def raw_key(run_date: date, emp: Employeur) -> str:
    return (
        f"raw/source={SOURCE}/date={run_date.isoformat()}/{emp.ats}__{emp.slug}.json.gz"
    )


def _exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        if err.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        raise


def collect(
    run_date: date,
    http: PoliteClient,
    settings: Settings,
    employeurs: list[Employeur],
    s3=None,
    today: date | None = None,
) -> list[dict]:
    today = today or datetime.now(UTC).date()
    if run_date != today:
        raise ValueError(
            f"collecte impossible pour {run_date} : une API donne l'état du jour "
            f"({today}). Pour une date passée, rejouer le parsing (raw -> Bronze)."
        )
    s3 = s3 or s3_client(settings)
    results = []
    for emp in employeurs:
        key = raw_key(run_date, emp)
        res = {"employeur": emp.nom, "ats": emp.ats, "cle": key}
        if _exists(s3, settings.minio_bucket, key):
            results.append({**res, "statut": "skipped"})
            continue
        url = API_URLS[emp.ats].format(slug=emp.slug)
        try:
            body = http.get_json(url)
        except (RobotsDenied, TransientHTTPError, requests.RequestException) as exc:
            results.append(
                {**res, "statut": "failed", "erreur": f"{type(exc).__name__}: {exc}"}
            )
            continue
        envelope = {
            "source": SOURCE,
            "ats": emp.ats,
            "employeur": emp.nom,
            "slug": emp.slug,
            "url": url,
            "recupere_le": datetime.now(UTC).isoformat(),
            "user_agent": http.user_agent,
            "corps": body,
        }
        payload = gzip.compress(
            json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        )
        try:
            s3.put_object(
                Bucket=settings.minio_bucket, Key=key, Body=payload, IfNoneMatch="*"
            )
        except ClientError as err:
            if err.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (
                409,
                412,
            ):
                results.append({**res, "statut": "skipped"})
                continue
            raise
        n_total = len(body.get("jobs", [])) if isinstance(body, dict) else len(body)
        n_maroc = len(PARSERS[emp.ats](body, emp.nom, emp.slug))
        results.append(
            {**res, "statut": "written", "offres": n_total, "offres_maroc": n_maroc}
        )
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Collecte brute des API d'ATS")
    p.add_argument("--date", type=date.fromisoformat, help="défaut : aujourd'hui (UTC)")
    args = p.parse_args(argv)
    run_date = args.date or datetime.now(UTC).date()
    settings = get_settings()
    http = PoliteClient(user_agent())
    if not os.getenv("COLLECTE_CONTACT"):
        print("[collecte] AVERTISSEMENT : COLLECTE_CONTACT non défini dans .env, "
              "le User-Agent n'a pas de moyen de contact")  # fmt: skip
    try:
        results = collect(run_date, http, settings, load_employeurs())
    except ValueError as exc:  # date passée : erreur d'usage, pas un incident
        print(f"[collecte] {exc}", file=sys.stderr)
        return 2
    for r in results:
        detail = (
            f"{r.get('offres_maroc')} offres au Maroc / {r.get('offres')}"
            if r["statut"] == "written"
            else r.get("erreur", "")
        )
        print(
            f"[collecte] {r['statut']:<8} {r['ats']:<11} {r['employeur']:<14} {detail}"
        )
    failed = [r for r in results if r["statut"] == "failed"]
    print(f"[collecte] {run_date} : {len(results) - len(failed)}/{len(results)} OK")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
