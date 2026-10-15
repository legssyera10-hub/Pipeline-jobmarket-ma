"""Vérifie si des employeurs publient des offres au Maroc sur Greenhouse / Lever.

  docker compose run --rm --no-deps app \
      python -m scripts.sonder_employeurs artefact Yassir

Pour chaque identifiant, interroge les deux API (client poli : robots.txt, délais) et
affiche le nombre total d'offres et le nombre d'offres au Maroc. Un employeur n'est
ajouté à reference/employeurs_ats.yaml qu'après cette vérification.
"""

from __future__ import annotations

import sys

import requests

from src.ingestion.ats.collecte import user_agent
from src.ingestion.ats.http import PoliteClient, RobotsDenied, TransientHTTPError
from src.ingestion.ats.parsers import API_URLS, PARSERS


def main(slugs: list[str]) -> int:
    if not slugs:
        print(__doc__)
        return 2
    http = PoliteClient(user_agent())
    for slug in slugs:
        for ats, url in API_URLS.items():
            try:
                body = http.get_json(url.format(slug=slug))
            except requests.HTTPError:
                continue  # identifiant inconnu sur cette plateforme
            except (RobotsDenied, TransientHTTPError) as exc:
                print(f"{ats:<11} {slug:<20} indisponible : {exc}")
                continue
            total = len(body.get("jobs", [])) if isinstance(body, dict) else len(body)
            maroc = PARSERS[ats](body, slug, slug)
            titres = [r["title"] for r in maroc[:3]]
            print(f"{ats:<11} {slug:<20} {total:>4} offres, {len(maroc):>3} au Maroc")
            print(f"{'':<33}ex. : {titres}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
