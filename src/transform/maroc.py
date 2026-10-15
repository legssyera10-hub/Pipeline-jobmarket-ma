"""Reconnaissance des lieux marocains (sources ATS, plus tard les sites marocains).

Les lieux arrivent sous des formes très variées : "Casablanca, Morocco",
"Capital Tower, Boulevard Mly Youssef, Casablanca, Maroc", "Remote - Morocco",
"Tangier", "الرباط". On cherche une ville du référentiel (variantes FR / EN / AR,
sans accents) délimitée comme un mot.
"""

from __future__ import annotations

import csv
import re
from functools import lru_cache
from pathlib import Path

from src.transform.text import strip_accents

REFERENCE = Path(__file__).resolve().parents[2] / "reference" / "ma_villes.csv"
_PAYS = re.compile(r"\b(?:maroc|morocco|marruecos)\b|المغرب")


def _norm(text: str) -> str:
    return strip_accents(text).lower()


@lru_cache(maxsize=1)
def _cities() -> list[tuple[re.Pattern, str, str]]:
    """(motif, ville canonique, région), variantes les plus longues d'abord."""
    entries = []
    with open(REFERENCE, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for variant in row["variantes"].split("|"):
                entries.append((_norm(variant), row["ville"], row["region"]))
    entries.sort(key=lambda e: len(e[0]), reverse=True)
    # (?<!\w) / (?!\w) plutôt que \b : fonctionne aussi pour l'arabe
    return [
        (re.compile(rf"(?<!\w){re.escape(v)}(?!\w)"), ville, region)
        for v, ville, region in entries
    ]


def find_city(text: str | None) -> tuple[str, str] | None:
    """Première ville marocaine citée (la plus à gauche) -> (ville, région)."""
    if not text:
        return None
    t = _norm(text)
    best = None
    for pattern, ville, region in _cities():
        m = pattern.search(t)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), ville, region)
    return (best[1], best[2]) if best else None


def is_morocco(text: str | None) -> bool:
    """Le lieu mentionne le Maroc ou une ville marocaine."""
    if not text:
        return False
    return bool(_PAYS.search(_norm(text))) or find_city(text) is not None


def region_of(ville: str | None) -> str | None:
    if not ville:
        return None
    for _, v, region in _cities():
        if v == ville:
            return region
    return None
