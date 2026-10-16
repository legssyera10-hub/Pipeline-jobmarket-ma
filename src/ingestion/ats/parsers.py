"""Réponse JSON brute d'une API d'ATS -> lignes Bronze (texte), offres au Maroc.

Séparé de la collecte : si une API change de format, on corrige le parseur et on
rejoue le parsing sur les réponses brutes déjà stockées (raw/), sans recollecter.

Contrat Bronze -> Silver : les champs utilisés par Silver gardent les noms déjà en
place (title, description, location, original_listed_time, work_type...). Les champs
propres aux ATS s'y ajoutent (ats, employeur, pays_code, workplace_type...).
"""

from __future__ import annotations

import html
import re

from bs4 import BeautifulSoup

from src.transform.maroc import is_morocco

GREENHOUSE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
LEVER_URL = "https://api.lever.co/v0/postings/{slug}?mode=json"

# Champs sans lesquels une offre est inexploitable. S'ils disparaissent, c'est que
# l'API a changé de format : on échoue avec un message explicite plutôt que de
# produire des lignes vides que seule la quarantaine finirait par remarquer.
REQUIRED_FIELDS = {
    "greenhouse": ("id", "title", "location", "absolute_url"),
    "lever": ("id", "text", "categories", "hostedUrl"),
}


class SchemaDriftError(ValueError):
    """La réponse de l'API n'a plus le format attendu par le parseur."""


def check_schema(ats: str, items) -> None:
    if not isinstance(items, list):
        raise SchemaDriftError(
            f"API {ats} : liste d'offres attendue, reçu {type(items).__name__}"
        )
    missing = {
        f: sum(1 for it in items if not isinstance(it, dict) or f not in it)
        for f in REQUIRED_FIELDS[ats]
    }
    missing = {f: n for f, n in missing.items() if n}
    if missing:
        detail = ", ".join(
            f"'{f}' absent de {n}/{len(items)} offres" for f, n in missing.items()
        )
        raise SchemaDriftError(
            f"API {ats} : format de réponse modifié ({detail}). Les réponses brutes "
            "sont conservées : corriger le parseur puis rejouer raw -> Bronze."
        )


_WORK_TYPES = [
    (re.compile(r"intern|stage|stagiaire|alternan|apprenti"), "INTERNSHIP"),
    (re.compile(r"part.?time|temps partiel"), "PART_TIME"),
    (
        re.compile(r"contract|freelanc|cdd|consultant|independant|indépendant"),
        "CONTRACT",
    ),
    (re.compile(r"temporar|interim|intérim|temp\b"), "TEMPORARY"),
    (re.compile(r"full.?time|fulltime|cdi|permanent|temps plein"), "FULL_TIME"),
]
_BLOCK_END = re.compile(
    r"<\s*(?:br\s*/?|/\s*(?:p|li|div|h[1-6]|tr|ul|ol|table|section))\s*>", re.I
)
_LEVER_PERIODS = {
    "per-year-salary": "YEARLY",
    "per-month-salary": "MONTHLY",
    "per-week-salary": "WEEKLY",
    "per-day-wage": "DAILY",
    "per-hour-wage": "HOURLY",
}


def html_to_text(fragment: str | None, escaped: bool = False) -> str | None:
    """HTML -> texte lisible, un bloc par ligne. `escaped` : HTML encodé (&lt;p&gt;)."""
    if not fragment:
        return None
    if escaped:
        fragment = html.unescape(fragment)
    # Saut de ligne aux seules balises de bloc : "Développer en <b>Kotlin</b>" doit
    # rester une phrase (get_text("\n") couperait à chaque balise, même en ligne)
    fragment = _BLOCK_END.sub("\n", fragment)
    text = BeautifulSoup(fragment, "lxml").get_text("")
    lines = (re.sub(r"[ \t ]+", " ", line).strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line) or None


def map_work_type(raw: str | None) -> str | None:
    if not raw:
        return None
    low = raw.lower()
    for pattern, code in _WORK_TYPES:
        if pattern.search(low):
            return code
    return "OTHER"


# Colonnes FIXES, quel que soit l'ATS : un fichier Bronze du jour ne doit pas changer
# de schéma selon les employeurs présents (Spark lit un lot de fichiers avec le schéma
# d'un seul : une colonne absente d'un fichier disparaîtrait silencieusement).
BRONZE_COLUMNS = [
    "job_id", "ats", "employeur_slug", "company_name", "title", "description",
    "location", "toutes_localisations", "pays_code", "job_posting_url",
    "original_listed_time", "listed_time", "expiry", "work_type", "workplace_type",
    "remote_allowed", "departement", "seniorite_source", "langue", "min_salary",
    "max_salary", "currency", "pay_period",
]  # fmt: skip


def _str(value) -> str | None:
    return None if value is None or value == "" else str(value)


def _record(**fields) -> dict:
    unknown = set(fields) - set(BRONZE_COLUMNS)
    if unknown:
        raise KeyError(f"colonnes hors contrat Bronze : {unknown}")
    return {c: _str(fields.get(c)) for c in BRONZE_COLUMNS}


# --- Greenhouse --------------------------------------------------------------------


def _gh_metadata(job: dict, name: str):
    for m in job.get("metadata") or []:
        if (m.get("name") or "").lower() == name.lower():
            return m.get("value")
    return None


def parse_greenhouse(body: dict, employeur: str, slug: str) -> list[dict]:
    if not isinstance(body, dict) or "jobs" not in body:
        raise SchemaDriftError("API greenhouse : clé 'jobs' absente de la réponse")
    check_schema("greenhouse", body["jobs"])
    out = []
    for job in body.get("jobs", []):
        candidates = [(job.get("location") or {}).get("name")] + [
            o.get("location") or o.get("name") for o in job.get("offices") or []
        ]
        candidates = [c for c in candidates if c]
        maroc = [c for c in candidates if is_morocco(c)]
        if not maroc:
            continue
        departments = job.get("departments") or []
        out.append(
            _record(
                job_id=f"greenhouse:{slug}:{job['id']}",
                ats="greenhouse",
                employeur_slug=slug,
                company_name=employeur,
                title=job.get("title"),
                description=html_to_text(job.get("content"), escaped=True),
                location=maroc[0],
                toutes_localisations=" | ".join(dict.fromkeys(candidates)),
                pays_code="MA",
                job_posting_url=job.get("absolute_url"),
                original_listed_time=job.get("first_published"),
                listed_time=job.get("updated_at"),
                expiry=job.get("application_deadline"),
                work_type=map_work_type(_gh_metadata(job, "Employment Type")),
                workplace_type=None,
                remote_allowed=None,
                departement=departments[0].get("name") if departments else None,
                seniorite_source=_gh_metadata(job, "Seniority"),
                langue=job.get("language"),
            )
        )
    return out


# --- Lever -------------------------------------------------------------------------


def _lever_description(post: dict) -> str | None:
    parts = [post.get("descriptionPlain")]
    for block in post.get("lists") or []:
        parts.append(block.get("text"))
        parts.append(html_to_text(block.get("content")))
    parts.append(post.get("additionalPlain"))
    text = "\n".join(p.strip() for p in parts if p and p.strip())
    return text or None


def parse_lever(body: list, employeur: str, slug: str) -> list[dict]:
    check_schema("lever", body)
    out = []
    for post in body:
        cat = post.get("categories") or {}
        candidates = [cat.get("location")] + list(cat.get("allLocations") or [])
        candidates = [c for c in dict.fromkeys(candidates) if c]
        maroc = [c for c in candidates if is_morocco(c)]
        if not maroc and post.get("country") != "MA":
            continue
        workplace = post.get("workplaceType")
        salary = post.get("salaryRange") or {}
        out.append(
            _record(
                job_id=f"lever:{slug}:{post['id']}",
                ats="lever",
                employeur_slug=slug,
                company_name=employeur,
                title=post.get("text"),
                description=_lever_description(post),
                location=maroc[0] if maroc else "Morocco",
                toutes_localisations=" | ".join(candidates),
                pays_code="MA",
                job_posting_url=post.get("hostedUrl"),
                # epoch en millisecondes, comme le dataset Kaggle
                original_listed_time=post.get("createdAt"),
                listed_time=post.get("createdAt"),
                expiry=None,
                work_type=map_work_type(cat.get("commitment")),
                workplace_type=workplace,
                remote_allowed="1.0" if workplace == "remote" else None,
                departement=cat.get("department"),
                seniorite_source=None,
                langue=None,
                min_salary=salary.get("min"),
                max_salary=salary.get("max"),
                currency=salary.get("currency"),
                pay_period=_LEVER_PERIODS.get(salary.get("interval")),
            )
        )
    return out


PARSERS = {"greenhouse": parse_greenhouse, "lever": parse_lever}
API_URLS = {"greenhouse": GREENHOUSE_URL, "lever": LEVER_URL}
