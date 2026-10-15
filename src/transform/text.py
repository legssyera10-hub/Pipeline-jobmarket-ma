"""Normalisations de texte de la couche Silver (fonctions pures, testables sans Spark).

Les clés normalisées (titre, entreprise, ville) servent à la déduplication : deux
écritures différentes de la même offre doivent produire la même clé.
"""

from __future__ import annotations

import re
import unicodedata


def strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    # NFC : recompose ce que NFKD a décomposé sans accent (syllabes coréennes)
    return unicodedata.normalize("NFC", stripped)


def normalize_whitespace(text: str | None) -> str | None:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned or None


# --- URL ---------------------------------------------------------------------


def clean_url(url: str | None) -> str | None:
    """Retire query string et fragment : "…/view/123/?trk=x" -> "…/view/123"."""
    if not url:
        return None
    return re.split(r"[?#]", url.strip(), maxsplit=1)[0].rstrip("/") or None


# --- Titre -------------------------------------------------------------------

_BRACKETS = re.compile(r"\([^)]*\)|\[[^\]]*\]")
_GENDER = re.compile(r"\b(?:h\s*/\s*f|f\s*/\s*h|m\s*/\s*[fw]\s*/\s*d|m\s*/\s*f)\b")
_TITLE_ABBREV = {
    "sr": "senior",
    "snr": "senior",
    "jr": "junior",
    "mgr": "manager",
    "engr": "engineer",
    "eng": "engineer",
    "dev": "developer",
    "assoc": "associate",
    "asst": "assistant",
    "mgmt": "management",
}


def normalize_title(title: str | None) -> str | None:
    """'Sr. Data Engineer (Remote) - H/F' -> 'senior data engineer'."""
    if not title:
        return None
    t = strip_accents(title).lower()
    t = _BRACKETS.sub(" ", t)
    t = _GENDER.sub(" ", t)
    # \w (Unicode) et non [a-z0-9] : un titre chinois, grec, hébreu ou arabe ne doit
    # pas devenir vide (jour 5 : 6 offres envoyées à tort en quarantaine)
    t = re.sub(r"[^\w+#]+|_", " ", t)
    words = [_TITLE_ABBREV.get(w, w) for w in t.split()]
    return " ".join(words) or None


# --- Entreprise ----------------------------------------------------------------

_LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation", "co",
    "company", "lp", "llp", "plc", "pllc", "pc", "sa", "sas", "sarl", "gmbh", "ag",
    "bv", "nv",
}  # fmt: skip


def normalize_company(name: str | None) -> str | None:
    """'COMPA Industries, Inc.' -> 'compa industries' ; 'The Ritz' -> 'ritz'."""
    if not name:
        return None
    c = strip_accents(name).lower().replace("&", " and ")
    # Sigles à points : "S.A." -> "sa", "U.S. Bank" -> "us bank"
    c = re.sub(r"\b([a-z])\.(?=[a-z]\b)", r"\1", c)
    words = re.sub(r"[^\w]+|_", " ", c).split()  # Unicode : voir normalize_title
    while len(words) > 1 and words[-1] in _LEGAL_SUFFIXES:
        words.pop()
    if len(words) > 1 and words[0] == "the":
        words.pop(0)
    return " ".join(words) or None


# --- Lieu ----------------------------------------------------------------------

_COUNTRY_ONLY = {"united states", "usa", "us"}
_NO_PLACE = {"namer", "north america", "remote", "anywhere"}
_STATE_SUFFIX = re.compile(r"\s+(?:metropolitan\s+)?area$", re.IGNORECASE)
_METRO_PREFIX = re.compile(r"^greater\s+", re.IGNORECASE)
_METRO_SUFFIX = re.compile(
    r"\s+(?:metropolitan area|metro area|metroplex|metro|bay area|area|region)$",
    re.IGNORECASE,
)


def _clean_city(city: str) -> str | None:
    return normalize_whitespace(city.strip(" .")) or None


def parse_location(raw: str | None) -> tuple[str | None, str | None, str | None]:
    """Lieu LinkedIn -> (ville, état brut [code ou nom], pays).

    'Austin, TX'                              -> ('Austin', 'TX', 'US')
    'Portland, Oregon Metropolitan Area'      -> ('Portland', 'Oregon', 'US')
    'Los Angeles, California, United States'  -> ('Los Angeles', 'California', 'US')
    'Greater Houston'                         -> ('Houston', None, 'US')
    'United States'                           -> (None, None, 'US')
    L'état brut est résolu ensuite par jointure avec reference/us_states.csv.
    """
    s = normalize_whitespace(raw)
    if not s:
        return (None, None, None)
    low = s.lower()
    if low in _COUNTRY_ONLY:
        return (None, None, "US")
    if low in _NO_PLACE:
        return (None, None, None)

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) >= 3 and parts[-1].lower() in _COUNTRY_ONLY:
        return (_clean_city(parts[0]), parts[1], "US")
    if len(parts) == 2:
        city, second = parts
        if second.lower() in _COUNTRY_ONLY:
            return (_clean_city(city), None, "US")
        return (_clean_city(city), _STATE_SUFFIX.sub("", second) or None, "US")
    if len(parts) == 1:
        # Zone métropolitaine : on garde la ville principale (la 1re citée)
        name = _METRO_SUFFIX.sub("", _METRO_PREFIX.sub("", s))
        first = re.split(r"--|-(?=[A-Z])", name)[0] if "-" in name else name
        return (_clean_city(first), None, "US")
    return (_clean_city(parts[0]), None, None)


# --- Famille de poste ------------------------------------------------------------
# Règles provisoires : remplacées par un classifieur au jour 8. Ordre = priorité
# ("Software Engineer, Machine Learning" -> AI_ML avant DEV).

FAMILIES = ["AI_ML", "DATA_SCI", "DATA_ENG", "BI", "DEV", "AUTRE"]

_FAMILY_RULES: list[tuple[str, re.Pattern, re.Pattern | None]] = [
    (
        "AI_ML",
        re.compile(
            r"machine learning|\bml\b|mlops|\bai\b|artificial intelligence"
            r"|intelligence artificielle|deep learning|computer vision|\bnlp\b"
            r"|natural language|\bllms?\b|generative ai|genai|prompt engineer"
            # Français : "IA" seulement après un mot de métier ("Des Moines, IA" = Iowa)
            r"|\b(?:ingenieur|architecte|expert|consultant|developpeur|data|chef de"
            r" projet)\b[\w &/-]{0,15}\bia\b|\bia generative|apprentissage automatique"
        ),
        None,
    ),
    (
        "DATA_SCI",
        re.compile(
            r"data scien|scientifique des donnees|statistician|statisticien"
            r"|quantitative (?:analyst|researcher)|decision scien"
        ),
        None,
    ),
    (
        "DATA_ENG",
        re.compile(
            r"data engineer|ingenieur (?:de )?(?:donnees|data)|\betl\b|data architect"
            r"|data platform|analytics engineer|data warehouse|big data|data pipeline"
            r"|data integration|database (?:engineer|developer|admin)|\bdba\b"
            r"|architecte (?:de )?(?:donnees|data)|administrateur (?:de )?bases? de"
        ),
        None,
    ),
    (
        "BI",
        re.compile(
            r"business intelligence|\bbi\b|data analyst|analyste (?:de )?donnees"
            r"|reporting analyst|analytics (?:analyst|manager|lead|specialist)"
            r"|data visuali|power bi|tableau|insights analyst|data analytics"
            r"|decisionnel|analyste bi"
        ),
        None,
    ),
    (
        "DEV",
        re.compile(
            r"software (?:engineer|developer|architect|development)|\bdeveloper\b"
            r"|developpeur|programmer|full ?-?stack|front ?-?end|back ?-?end|devops"
            r"|\bsre\b|site reliability|cloud engineer|web developer|\bios\b"
            r"|android|platform engineer|qa automation|test automation|\bsdet\b"
            r"|ingenieur (?:logiciel|cloud|devops|systemes?|reseaux?|developpement)"
            r"|ingenieur (?:d.)?etudes et developpement|tech lead|lead dev"
        ),
        re.compile(r"business develop|real estate|property developer|sales"),
    ),
]


def classify_family(title: str | None) -> str:
    if not title:
        return "AUTRE"
    t = strip_accents(title).lower()
    for family, include, exclude in _FAMILY_RULES:
        if include.search(t) and not (exclude and exclude.search(t)):
            return family
    return "AUTRE"
