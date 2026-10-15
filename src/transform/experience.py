"""Extraction des années d'expérience demandées, depuis la description (EN + FR).

Une mention n'est retenue que si elle est accompagnée du mot « experience » juste
avant ou juste après : sans ce contexte, « at least 18 years of age » ou « 40+ years
of serving our community » seraient comptés. Valeurs > 20 ans ignorées (historique
de l'entreprise, pas une exigence).

Plusieurs mentions dans une offre : on garde la plus FAIBLE, car les descriptions
listent souvent des alternatives (« 14 ans d'expérience ; OU master et 12 ans ») et le
minimum exigé est la plus petite. Coût accepté : « 1+ an de SDLC, 5 ans de dev »
donne 1. À arbitrer avec l'échantillon annoté.
"""

from __future__ import annotations

import re

from src.transform.text import strip_accents

MAX_MIN_YEARS = 20
MAX_MAX_YEARS = 30

_WORD_NUMBERS = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5", "six": "6",
    "seven": "7", "eight": "8", "nine": "9", "ten": "10",
}  # fmt: skip
_WORDS = "|".join(_WORD_NUMBERS)
# "five (5) years" -> "5 years" ; "three years" -> "3 years"
_WORD_WITH_DIGIT = re.compile(rf"\b(?:{_WORDS})\s*\((\d{{1,2}})\)")
_WORD_NUMBER = re.compile(rf"\b({_WORDS})\b(?=\s*\+?\s*(?:years?|yrs?)\b)")

_MENTION = re.compile(
    r"(?<!\d)(\d{1,2})\s*\+?\s*"  # 3 / 3+ (sans \b : "science1+ years" collé)
    r"(?:(?:-|to|a)\s*(\d{1,2})\s*\+?\s*)?"  # - 5 / to 5 / à 5 (accents retirés)
    r"(?:years?|yrs?|ans)\b"
)
_AGE_AFTER = re.compile(r"\s*(?:of\s+age|old|d'age)\b")
_EXPERIENCE = re.compile(r"experi|\bexp\b")
_SENTENCE_END = re.compile(r"[.;!?]\s|\n")


def _normalize(text: str) -> str:
    t = strip_accents(text).lower()
    t = t.replace("\u2019", "'").replace("\u2018", "'").replace("\u00a0", " ")
    t = t.replace("\u2013", "-").replace("\u2014", "-")
    t = _WORD_WITH_DIGIT.sub(r"\1", t)
    return _WORD_NUMBER.sub(lambda m: _WORD_NUMBERS[m.group(1)], t)


def _has_experience_context(t: str, start: int, end: int) -> bool:
    after = t[end : end + 70]
    if after.startswith("."):  # abréviation "yrs." : pas une fin de phrase
        after = after[1:]
    cut = _SENTENCE_END.search(after)
    after = after[: cut.start()] if cut else after
    before = t[max(0, start - 40) : start]
    cut = list(_SENTENCE_END.finditer(before))
    before = before[cut[-1].end() :] if cut else before
    return bool(_EXPERIENCE.search(after) or _EXPERIENCE.search(before))


def find_mentions(text: str | None) -> list[tuple[int, int | None]]:
    """Toutes les mentions valides (min, max) trouvées dans le texte."""
    if not text:
        return []
    t = _normalize(text)
    mentions = []
    for m in _MENTION.finditer(t):
        if _AGE_AFTER.match(t, m.end()):
            continue
        if not _has_experience_context(t, m.start(), m.end()):
            continue
        low = int(m.group(1))
        high = int(m.group(2)) if m.group(2) else None
        if low > MAX_MIN_YEARS:
            continue
        if high is not None and not (low <= high <= MAX_MAX_YEARS):
            continue
        mentions.append((low, high))
    return mentions


def parse_experience(text: str | None) -> tuple[int | None, int | None]:
    """'3-5 years of experience' -> (3, 5) ; '5+ years experience' -> (5, None)."""
    mentions = find_mentions(text)
    if not mentions:
        return (None, None)
    return min(mentions, key=lambda m: (m[0], m[1] is None, m[1] or 0))
