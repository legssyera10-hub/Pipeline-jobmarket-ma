"""Extraction des technologies citées dans une offre (reference/technologies.yaml).

Délimitation d'une variante :
- avant : pas de lettre, chiffre, _, +, #, . collé (sinon "sparkling" -> spark,
  "ASP.NET" -> .net seul) ;
- après : pas de lettre, chiffre, _, +, # (sinon "C" dans "C++", "java" dans "javax") ;
- exception : transition minuscule -> Majuscule ("PythonLinux", "experienceSQL"),
  fréquente car le HTML des descriptions a été retiré sans espace. Désactivée par
  `strict: true` (sql : sinon "NoSQL" et "MySQL" donneraient sql).
Les lookarounds sont sensibles à la casse ; seules les variantes sont dans (?i:...).

Performance : chaque entrée a des « indices » (ses variantes en minuscules) ; la regex
n'est évaluée que si un indice est présent dans le texte (recherche de sous-chaîne,
bien plus rapide que 174 regex sur 3 600 caractères).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "reference" / "technologies.yaml"

_BEFORE_STRICT = r"(?<![A-Za-z0-9_+#.])"
_AFTER_STRICT = r"(?![A-Za-z0-9_+#])"
_CAMEL = r"(?<=[a-z])(?=[A-Z])"


@dataclass(frozen=True)
class TechPattern:
    name: str
    hints: tuple[str, ...]  # vide = toujours évaluer (motifs regex bruts)
    pattern: re.Pattern

    def found_in(self, text: str, text_lower: str) -> bool:
        if self.hints and not any(h in text_lower for h in self.hints):
            return False
        return self.pattern.search(text) is not None


def _alternation(variants: list[str]) -> str:
    # Les plus longues d'abord : "power bi" avant "power"
    return "|".join(re.escape(v) for v in sorted(variants, key=len, reverse=True))


def _compile_entry(name: str, spec) -> TechPattern:
    if isinstance(spec, list):
        spec = {"variants": spec}
    variants = [str(v) for v in spec.get("variants", [])]
    variants_cs = [str(v) for v in spec.get("variants_cs", [])]
    regexes = [str(r) for r in spec.get("regex", [])]
    strict = spec.get("strict", False)
    exclude = spec.get("exclude_after", [])

    before = _BEFORE_STRICT if strict else f"(?:{_BEFORE_STRICT}|{_CAMEL})"
    after = _AFTER_STRICT if strict else f"(?:{_AFTER_STRICT}|{_CAMEL})"
    not_after = f"(?!(?i:{'|'.join(exclude)}))" if exclude else ""

    groups = []
    if variants:
        groups.append(f"(?i:{_alternation(variants)})")
    if variants_cs:
        groups.append(f"(?:{_alternation(variants_cs)})")
    alternatives = []
    if groups:
        alternatives.append(f"{before}(?:{'|'.join(groups)}){after}{not_after}")
    alternatives.extend(f"(?:{r})" for r in regexes)
    if not alternatives:
        raise ValueError(f"technologie '{name}' sans variante ni regex")

    hints = () if regexes else tuple({v.lower() for v in variants + variants_cs})
    return TechPattern(name, hints, re.compile("|".join(alternatives)))


@lru_cache(maxsize=4)
def load_patterns(path: str | Path = DEFAULT_PATH) -> tuple[TechPattern, ...]:
    with open(path, encoding="utf-8") as f:
        mapping = yaml.safe_load(f)
    return tuple(_compile_entry(name, spec) for name, spec in mapping.items())


def extract_technologies(
    text: str | None, patterns: tuple[TechPattern, ...] | None = None
) -> list[str]:
    """Noms canoniques des technologies citées, triés, sans doublon."""
    if not text:
        return []
    patterns = patterns or load_patterns()
    lower = text.lower()
    return sorted(p.name for p in patterns if p.found_in(text, lower))
