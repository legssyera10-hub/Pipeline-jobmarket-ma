"""Contrôles au niveau du jeu de données (Great Expectations 1.x) : portillon Silver.

Les règles ligne à ligne (rules.py) écartent les offres inexploitables. Elles ne voient
pas ce qui n'apparaît qu'à l'échelle du lot : doublons, valeurs hors référentiel, ou
dérive d'un taux de remplissage (la source change de format et 100 % des villes
deviennent vides — chaque ligne reste « valide », le lot ne l'est plus).

- suite CRITIQUE      : un échec bloque la publication de Silver (code de sortie 1) ;
- suite AVERTISSEMENT : un échec est journalisé, la publication continue.
Les seuils `mostly` = valeur observée sur le dataset de référence moins une marge
(mesures du jour 3 : ville 93,4 %, état 89,8 %, entreprise 98,6 %).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from pyspark.sql import DataFrame

import great_expectations as gx
import great_expectations.expectations as gxe
from src.quality.rules import SALAIRE_MAX_PLAUSIBLE, SALAIRE_MIN_PLAUSIBLE

REFERENCE_DIR = Path(__file__).resolve().parents[2] / "reference"
SEUIL_TAUX_REJET = 0.05  # au-delà, le lot est suspect même si chaque rejet est justifié

FAMILLES = ["AI_ML", "DATA_SCI", "DATA_ENG", "BI", "DEV", "AUTRE"]
TYPES_CONTRAT = [
    "FULL_TIME", "PART_TIME", "CONTRACT", "TEMPORARY", "INTERNSHIP", "VOLUNTEER",
    "AUTRE",
]  # fmt: skip
TELETRAVAIL = ["TELETRAVAIL", "HYBRIDE", "SUR_SITE", "NON_PRECISE"]
NIVEAUX = ["STAGE", "DEBUTANT", "ASSOCIE", "CONFIRME", "DIRECTEUR", "DIRIGEANT"]


# Seuils par source (D22 : valeur observée moins une marge). Le profil "defaut" a
# été calibré sur le dataset américain ; une source d'instantanés peut n'avoir aucune
# NOUVELLE offre un jour donné (min_lignes = 0), et n'a pas d'état américain.
PROFILS = {
    "defaut": {
        "min_lignes": 1,
        "ville_mostly": 0.85,
        "etat_mostly": 0.80,
        "entreprise_mostly": 0.95,
        "experience_mostly": 0.45,
        "devises": ["USD"],
    },
    "ats_maroc": {
        "min_lignes": 0,
        "ville_mostly": 0.60,  # provisoire : à recalibrer après 2 semaines
        "etat_mostly": None,  # pas d'état : contrôle sans objet
        "entreprise_mostly": 0.99,  # l'employeur vient du référentiel
        "experience_mostly": 0.30,
        "devises": ["MAD", "EUR", "USD"],
    },
}


def profil(source: str) -> dict:
    return PROFILS.get(source, PROFILS["defaut"])


def etat_codes() -> list[str]:
    with open(REFERENCE_DIR / "us_states.csv", encoding="utf-8") as f:
        return [row["etat_code"] for row in csv.DictReader(f)]


def critical_suite(source: str = "defaut") -> gx.ExpectationSuite:
    p = profil(source)
    suite = gx.ExpectationSuite(name="silver_critique")
    expectations = [
        gxe.ExpectTableRowCountToBeBetween(min_value=p["min_lignes"]),
        gxe.ExpectColumnValuesToBeUnique(column="offre_id"),
        *[
            gxe.ExpectColumnValuesToNotBeNull(column=c)
            for c in (
                "offre_id",
                "source",
                "titre_normalise",
                "date_publication",
                "date_collecte",
            )
        ],  # fmt: skip
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="date_collecte", column_B="date_publication", or_equal=True
        ),
        gxe.ExpectColumnValuesToBeInSet(column="famille_poste", value_set=FAMILLES),
        gxe.ExpectColumnValuesToBeInSet(column="type_contrat", value_set=TYPES_CONTRAT),
        gxe.ExpectColumnValuesToBeInSet(column="teletravail", value_set=TELETRAVAIL),
        gxe.ExpectColumnValuesToBeInSet(column="niveau_experience", value_set=NIVEAUX),
        gxe.ExpectColumnValuesToBeInSet(column="etat_code", value_set=etat_codes()),
        gxe.ExpectColumnValuesToBeBetween(
            column="experience_min_annees", min_value=0, max_value=20
        ),
        # Dérive des taux de remplissage (format de source modifié, parseur cassé)
        gxe.ExpectColumnValuesToNotBeNull(
            column="ville_normalisee", mostly=p["ville_mostly"]
        ),
        gxe.ExpectColumnValuesToNotBeNull(
            column="entreprise_normalisee", mostly=p["entreprise_mostly"]
        ),
    ]
    if p["etat_mostly"] is not None:
        expectations.append(
            gxe.ExpectColumnValuesToNotBeNull(
                column="etat_code", mostly=p["etat_mostly"]
            )
        )
    for e in expectations:
        suite.add_expectation(e)
    return suite


def warning_suite(source: str = "defaut") -> gx.ExpectationSuite:
    p = profil(source)
    suite = gx.ExpectationSuite(name="silver_avertissement")
    for e in [
        gxe.ExpectColumnValuesToBeBetween(
            column="salaire_min_annuel",
            min_value=SALAIRE_MIN_PLAUSIBLE,
            max_value=SALAIRE_MAX_PLAUSIBLE,
            mostly=0.98,
        ),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="date_expiration",
            column_B="date_publication",
            or_equal=True,
            ignore_row_if="either_value_is_missing",
        ),
        gxe.ExpectColumnValuesToNotBeNull(
            column="experience_min_annees", mostly=p["experience_mostly"]
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="devise", value_set=p["devises"], mostly=0.99
        ),
    ]:
        suite.add_expectation(e)
    return suite


@dataclass
class QualityReport:
    results: list[dict] = field(default_factory=list)

    @property
    def critical_ok(self) -> bool:
        return all(r["succes"] for r in self.results if r["suite"] == "critique")

    def failures(self, suite: str) -> list[dict]:
        return [r for r in self.results if r["suite"] == suite and not r["succes"]]


def _to_row(suite: str, r) -> dict:
    kw = r.expectation_config.kwargs
    res = r.result or {}
    column = kw.get("column") or (
        f"{kw['column_A']} >= {kw['column_B']}" if "column_A" in kw else None
    )
    observed = res.get("observed_value")
    examples = res.get("partial_unexpected_list") or []
    return {
        "suite": suite,
        "controle": r.expectation_config.type,
        "colonne": column,
        "succes": bool(r.success),
        "nb_inattendus": res.get("unexpected_count"),
        "pct_inattendus": res.get("unexpected_percent"),
        "valeur_observee": None if observed is None else str(observed),
        "exemples": ", ".join(map(str, examples[:5])) or None,
    }


@lru_cache(maxsize=1)
def _context():
    return gx.get_context(mode="ephemeral")


def validate(
    valid: DataFrame, n_rejets: int, motifs: dict[str, int], source: str = "defaut"
) -> QualityReport:
    """Valide le lot Silver ; le taux de rejet est ajouté comme contrôle critique."""
    ctx = _context()
    name = f"lot_{id(valid)}"  # un asset par appel : le contexte est partagé
    batch = (
        ctx.data_sources.add_or_update_spark(name=name)
        .add_dataframe_asset(name="offres")
        .add_batch_definition_whole_dataframe("lot")
        .get_batch(batch_parameters={"dataframe": valid})
    )
    report = QualityReport()
    for label, suite in (
        ("critique", critical_suite(source)),
        ("avertissement", warning_suite(source)),
    ):
        for r in batch.validate(suite).results:
            report.results.append(_to_row(label, r))
    ctx.data_sources.delete(name)

    n_valid = next(
        int(r["valeur_observee"])
        for r in report.results
        if r["controle"] == "expect_table_row_count_to_be_between"
    )
    total = n_valid + n_rejets
    rate = n_rejets / total if total else 0.0
    report.results.append(
        {
            "suite": "critique",
            "controle": "taux_de_rejet_maximal",
            "colonne": None,
            "succes": rate <= SEUIL_TAUX_REJET,
            "nb_inattendus": n_rejets,
            "pct_inattendus": rate * 100,
            "valeur_observee": f"{rate:.2%} (seuil {SEUIL_TAUX_REJET:.0%})",
            "exemples": ", ".join(f"{k}={v}" for k, v in motifs.items()) or None,
        }
    )
    return report


def log_report(report: QualityReport, source: str, start, end, settings=None) -> None:
    """Historise les résultats dans ops.controles_qualite (suivi dans le temps)."""
    from src.utils.db import connect

    cols = ["suite", "controle", "colonne", "succes", "nb_inattendus",
            "pct_inattendus", "valeur_observee", "exemples"]  # fmt: skip
    with connect(settings) as conn, conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO ops.controles_qualite (source, date_debut, date_fin, "
            f"{', '.join(cols)}) VALUES (%s, %s, %s, {', '.join(['%s'] * len(cols))})",
            [(source, start, end, *[r[c] for c in cols]) for r in report.results],
        )
