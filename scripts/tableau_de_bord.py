"""Tableau de bord web : page HTML autonome générée depuis les agrégats Gold.

  docker compose run --rm --no-deps app python -m scripts.tableau_de_bord

Lit les vues bi.* et les tables gold.* (jamais les annonces), injecte les chiffres
dans scripts/tableau_de_bord.html et écrit docs/dashboard/index.html : un seul
fichier, sans serveur, publiable tel quel (GitHub Pages). Mêmes règles que le
guide Power BI : une source à la fois, couverture affichée avec tout salaire,
médiane masquée sous 30 salaires.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from src.utils.cli import run_cli
from src.utils.config import get_settings
from src.utils.db import connect

TEMPLATE = Path(__file__).with_suffix(".html")
OUT = Path("docs/dashboard/index.html")
SOURCES = {
    "kaggle_linkedin": "Dataset LinkedIn (États-Unis, 2023-2024)",
    "ats_maroc": "Offres au Maroc (API de recrutement)",
}
MIN_SALAIRES = 30
N_TECH, N_COOC = 15, 10


def rows(cur, sql: str, params=()) -> list[dict]:
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [
        {
            c: (float(v) if hasattr(v, "is_finite") else v)
            for c, v in zip(cols, r, strict=True)
        }
        for r in cur.fetchall()
    ]


def source_data(cur, src: str) -> dict:
    familles = rows(cur, "SELECT famille_poste f, SUM(nb_offres)::int n "
                    "FROM bi.offres_mensuelles WHERE source = %s GROUP BY 1 "
                    "ORDER BY 2 DESC", (src,))  # fmt: skip
    total = sum(r["n"] for r in familles)
    tech = sum(r["n"] for r in familles if r["f"] != "AUTRE")
    villes = rows(cur, "SELECT ville v, etat_code e, SUM(nb_offres)::int n "
                  "FROM gold.offres_par_ville WHERE source = %s "
                  "AND famille_poste <> 'AUTRE' GROUP BY 1, 2 ORDER BY 3 DESC",
                  (src,))  # fmt: skip
    sans_ville = sum(r["n"] for r in villes if r["v"] == "Non précisée")
    villes = [r for r in villes if r["v"] != "Non précisée"][:10]
    techs = rows(cur, "SELECT technologie t, nb_offres n, part_offres_tech p "
                 "FROM bi.tech_classement WHERE source = %s "
                 "ORDER BY nb_offres DESC, technologie LIMIT %s",
                 (src, N_TECH))  # fmt: skip
    top = [r["t"] for r in techs[:N_COOC]]
    cooc = rows(cur, "SELECT tech_a a, tech_b b, nb_offres_communes c, "
                "nb_offres_a na, nb_offres_b nb FROM gold.cooccurrence_tech "
                "WHERE source = %s AND tech_a = ANY(%s) AND tech_b = ANY(%s)",
                (src, top, top))  # fmt: skip
    supports = {}
    for r in cooc:
        supports[r["a"]], supports[r["b"]] = r["na"], r["nb"]
    couverture = rows(cur, "SELECT nb_offres, nb_avec_salaire, taux_couverture "
                      "FROM bi.couverture_salaires WHERE source = %s",
                      (src,))  # fmt: skip
    salaires = rows(cur, "SELECT famille_poste f, niveau_experience niv, nb_offres, "
                    "nb_avec_salaire, nb_salaires_usd n_usd, salaire_p25_usd p25, "
                    "salaire_median_usd med, salaire_p75_usd p75 FROM bi.salaires "
                    "WHERE source = %s AND famille_poste <> 'AUTRE'",
                    (src,))  # fmt: skip
    for r in salaires:  # même règle que la mesure Power BI
        if r["n_usd"] < MIN_SALAIRES:
            r["p25"] = r["med"] = r["p75"] = None
    experience = rows(cur, "SELECT famille_poste f, tranche t, ordre_tranche o, "
                      "nb_offres n, part_famille p FROM gold.experience_demandee "
                      "WHERE source = %s AND famille_poste <> 'AUTRE' "
                      "ORDER BY f, o", (src,))  # fmt: skip
    prime = rows(cur, "SELECT technologie t, effet_pct e, nb_offres n "
                 "FROM gold.prime_salariale_tech WHERE source = %s "
                 "ORDER BY effet_pct DESC", (src,))  # fmt: skip
    return {
        "libelle": SOURCES[src],
        "total": total,
        "tech": tech,
        "familles": familles,
        "villes": villes,
        "sans_ville": sans_ville,
        "techs": techs,
        "cooc": {"techs": top, "paires": cooc, "supports": supports},
        "couverture": couverture[0] if couverture else None,
        "salaires": salaires,
        "experience": experience,
        "prime": prime,
    }


def build() -> dict:
    with connect(get_settings()) as conn, conn.cursor() as cur:
        data = {src: source_data(cur, src) for src in SOURCES}
    return {"genere_le": date.today().isoformat(), "min_salaires": MIN_SALAIRES,
            "sources": data}  # fmt: skip


def render(data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    payload = payload.replace("</", "<\\/")  # jamais de </script> dans les données
    return TEMPLATE.read_text(encoding="utf-8").replace("/*__DONNEES__*/null", payload)


def main(argv: list[str] | None = None) -> int:
    html = render(build())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"{OUT} ({len(html.encode()) // 1024} Ko)")
    return 0


if __name__ == "__main__":
    run_cli(main)
