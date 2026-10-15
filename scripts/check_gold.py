"""Contrôle de la couche Gold (PostgreSQL) et aperçu des résultats.

Usage :
  docker compose run --rm --no-deps app python -m scripts.check_gold [--empreintes]

--empreintes : n'affiche que l'empreinte MD5 du contenu de chaque table (comparer deux
exécutions du job Gold prouve son idempotence).
"""

from __future__ import annotations

import sys

from src.transform.gold import TABLES
from src.utils.db import connect

QUERIES = {
    "Technologies les plus demandées (offres tech, avril 2024)": """
        SELECT technologie, nb_offres, round(100 * part_relative)::int AS pct
        FROM gold.tech_par_mois
        WHERE perimetre = 'tech' AND mois = '2024-04-01'
        ORDER BY nb_offres DESC LIMIT 10""",
    "Paires de technologies les plus associées (>= 30 offres communes)": """
        SELECT tech_a, tech_b, nb_offres_communes,
               round(jaccard::numeric, 2) AS jaccard, round(lift::numeric, 1) AS lift
        FROM gold.cooccurrence_tech
        WHERE nb_offres_communes >= 30
        ORDER BY jaccard DESC LIMIT 10""",
    "Expérience demandée par famille (offres uniques, % de la famille)": """
        SELECT famille_poste, tranche, nb_offres, round(100 * part_famille)::int AS pct
        FROM gold.experience_demandee
        WHERE famille_poste IN ('DATA_ENG', 'DATA_SCI', 'BI')
        ORDER BY famille_poste, ordre_tranche""",
    "Villes avec le plus d'offres data/IA (offres uniques)": """
        SELECT ville, etat_code, sum(nb_offres) AS offres
        FROM gold.offres_par_ville
        WHERE famille_poste IN ('DATA_ENG', 'DATA_SCI', 'BI', 'AI_ML')
          AND ville <> 'Non précisée'
        GROUP BY ville, etat_code ORDER BY offres DESC LIMIT 8""",
    "Derniers contrôles qualité en échec (ops.controles_qualite)": """
        SELECT source, execute_le::timestamp(0), suite, controle, colonne
        FROM ops.controles_qualite WHERE NOT succes
        ORDER BY execute_le DESC, id LIMIT 8""",
}


def fingerprints(cur) -> dict[str, tuple[int, str]]:
    out = {}
    for t in TABLES:
        cur.execute(
            f"SELECT count(*), md5(coalesce(string_agg(r::text, '|' ORDER BY r::text),"
            f" '')) FROM gold.{t} r"
        )
        out[t] = cur.fetchone()
    return out


def main() -> int:
    with connect() as conn, conn.cursor() as cur:
        fp = fingerprints(cur)
        for t, (n, h) in fp.items():
            print(f"gold.{t:<20} {n:>7,} lignes  md5={h}")
        if "--empreintes" in sys.argv:
            return 0
        for title, sql in QUERIES.items():
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            print(f"\n--- {title}\n  " + " | ".join(cols))
            for row in cur.fetchall():
                print("  " + " | ".join(str(v) for v in row))
    return 0 if all(n > 0 for n, _ in fp.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
