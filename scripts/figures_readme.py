"""Figures du README, générées depuis les agrégats Gold (jamais depuis les annonces).

  docker compose run --rm --no-deps app python -m scripts.figures_readme

Aperçu reproductible du tableau de bord : mêmes données que Power BI (vues bi.*),
mêmes règles (couverture affichée, médiane masquée sous 30 salaires). Sortie :
docs/img/*.png. Source : dataset Kaggle (États-Unis) ; la source marocaine (37 offres)
est trop petite pour ces graphiques.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from src.utils.cli import run_cli  # noqa: E402
from src.utils.config import get_settings  # noqa: E402
from src.utils.db import connect  # noqa: E402

OUT = Path("docs/img")
SOURCE = "kaggle_linkedin"
MIN_SALAIRES = 30  # même seuil que bi.salaires.mediane_fiable

# Palette de référence (validée : scripts/validate_palette.js du skill dataviz)
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, RED = "#2a78d6", "#e34948"
FAMILLES = {
    "DATA_ENG": "Data engineering",
    "DATA_SCI": "Data science",
    "AI_ML": "IA / ML",
    "BI": "BI / analyse",
    "DEV": "Développement",
}


def entier(v: float) -> str:
    """12345 -> « 12 345 » (séparateur de milliers français)."""
    return f"{v:,.0f}".replace(",", " ")


def pct(v: float, decimals: int = 1) -> str:
    """Pourcentage signé à la française : +8,9 % ; 0 sans signe."""
    if round(v, decimals) == 0:
        return "0 %"
    return f"{v:+.{decimals}f}".replace(".", ",") + " %"


def query(sql: str, params=()) -> pd.DataFrame:
    with connect(get_settings()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def style(ax, title: str, subtitle: str, note: str | None = None) -> None:
    fig = ax.figure
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    fig.text(0.02, 0.97, title, fontsize=13, fontweight="bold", color=INK, va="top")
    fig.text(0.02, 0.905, subtitle, fontsize=9.5, color=INK_2, va="top")
    if note:
        fig.text(0.02, 0.02, note, fontsize=8, color=INK_2, va="bottom")
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def hbars(ax, labels, values, color, fmt) -> None:
    """Barres horizontales fines, la plus grande en haut, valeur en bout de barre."""
    y = range(len(labels))[::-1]
    ax.barh(list(y), values, height=0.62, color=color, edgecolor=SURFACE, linewidth=2)
    ax.set_yticks(list(y), labels)
    span = max(abs(v) for v in values)
    for yi, v in zip(y, values, strict=True):
        ax.text(v + span * 0.01, yi, fmt(v), va="center", fontsize=8.5, color=INK)
    ax.set_xlim(0, span * 1.15)


def save(fig, name: str) -> Path:
    path = OUT / name
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return path


def fig_familles() -> Path:
    df = query(
        "SELECT famille_poste, SUM(nb_offres) n FROM bi.offres_mensuelles "
        "WHERE source = %s GROUP BY 1 ORDER BY 2 DESC",
        (SOURCE,),
    )
    autre = int(df.loc[df.famille_poste == "AUTRE", "n"].sum())
    tech = df[df.famille_poste != "AUTRE"]
    fig, ax = plt.subplots(figsize=(8, 3.6))
    fig.subplots_adjust(left=0.2, right=0.95, top=0.78, bottom=0.14)
    hbars(ax, [FAMILLES[f] for f in tech.famille_poste], tech.n.astype(int).tolist(),
          BLUE, entier)  # fmt: skip
    ax.xaxis.set_major_formatter(lambda v, _: entier(v))
    style(ax, "Offres tech par famille de poste",
          f"Offres uniques, dataset LinkedIn US 2023-2024 ; {entier(autre)} offres "
          "non tech exclues")  # fmt: skip
    return save(fig, "offres_par_famille.png")


def fig_villes() -> Path:
    df = query(
        "SELECT ville || ' (' || etat_code || ')' AS ville, SUM(nb_offres) n "
        "FROM gold.offres_par_ville WHERE source = %s AND famille_poste <> 'AUTRE' "
        "GROUP BY ville, etat_code ORDER BY n DESC",
        (SOURCE,),
    )
    inconnue = int(df.loc[df.ville.str.startswith("Non précisée"), "n"].sum())
    top = df[~df.ville.str.startswith("Non précisée")].head(10)
    fig, ax = plt.subplots(figsize=(8, 4.4))
    fig.subplots_adjust(left=0.24, right=0.95, top=0.82, bottom=0.12)
    hbars(ax, top.ville.tolist(), top.n.astype(int).tolist(), BLUE, str)
    style(ax, "Villes qui publient le plus d'offres tech",
          "Offres tech uniques, 10 premières villes",
          f"{entier(inconnue)} offres tech sans ville précise "
          "(télétravail, « United States »…)")  # fmt: skip
    return save(fig, "top_villes.png")


def fig_technologies() -> Path:
    df = query(
        "SELECT technologie, part_offres_tech FROM bi.tech_classement "
        "WHERE source = %s ORDER BY nb_offres DESC LIMIT 15",
        (SOURCE,),
    )
    fig, ax = plt.subplots(figsize=(8, 5.4))
    fig.subplots_adjust(left=0.17, right=0.95, top=0.86, bottom=0.08)
    hbars(ax, df.technologie.tolist(), (df.part_offres_tech * 100).tolist(), BLUE,
          lambda v: f"{v:.0f} %")  # fmt: skip
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0f} %")
    style(ax, "Technologies les plus demandées",
          "Part des offres tech qui citent la technologie (15 premières)")  # fmt: skip
    return save(fig, "technologies.png")


def fig_salaires() -> Path:
    df = query(
        "SELECT famille_poste, nb_offres, nb_avec_salaire, nb_salaires_usd, "
        "salaire_p25_usd p25, salaire_median_usd med, salaire_p75_usd p75 "
        "FROM bi.salaires WHERE source = %s AND niveau_experience = 'CONFIRME' "
        "AND famille_poste <> 'AUTRE' ORDER BY med DESC NULLS LAST",
        (SOURCE,),
    )
    df = df[df.nb_salaires_usd >= MIN_SALAIRES]
    cov = query("SELECT taux_couverture FROM bi.couverture_salaires WHERE source = %s",
                (SOURCE,)).iloc[0, 0]  # fmt: skip
    fig, ax = plt.subplots(figsize=(8, 3.8))
    fig.subplots_adjust(left=0.2, right=0.95, top=0.76, bottom=0.2)
    y = list(range(len(df)))[::-1]
    k = 1 / 1000
    for yi, r in zip(y, df.itertuples(), strict=True):
        ax.plot([r.p25 * k, r.p75 * k], [yi, yi], color=BLUE, linewidth=2,
                solid_capstyle="round", alpha=0.45)  # fmt: skip
        ax.plot(r.med * k, yi, "o", color=BLUE, markersize=8,
                markeredgecolor=SURFACE, markeredgewidth=2)  # fmt: skip
        ax.text(r.p75 * k + 3, yi, f"{r.med * k:.0f} k$  (n = {r.nb_salaires_usd})",
                va="center", fontsize=8.5, color=INK)  # fmt: skip
    ax.set_yticks(y, [FAMILLES[f] for f in df.famille_poste])
    ax.set_xlim(df.p25.min() * k * 0.9, df.p75.max() * k * 1.25)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0f} k$")
    style(ax, "Salaire annuel, niveau confirmé",
          "Médiane (point) et moitié centrale des salaires, 25e-75e centile (trait)",
          f"Seulement {cov * 100:.0f} % des offres publient un salaire : n = salaires "
          f"utilisés. Médiane masquée sous {MIN_SALAIRES} salaires. "
          "Dollars US.")  # fmt: skip
    return save(fig, "salaires.png")


def fig_prime_tech() -> Path:
    df = query(
        "SELECT technologie, effet_pct, nb_offres FROM gold.prime_salariale_tech "
        "WHERE source = %s ORDER BY effet_pct DESC",
        (SOURCE,),
    )
    sel = pd.concat([df.head(8), df.tail(6)]).drop_duplicates("technologie")
    fig, ax = plt.subplots(figsize=(8, 5.2))
    fig.subplots_adjust(left=0.18, right=0.95, top=0.84, bottom=0.14)
    y = list(range(len(sel)))[::-1]
    colors = [BLUE if v >= 0 else RED for v in sel.effet_pct]
    ax.barh(y, sel.effet_pct, height=0.62, color=colors, edgecolor=SURFACE, linewidth=2)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    for yi, v in zip(y, sel.effet_pct, strict=True):  # signe écrit : pas que la couleur
        ax.text(v + (0.25 if v >= 0 else -0.25), yi, pct(v), va="center",
                ha="left" if v >= 0 else "right", fontsize=8.5, color=INK)  # fmt: skip
    ax.set_yticks(y, sel.technologie)
    lim = sel.effet_pct.abs().max() * 1.35
    ax.set_xlim(-lim, lim)
    ax.xaxis.set_major_formatter(lambda v, _: pct(v, 0))
    style(ax, "Technologies et salaire estimé (modèle LightGBM, SHAP)",
          "Écart moyen du salaire estimé des offres qui citent la technologie",
          "Association à profil comparable (famille, niveau, lieu, titre), "
          "pas un effet causal. Dataset US, au moins 50 offres par "
          "technologie.")  # fmt: skip
    return save(fig, "prime_technologies.png")


def main(argv: list[str] | None = None) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    for build in (fig_familles, fig_villes, fig_technologies, fig_salaires,
                  fig_prime_tech):  # fmt: skip
        print(f"  {build()}")
    return 0


if __name__ == "__main__":
    run_cli(main)
