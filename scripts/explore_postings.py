"""Exploration de data/raw/postings.csv sur un échantillon (jamais le fichier entier).

Usage :
  docker compose run --rm --no-deps app python -m scripts.explore_postings [nrows]
"""

from __future__ import annotations

import sys

import pandas as pd

PATH = "data/raw/postings.csv"
TIME_COLS = ["original_listed_time", "listed_time", "expiry", "closed_time"]
SALARY_COLS = ["min_salary", "med_salary", "max_salary", "normalized_salary"]


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main(nrows: int) -> None:
    df = pd.read_csv(PATH, nrows=nrows)
    pd.set_option("display.width", 160, "display.max_rows", 100)

    section(f"Échantillon : {len(df):,} lignes x {df.shape[1]} colonnes")
    print(
        pd.DataFrame(
            {
                "dtype": df.dtypes.astype(str),
                "remplissage_%": (df.notna().mean() * 100).round(1),
                "n_distinct": df.nunique(),
            }
        ).to_string()
    )

    section("Unicité de job_id")
    print(f"doublons job_id : {df['job_id'].duplicated().sum()}")

    section("Colonnes temporelles (valeurs brutes puis interprétées en epoch ms)")
    for col in TIME_COLS:
        raw = df[col].dropna()
        as_dt = pd.to_datetime(raw, unit="ms", utc=True)
        print(
            f"{col:<22} ex. brut={raw.iloc[0]!r:<16} "
            f"min={as_dt.min()}  max={as_dt.max()}"
        )
    listed = pd.to_datetime(df["listed_time"], unit="ms", utc=True)
    orig = pd.to_datetime(df["original_listed_time"], unit="ms", utc=True)
    print(f"listed_time != original_listed_time : {(listed != orig).mean():.1%}")
    print(
        "jours de republication (listed - original), quantiles :",
        (listed - orig).dt.days.quantile([0.5, 0.9, 0.99]).to_dict(),
    )
    print("\nOffres par jour de original_listed_time (top 15) :")
    print(orig.dt.date.value_counts().sort_index().tail(15).to_string())
    print(f"nombre de jours distincts : {orig.dt.date.nunique()}")

    section("Structure des salaires")
    for col in ["pay_period", "currency", "compensation_type"]:
        print(f"\n{col} :\n{df[col].value_counts(dropna=False).to_string()}")
    has = df[["min_salary", "med_salary", "max_salary"]].notna()
    combos = has.apply(
        lambda r: "+".join(c.split("_")[0] for c in has.columns if r[c]) or "aucun",
        axis=1,
    )
    print(
        f"\ncombinaisons min/med/max renseignées :\n{combos.value_counts().to_string()}"
    )
    print("\nstatistiques par pay_period :")
    print(df.groupby("pay_period")[SALARY_COLS].median().round(0).to_string())
    both = df.dropna(subset=["min_salary", "max_salary"])
    print(f"min > max : {(both['min_salary'] > both['max_salary']).sum()} lignes")

    section("Autres colonnes catégorielles")
    for col in [
        "formatted_work_type",
        "formatted_experience_level",
        "remote_allowed",
        "application_type",
        "sponsored",
    ]:
        print(f"\n{col} :\n{df[col].value_counts(dropna=False).head(8).to_string()}")

    section("Localisation (top 10)")
    print(df["location"].value_counts().head(10).to_string())

    section("Longueur des descriptions (caractères)")
    print(df["description"].str.len().describe().round(0).to_string())


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 50_000)
