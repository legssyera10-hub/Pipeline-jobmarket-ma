"""Mesure de la précision de l'extraction des technologies sur un échantillon annoté.

1. Tirer l'échantillon (100 offres tech, graine fixe -> reproductible) :
     docker compose run --rm app python -m scripts.annotation_technologies sample
   -> data/annotation/technologies.csv (non versionné : contient des annonces)
2. Ouvrir le CSV, remplir la colonne `technologies_reelles` pour chaque offre
   (noms canoniques de reference/technologies.yaml, séparés par ";"), en lisant la
   description SANS regarder `technologies_extraites`.
3. Calculer le score :
     docker compose run --rm --no-deps app \
         python -m scripts.annotation_technologies score
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import pandas as pd

PATH = Path("data/annotation/technologies.csv")
SEED = 42
SIZE = 100


def sample() -> None:
    from pyspark.sql import functions as F

    from src.utils.config import get_settings
    from src.utils.spark import get_spark

    if PATH.exists():
        sys.exit(
            f"{PATH} existe déjà : on n'écrase pas un fichier en cours d'annotation"
        )
    s = get_settings()
    spark = get_spark("annotation", s)
    try:
        df = (
            spark.read.parquet(s.s3_path("silver/offres", scheme="s3a"))
            .where(F.col("famille_poste") != "AUTRE")
            .orderBy("offre_id")  # ordre stable avant tirage -> reproductible
            .select(
                "offre_id",
                "titre_brut",
                "description",
                F.array_join("technologies", ";").alias("technologies_extraites"),
            )
            .toPandas()
        )
    finally:
        spark.stop()
    out = df.sample(n=SIZE, random_state=SEED).assign(technologies_reelles="")
    PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(PATH, index=False, encoding="utf-8")
    print(f"{len(out)} offres -> {PATH}")


def _split(cell) -> set[str]:
    if pd.isna(cell) or not str(cell).strip():
        return set()
    return {t.strip() for t in str(cell).split(";") if t.strip()}


def score_frame(df: pd.DataFrame) -> dict:
    """Précision / rappel micro, plus les erreurs les plus fréquentes."""
    tp = fp = fn = 0
    false_pos, false_neg = Counter(), Counter()
    for _, row in df.iterrows():
        pred, gold = _split(row["technologies_extraites"]), _split(
            row["technologies_reelles"]
        )
        tp += len(pred & gold)
        fp += len(pred - gold)
        fn += len(gold - pred)
        false_pos.update(pred - gold)
        false_neg.update(gold - pred)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "offres": len(df),
        "precision": precision,
        "rappel": recall,
        "f1": f1,
        "faux_positifs": false_pos.most_common(10),
        "faux_negatifs": false_neg.most_common(10),
    }


def score() -> None:
    df = pd.read_csv(PATH, dtype=str, keep_default_na=False)
    done = df[df["technologies_reelles"].str.strip() != ""]
    if done.empty:
        sys.exit("aucune offre annotée (colonne technologies_reelles vide)")
    # Une offre sans aucune techno s'annote "-" pour la distinguer d'une case oubliée
    done = done.assign(
        technologies_reelles=done["technologies_reelles"].replace("-", "")
    )
    r = score_frame(done)
    print(f"{r['offres']} offres annotées sur {len(df)}")
    print(
        f"précision {r['precision']:.1%} | rappel {r['rappel']:.1%} | F1 {r['f1']:.1%}"
    )
    print(f"faux positifs les plus fréquents : {r['faux_positifs']}")
    print(f"faux négatifs les plus fréquents : {r['faux_negatifs']}")


if __name__ == "__main__":
    {"sample": sample, "score": score}[sys.argv[1] if len(sys.argv) > 1 else "score"]()
