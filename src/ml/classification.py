"""Classification des offres par famille de poste : règles actuelles vs modèles.

  # 1. tirer l'échantillon à annoter (402 offres, reproductible)
  docker compose run --rm app python -m src.ml.classification echantillon
  # 2. remplir la colonne `famille_reelle` de data/annotation/familles.csv
  #    (guide : docs/guide_annotation.md)
  # 3. comparer règles et modèles, suivi dans MLflow
  docker compose run --rm --no-deps app python -m src.ml.classification evaluer

Pourquoi annoter à la main : sans vérité terrain, on ne peut ni mesurer les règles
actuelles (src/transform/text.py), ni entraîner un modèle autrement qu'en imitant ces
mêmes règles, ce qui ne prouverait rien.

L'échantillon est stratifié selon la famille prédite par les règles (sinon 94 % des
offres seraient « AUTRE » et certaines familles absentes), puis MÉLANGÉ : l'annotateur
ne voit ni l'ordre ni la prédiction. Conséquence : les scores mesurent la qualité de
séparation des familles, pas la précision sur la population réelle.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from src.transform.text import FAMILIES, classify_family

PATH = Path("data/annotation/familles.csv")
EXPERIMENT = "classification_famille"
SEED = 42
PAR_FAMILLE_TECH = 65
N_AUTRE = 40
MIN_ANNOTEES = 150
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


# --- Échantillon ---------------------------------------------------------------------


def choose_ids(offers: pd.DataFrame, seed: int = SEED) -> list[str]:
    """Toutes les offres marocaines + un tirage par famille (règles) du dataset US.

    N'a besoin que de offre_id, source et famille_poste : le texte des offres n'est
    chargé qu'ensuite, pour les seules offres retenues. Ordre rendu : mélangé.
    """
    ma = offers[offers["source"] == "ats_maroc"]
    us = offers[offers["source"] != "ats_maroc"]
    parts = [ma]
    for fam in FAMILIES:
        pool = us[us["famille_poste"] == fam]
        n = N_AUTRE if fam == "AUTRE" else PAR_FAMILLE_TECH
        parts.append(pool.sample(n=min(n, len(pool)), random_state=seed))
    return pd.concat(parts).sample(frac=1, random_state=seed)["offre_id"].tolist()


def format_sample(details: pd.DataFrame, ids: list[str]) -> pd.DataFrame:
    """Fichier à annoter, dans l'ordre de `ids`, sans la prédiction des règles."""
    cols = ["offre_id", "source", "titre_brut", "entreprise", "description"]
    out = details.set_index("offre_id").loc[ids].reset_index()[cols]
    out["description"] = (
        out["description"].fillna("").str.replace(r"\s+", " ", regex=True).str[:1500]
    )
    out.insert(0, "num", range(1, len(out) + 1))
    out["famille_reelle"] = ""
    return out


def stratified_sample(offers: pd.DataFrame, seed: int = SEED) -> pd.DataFrame:
    return format_sample(offers, choose_ids(offers, seed))


def sample_command() -> int:
    from src.utils.config import get_settings
    from src.utils.spark import get_spark

    if PATH.exists():
        print(f"{PATH} existe déjà : on n'écrase pas un fichier en cours d'annotation")
        return 2
    from pyspark.sql import functions as F

    s = get_settings()
    spark = get_spark("annotation_familles", s)
    try:
        offers = spark.read.parquet(s.s3_path("silver/offres", scheme="s3a")).where(
            "source in ('kaggle_linkedin', 'ats_maroc') and not est_republication"
        )
        # 1. tirage sur les colonnes légères (ordre stable -> reproductible) ;
        # rapatrier les 115 000 descriptions saturait le driver Spark (jour 8)
        light = (
            offers.select("offre_id", "source", "famille_poste")
            .orderBy("offre_id")
            .toPandas()
        )
        ids = choose_ids(light)
        # 2. texte des seules offres retenues
        details = (
            offers.where(F.col("offre_id").isin(ids))
            .select("offre_id", "source", "titre_brut", "entreprise", "description")
            .toPandas()
        )
    finally:
        spark.stop()
    sample = format_sample(details, ids)
    PATH.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(PATH, index=False, encoding="utf-8")
    n_ma = int((sample["source"] == "ats_maroc").sum())
    print(f"{len(sample)} offres -> {PATH} (dont {n_ma} au Maroc)")
    print("Guide : docs/guide_annotation.md")
    return 0


# --- Évaluation ----------------------------------------------------------------------


def load_annotations(path: Path = PATH) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["famille_reelle"] = df["famille_reelle"].str.strip().str.upper()
    done = df[df["famille_reelle"] != ""].copy()
    invalid = done[~done["famille_reelle"].isin(FAMILIES)]
    if not invalid.empty:
        lignes = invalid["num"].tolist()[:10]
        codes = sorted(invalid["famille_reelle"].unique())
        raise ValueError(
            f"familles inconnues (attendu {FAMILIES}) aux lignes num {lignes} : {codes}"
        )
    done["texte"] = done["titre_brut"] + " \n " + done["description"]
    return done.reset_index(drop=True)


def score(y_true, y_pred) -> dict:
    from sklearn.metrics import accuracy_score, classification_report, f1_score

    labels = [f for f in FAMILIES if f in set(y_true) | set(y_pred)]
    kw = {"labels": labels, "zero_division": 0}
    return {
        "f1_macro": float(f1_score(y_true, y_pred, average="macro", **kw)),
        "exactitude": float(accuracy_score(y_true, y_pred)),
        "rapport": classification_report(y_true, y_pred, **kw),
    }


def evaluate_rules(df: pd.DataFrame) -> dict:
    """Les règles actuelles, appliquées au titre brut comme dans Silver."""
    return score(df["famille_reelle"], df["titre_brut"].map(classify_family))


def _folds(y: pd.Series) -> int:
    return int(max(2, min(5, y.value_counts().min())))


def evaluate_tfidf(df: pd.DataFrame, seed: int = SEED) -> dict:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedKFold, cross_val_predict
    from sklearn.pipeline import make_pipeline

    pipe = make_pipeline(
        TfidfVectorizer(ngram_range=(1, 2), min_df=2, sublinear_tf=True),
        LogisticRegression(max_iter=2000, class_weight="balanced"),
    )
    cv = StratifiedKFold(
        n_splits=_folds(df["famille_reelle"]), shuffle=True, random_state=seed
    )
    pred = cross_val_predict(pipe, df["texte"], df["famille_reelle"], cv=cv)
    return score(df["famille_reelle"], pred)


def evaluate_embeddings(df: pd.DataFrame, seed: int = SEED) -> dict | None:
    """Embeddings multilingues (FR/EN) + régression logistique, si installés."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedKFold, cross_val_predict

    X = SentenceTransformer(EMBEDDING_MODEL).encode(df["texte"].tolist(), batch_size=32)
    cv = StratifiedKFold(
        n_splits=_folds(df["famille_reelle"]), shuffle=True, random_state=seed
    )
    pred = cross_val_predict(
        LogisticRegression(max_iter=2000, class_weight="balanced"),
        np.asarray(X), df["famille_reelle"], cv=cv,
    )  # fmt: skip
    return score(df["famille_reelle"], pred)


def evaluate_command() -> int:
    import mlflow

    if not PATH.exists():
        print(f"{PATH} absent : lancer d'abord `echantillon`, puis annoter")
        return 2
    df = load_annotations()
    if len(df) < MIN_ANNOTEES:
        print(f"{len(df)} offres annotées : au moins {MIN_ANNOTEES} sont nécessaires "
              "pour une évaluation qui ait un sens")  # fmt: skip
        return 2
    mlflow.set_experiment(EXPERIMENT)
    candidates = {
        "regles_actuelles": evaluate_rules,
        "tfidf_logreg": evaluate_tfidf,
        "embeddings_logreg": evaluate_embeddings,
    }
    print(f"[ml] {len(df)} offres annotées ; répartition : "
          f"{df['famille_reelle'].value_counts().to_dict()}")  # fmt: skip
    for name, fn in candidates.items():
        res = fn(df)
        if res is None:
            print(f"  {name:<20} non évalué : sentence-transformers non installé")
            continue
        with mlflow.start_run(run_name=name):
            validation = "règles : directe ; modèles : croisée stratifiée"
            mlflow.log_params(
                {"modele": name, "offres_annotees": len(df), "validation": validation}
            )
            mlflow.log_metrics(
                {"f1_macro": res["f1_macro"], "exactitude": res["exactitude"]}
            )
            with tempfile.TemporaryDirectory() as tmp:
                rapport = Path(tmp) / "rapport.txt"
                rapport.write_text(res["rapport"], encoding="utf-8")
                mlflow.log_artifact(str(rapport))
        f1, acc = res["f1_macro"], res["exactitude"]
        print(f"  {name:<20} F1 macro {f1:.3f} | exactitude {acc:.3f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Classification des familles de poste")
    p.add_argument("commande", choices=["echantillon", "evaluer"])
    args = p.parse_args(argv)
    return sample_command() if args.commande == "echantillon" else evaluate_command()


if __name__ == "__main__":
    from src.utils.cli import run_cli

    run_cli(main)
