"""Modèle d'estimation du salaire annuel (USD), suivi dans MLflow.

  docker compose run --rm app python -m src.ml.salaire

Données : silver/offres de la source `kaggle_linkedin` (États-Unis). La source
marocaine n'a AUCUN salaire publié (0 sur 37 offres) : le modèle décrit le marché
américain du dataset de développement, pas le marché marocain.

Cible : log(salaire annuel médian de la fourchette). Exclus : salaires signalés en
anomalie (jour 5), devises autres que USD, republications (doublons d'annonces).
Variables : uniquement ce qui existe AUSSI quand le salaire manque (sinon l'imputation
serait impossible) : famille, niveau, contrat, télétravail, état, région, expérience,
technologies, titre (TF-IDF -> SVD).
Découpage : par ENTREPRISE (GroupShuffleSplit) ; les annonces quasi identiques d'une
même entreprise ne se retrouvent jamais des deux côtés (sinon score gonflé).

5 configurations comparées : 2 références naïves puis 3 LightGBM. La meilleure est
enregistrée dans le registre MLflow, expliquée par SHAP (effet associé à chaque
technologie -> gold.prime_salariale_tech) et sert à imputer les salaires manquants.
"""

from __future__ import annotations

import argparse
import tempfile
import time
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import GroupShuffleSplit

SOURCE = "kaggle_linkedin"
EXPERIMENT = "salaire_annuel_usd"
MODEL_NAME = "salaire_annuel_usd"
SEED = 42
CATEGORIES = [
    "famille_poste", "niveau_experience", "type_contrat", "teletravail",
    "etat_code", "region",
]  # fmt: skip
N_TECH = 80  # technologies les plus fréquentes, en indicatrices
MIN_OFFRES_SHAP = 50  # en dessous, l'effet SHAP d'une technologie est trop bruité
MODEL_REQUIREMENTS = ["lightgbm", "scikit-learn", "pandas", "numpy", "skops"]


# --- Données -----------------------------------------------------------------------


def load_offers(spark, settings, source: str = SOURCE) -> pd.DataFrame:
    """Offres uniques de la source (hors republications), avec ou sans salaire."""
    from pyspark.sql import functions as F

    df = spark.read.parquet(
        settings.s3_path(f"silver/offres/source={source}", scheme="s3a")
    ).where(~F.col("est_republication"))
    anomalie_salaire = F.exists("_anomalies", lambda a: a.startswith("salaire"))
    return df.select(
        "offre_id", "entreprise_normalisee", "titre_normalise", *CATEGORIES,
        "experience_min_annees", "technologies", "salaire_min_annuel",
        "salaire_max_annuel", "devise", anomalie_salaire.alias("salaire_anormal"),
    ).toPandas()  # fmt: skip


def training_set(offers: pd.DataFrame) -> pd.DataFrame:
    """Offres au salaire exploitable, avec la cible y = log(salaire médian)."""
    ok = (
        offers["salaire_min_annuel"].notna()
        & offers["salaire_max_annuel"].notna()
        & (offers["devise"] == "USD")
        & ~offers["salaire_anormal"].fillna(False).astype(bool)
    )
    df = offers[ok].copy()
    df["salaire"] = (df["salaire_min_annuel"] + df["salaire_max_annuel"]) / 2
    df["y"] = np.log(df["salaire"])
    return df.reset_index(drop=True)


def split_by_company(df: pd.DataFrame, test_size: float = 0.2, seed: int = SEED):
    """Entraînement / test disjoints PAR ENTREPRISE."""
    groups = df["entreprise_normalisee"].fillna("(inconnue)")
    train_idx, test_idx = next(
        GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed).split(
            df, groups=groups
        )
    )
    return df.iloc[train_idx].copy(), df.iloc[test_idx].copy()


# --- Variables ---------------------------------------------------------------------


def top_technologies(techs: pd.Series, n: int = N_TECH) -> list[str]:
    counts = pd.Series([t for lst in techs for t in (lst if lst is not None else [])])
    return sorted(counts.value_counts().head(n).index) if len(counts) else []


def tech_matrix(techs: pd.Series, vocab: list[str]) -> pd.DataFrame:
    """Indicatrices tech_<nom> (1 si l'offre cite la technologie)."""
    sets = [set(lst) if lst is not None else set() for lst in techs]
    return pd.DataFrame(
        {f"tech_{t}": [int(t in s) for s in sets] for t in vocab}, index=techs.index
    )


@dataclass
class FeatureBuilder:
    """Ajusté sur l'ENTRAÎNEMENT seulement, puis appliqué au test et à l'imputation."""

    with_title: bool = True
    n_svd: int = 30
    tech_vocab: list[str] = field(default_factory=list)
    categories: dict[str, list] = field(default_factory=dict)
    tfidf: TfidfVectorizer | None = None
    svd: TruncatedSVD | None = None

    def fit(self, df: pd.DataFrame) -> FeatureBuilder:
        self.tech_vocab = top_technologies(df["technologies"])
        self.categories = {
            c: sorted(df[c].dropna().astype(str).unique()) for c in CATEGORIES
        }
        if self.with_title:
            self.tfidf = TfidfVectorizer(
                ngram_range=(1, 2), min_df=5, max_features=20000
            )
            matrix = self.tfidf.fit_transform(df["titre_normalise"].fillna(""))
            self.svd = TruncatedSVD(n_components=self.n_svd, random_state=SEED)
            self.svd.fit(matrix)
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        for c in CATEGORIES:  # catégories inconnues à l'entraînement -> manquantes
            out[c] = pd.Categorical(
                df[c].astype("string"), categories=self.categories[c]
            )
        out["experience_min_annees"] = df["experience_min_annees"].astype("float")
        out = out.join(tech_matrix(df["technologies"], self.tech_vocab))
        if self.with_title:
            svd = self.svd.transform(
                self.tfidf.transform(df["titre_normalise"].fillna(""))
            )
            cols = [f"titre_svd_{i}" for i in range(svd.shape[1])]
            out = out.join(pd.DataFrame(svd, index=df.index, columns=cols))
        return out


# --- Métriques et modèles ----------------------------------------------------------


def metrics(y_true_log: np.ndarray, y_pred_log: np.ndarray) -> dict[str, float]:
    """Erreurs en DOLLARS (on prédit le log, on évalue en salaire)."""
    true, pred = np.exp(y_true_log), np.exp(y_pred_log)
    err = np.abs(pred - true)
    ss_res = np.sum((y_true_log - y_pred_log) ** 2)
    ss_tot = np.sum((y_true_log - np.mean(y_true_log)) ** 2)
    return {
        "mae_usd": float(err.mean()),
        "erreur_mediane_usd": float(np.median(err)),
        "erreur_relative_mediane": float(np.median(err / true)),
        "r2_log": float(1 - ss_res / ss_tot) if ss_tot else 0.0,
    }


def median_baseline(
    train: pd.DataFrame, test: pd.DataFrame, by: list[str]
) -> np.ndarray:
    """Médiane (en log) par groupe, repli sur la médiane globale."""
    glob = train["y"].median()
    if not by:
        return np.full(len(test), glob)
    med = train.groupby(by, dropna=False, observed=True)["y"].median().rename("_m")
    joined = test[by].merge(med, left_on=by, right_index=True, how="left")
    return joined["_m"].fillna(glob).to_numpy()


_BASE = dict(n_estimators=600, learning_rate=0.05, num_leaves=31, min_child_samples=30)
LGBM_CONFIGS = {
    "lgbm_sans_titre": dict(with_title=False, params=_BASE),
    "lgbm_avec_titre": dict(with_title=True, params=_BASE),
    "lgbm_avec_titre_regularise": dict(
        with_title=True,
        params=dict(
            n_estimators=1500, learning_rate=0.03, num_leaves=63, min_child_samples=40,
            subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
        ),
    ),
}  # fmt: skip


def fit_lgbm(train: pd.DataFrame, config: dict):
    import lightgbm as lgb

    builder = FeatureBuilder(with_title=config["with_title"]).fit(train)
    model = lgb.LGBMRegressor(random_state=SEED, verbose=-1, **config["params"])
    model.fit(builder.transform(train), train["y"])
    return builder, model


# --- SHAP : effet associé à chaque technologie --------------------------------------


def tech_effects(model, X: pd.DataFrame, train: pd.DataFrame) -> pd.DataFrame:
    """Effet SHAP moyen d'une technologie sur les offres qui la citent.

    En log : +0,10 ~ +10,5 % de salaire. C'est une ASSOCIATION à niveau, famille,
    lieu et titre comparables dans le modèle, pas un effet causal.
    """
    import shap

    values = shap.TreeExplainer(model).shap_values(X)
    rows = []
    for j, col in enumerate(X.columns):
        if not col.startswith("tech_"):
            continue
        present = X[col].to_numpy() == 1
        n_train = int(
            train["technologies"]
            .apply(lambda lst, t=col[5:]: lst is not None and t in lst)
            .sum()
        )
        if present.sum() == 0 or n_train < MIN_OFFRES_SHAP:
            continue
        mean_log = float(values[present, j].mean())
        rows.append(
            {
                "technologie": col[5:],
                "nb_offres": n_train,
                "shap_moyen_log": mean_log,
                "effet_pct": (np.exp(mean_log) - 1) * 100,
            }
        )
    return (
        pd.DataFrame(rows)
        .sort_values("effet_pct", ascending=False)
        .reset_index(drop=True)
    )


def plot_effects(effects: pd.DataFrame, path: Path, top: int = 15) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sel = pd.concat([effects.head(top), effects.tail(top)]).drop_duplicates(
        "technologie"
    )
    sel = sel.sort_values("effet_pct")
    fig, ax = plt.subplots(figsize=(8, 0.32 * len(sel) + 1))
    ax.barh(
        sel["technologie"],
        sel["effet_pct"],
        color=np.where(sel["effet_pct"] >= 0, "#2a7", "#c55"),
    )
    ax.axvline(0, color="#555", lw=0.8)
    ax.set_xlabel("effet associé sur le salaire (%, SHAP moyen)")
    ax.set_title("Technologies et salaire (dataset US, association non causale)")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


# --- Gold et imputation ---------------------------------------------------------------


def publish_effects(effects: pd.DataFrame, source: str, run_id: str, settings) -> None:
    from src.utils.db import connect

    with connect(settings) as conn, conn.cursor() as cur:  # une transaction
        cur.execute(
            "DELETE FROM gold.prime_salariale_tech WHERE source = %s", (source,)
        )
        cur.executemany(
            "INSERT INTO gold.prime_salariale_tech (source, technologie, nb_offres, "
            "effet_pct, shap_moyen_log, mlflow_run_id) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (
                    source,
                    r.technologie,
                    r.nb_offres,
                    r.effet_pct,
                    r.shap_moyen_log,
                    run_id,
                )
                for r in effects.itertuples()
            ],
        )


def impute(
    builder, model, offers: pd.DataFrame, settings, source: str, run_id: str
) -> int:
    """Salaire estimé pour les offres qui n'en publient pas -> Parquet dans MinIO."""
    missing = offers[offers["salaire_min_annuel"].isna()].copy()
    if missing.empty:
        return 0
    missing["salaire_estime_usd"] = np.exp(model.predict(builder.transform(missing)))
    out = missing[["offre_id"]].assign(
        salaire_estime_usd=missing["salaire_estime_usd"].round(0), mlflow_run_id=run_id
    )
    out.to_parquet(
        settings.s3_path(f"ml/predictions/salaire/source={source}/data.parquet"),
        storage_options=settings.storage_options,
        index=False,
    )
    return len(out)


# --- Orchestration de l'entraînement ------------------------------------------------


def run(
    offers: pd.DataFrame, settings, source: str = SOURCE, publish: bool = True
) -> dict:
    import mlflow
    import mlflow.lightgbm

    data = training_set(offers)
    train, test = split_by_company(data)
    common = {
        "source": source,
        "offres_entrainement": len(train),
        "offres_test": len(test),
        "entreprises_test": test["entreprise_normalisee"].nunique(),
        "decoupage": "par entreprise (GroupShuffleSplit 80/20)",
    }
    mlflow.set_experiment(EXPERIMENT)
    results = {}

    for name, by in (
        ("mediane_globale", []),
        ("mediane_famille_niveau", ["famille_poste", "niveau_experience"]),
    ):
        with mlflow.start_run(run_name=name):
            mlflow.log_params({**common, "modele": name})
            m = metrics(test["y"].to_numpy(), median_baseline(train, test, by))
            mlflow.log_metrics(m)
            results[name] = {**m, "run_id": mlflow.active_run().info.run_id}

    fitted = {}
    for name, config in LGBM_CONFIGS.items():
        with mlflow.start_run(run_name=name):
            t0 = time.perf_counter()
            builder, model = fit_lgbm(train, config)
            X_test = builder.transform(test)
            m = metrics(test["y"].to_numpy(), model.predict(X_test))
            m["duree_entrainement_s"] = time.perf_counter() - t0
            params = {"modele": name, "avec_titre": config["with_title"],
                      "nb_variables": X_test.shape[1], **config["params"]}  # fmt: skip
            mlflow.log_params({**common, **params})
            mlflow.log_metrics(m)
            fitted[name] = (builder, model, X_test, mlflow.active_run().info.run_id)
            results[name] = {**m, "run_id": mlflow.active_run().info.run_id}

    best = min(fitted, key=lambda k: results[k]["mae_usd"])
    builder, model, X_test, run_id = fitted[best]
    effects = tech_effects(model, X_test, train)
    with mlflow.start_run(run_id=run_id):
        mlflow.set_tag("meilleure_configuration", "oui")
        with tempfile.TemporaryDirectory() as tmp:
            csv, png = (
                Path(tmp) / "effets_technologies.csv",
                Path(tmp) / "effets_technologies.png",
            )
            effects.to_csv(csv, index=False)
            plot_effects(effects, png)
            mlflow.log_artifact(str(csv))
            mlflow.log_artifact(str(png))
        # Le modèle prend les variables préparées par FeatureBuilder (catégories
        # pandas) : MLflow ne sait pas en déduire une signature, donc pas d'exemple
        # d'entrée. Les noms des variables restent dans le modèle LightGBM.
        # Dépendances déclarées (versions installées) : la déduction automatique
        # recharge le modèle dans un sous-processus (~3 min ici) et en oubliait.
        mlflow.lightgbm.log_model(
            model, name="modele", registered_model_name=MODEL_NAME,
            pip_requirements=[f"{p}=={version(p)}" for p in MODEL_REQUIREMENTS],
        )  # fmt: skip
        n_imputed = (
            impute(builder, model, offers, settings, source, run_id) if publish else 0
        )
        mlflow.log_metric("offres_imputees", n_imputed)
    if publish:
        publish_effects(effects, source, run_id, settings)
    return {
        "resultats": results,
        "meilleur": best,
        "effets": effects,
        "imputees": n_imputed,
    }


def main(argv: list[str] | None = None) -> int:
    from src.utils.config import get_settings
    from src.utils.db import apply_schema
    from src.utils.spark import get_spark

    p = argparse.ArgumentParser(description="Modèle de salaire (MLflow)")
    p.add_argument("--source", default=SOURCE)
    args = p.parse_args(argv)
    settings = get_settings()
    apply_schema(settings)
    spark = get_spark("ml_salaire", settings)
    try:
        offers = load_offers(spark, settings, args.source)
    finally:
        spark.stop()
    out = run(offers, settings, args.source)

    print(f"\n[ml] {len(offers):,} offres uniques ; comparaison des 5 configurations :")
    for name, r in sorted(out["resultats"].items(), key=lambda kv: kv[1]["mae_usd"]):
        mark = "  <- retenu" if name == out["meilleur"] else ""
        mae, med = r["mae_usd"], r["erreur_mediane_usd"]
        rel, r2 = r["erreur_relative_mediane"], r["r2_log"]
        print(f"  {name:<28} MAE {mae:>9,.0f} $ | erreur médiane {med:>8,.0f} $ "
              f"({rel:.0%}) | R² (log) {r2:.3f}{mark}")  # fmt: skip
    eff = out["effets"]
    print("\n[ml] technologies associées aux salaires les plus élevés (SHAP) :")
    for r in eff.head(8).itertuples():
        print(f"  {r.technologie:<16} {r.effet_pct:+6.1f} %  ({r.nb_offres} offres)")
    print("[ml] ... et les plus bas :")
    for r in eff.tail(5).itertuples():
        print(f"  {r.technologie:<16} {r.effet_pct:+6.1f} %  ({r.nb_offres} offres)")
    n = out["imputees"]
    print(f"\n[ml] {n:,} salaires manquants estimés -> ml/predictions/salaire/")
    return 0


if __name__ == "__main__":
    from src.utils.cli import run_cli

    run_cli(main)
