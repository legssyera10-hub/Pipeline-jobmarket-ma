"""Couche ML : données synthétiques uniquement (aucune annonce réelle)."""

import numpy as np
import pandas as pd
import pytest

from src.ml import classification as clf
from src.ml import salaire as sal

# --- Salaire -------------------------------------------------------------------------


def _offers(n: int = 600, seed: int = 0) -> pd.DataFrame:
    """Offres fictives : salaire = base(niveau) x 1,20 si `spark` est cité."""
    rng = np.random.default_rng(seed)
    niveau = rng.choice(["Entry level", "Mid-Senior level"], n)
    has_spark = rng.random(n) < 0.4
    techs = [
        ["spark", "python"] if s else (["python"] if i % 2 else ["excel"])
        for i, s in enumerate(has_spark)
    ]
    base = np.where(niveau == "Entry level", 70_000, 120_000) * np.where(
        has_spark, 1.2, 1.0
    )
    salary = base * np.exp(rng.normal(0, 0.03, n))
    return pd.DataFrame(
        {
            "offre_id": [f"o{i}" for i in range(n)],
            "entreprise_normalisee": [f"ent{i % 60}" for i in range(n)],
            "titre_normalise": rng.choice(
                ["data engineer", "software engineer", "analyst"], n
            ),
            "famille_poste": rng.choice(["DATA_ENG", "DEV"], n),
            "niveau_experience": niveau,
            "type_contrat": "Full-time",
            "teletravail": rng.choice(["oui", "non"], n),
            "etat_code": rng.choice(["CA", "NY", "TX"], n),
            "region": "West",
            "experience_min_annees": rng.integers(0, 8, n).astype(float),
            "technologies": techs,
            "salaire_min_annuel": salary * 0.9,
            "salaire_max_annuel": salary * 1.1,
            "devise": "USD",
            "salaire_anormal": False,
        }
    )


def test_training_set_exclut_anomalies_devises_et_manquants():
    df = _offers(10)
    df.loc[0, "salaire_anormal"] = True
    df.loc[1, "devise"] = "EUR"
    df.loc[2, "salaire_min_annuel"] = np.nan
    out = sal.training_set(df)
    assert len(out) == 7
    row = df.iloc[3]
    expected = np.log((row["salaire_min_annuel"] + row["salaire_max_annuel"]) / 2)
    assert out.loc[out["offre_id"] == "o3", "y"].item() == pytest.approx(expected)


def test_decoupage_sans_entreprise_commune():
    train, test = sal.split_by_company(sal.training_set(_offers()))
    assert set(train["entreprise_normalisee"]).isdisjoint(test["entreprise_normalisee"])
    assert 0.1 < len(test) / (len(train) + len(test)) < 0.3


def test_tech_matrix_et_vocabulaire():
    techs = pd.Series([["spark", "python"], None, ["python"]])
    assert sal.top_technologies(techs, n=1) == ["python"]
    m = sal.tech_matrix(techs, ["python", "spark"])
    assert m.to_dict("list") == {"tech_python": [1, 0, 1], "tech_spark": [1, 0, 0]}


def test_metrics_en_dollars():
    y = np.log(np.array([100_000.0, 50_000.0]))
    m = sal.metrics(y, np.log(np.array([110_000.0, 50_000.0])))
    assert m["mae_usd"] == pytest.approx(5_000)
    assert m["erreur_relative_mediane"] == pytest.approx(0.05)
    assert sal.metrics(y, y)["r2_log"] == pytest.approx(1.0)


def test_reference_mediane_par_groupe_avec_repli():
    train = pd.DataFrame({"g": ["a", "a", "b"], "y": [1.0, 3.0, 10.0]})
    test = pd.DataFrame({"g": ["a", "b", "inconnu"]})
    assert sal.median_baseline(train, test, ["g"]).tolist() == [2.0, 10.0, 3.0]
    assert sal.median_baseline(train, test, []).tolist() == [3.0, 3.0, 3.0]


def test_features_categorie_inconnue_devient_manquante():
    train = _offers(200)
    fb = sal.FeatureBuilder(with_title=False).fit(train)
    test = train.head(3).copy()
    test["etat_code"] = "ZZ"
    X = fb.transform(test)
    assert X["etat_code"].isna().all()
    assert {"tech_spark", "tech_python", "experience_min_annees"} <= set(X.columns)


def test_lgbm_bat_la_mediane_et_shap_retrouve_la_prime_spark():
    data = sal.training_set(_offers())
    train, test = sal.split_by_company(data)
    config = {
        "with_title": False,
        "params": {"n_estimators": 200, "min_child_samples": 10},
    }
    builder, model = sal.fit_lgbm(train, config)
    X_test = builder.transform(test)
    lgbm = sal.metrics(test["y"].to_numpy(), model.predict(X_test))
    naive = sal.metrics(test["y"].to_numpy(), sal.median_baseline(train, test, []))
    assert lgbm["mae_usd"] < naive["mae_usd"] / 3

    effects = sal.tech_effects(model, X_test, train).set_index("technologie")
    # +20 % simulé ; SHAP l'exprime par rapport à l'offre moyenne (40 % citent spark),
    # donc ~ +20 % x 0,6 ~ +11,6 % en log
    assert 8 < effects.loc["spark", "effet_pct"] < 16
    assert abs(effects.loc["python", "effet_pct"]) < abs(
        effects.loc["spark", "effet_pct"]
    )


# --- Classification ------------------------------------------------------------------


def _pool() -> pd.DataFrame:
    rows = []
    for fam in clf.FAMILIES:
        for i in range(100):
            rows.append(("kaggle_linkedin", f"{fam}-{i}", fam))
    rows += [("ats_maroc", f"ma-{i}", "DEV") for i in range(37)]
    return pd.DataFrame(
        {
            "offre_id": [r[1] for r in rows],
            "source": [r[0] for r in rows],
            "titre_brut": [f"titre {r[1]}" for r in rows],
            "entreprise": "Ent",
            "description": "  texte \n sur   plusieurs lignes  ",
            "famille_poste": [r[2] for r in rows],
        }
    )


def test_echantillon_stratifie_melange_et_sans_prediction():
    s = clf.stratified_sample(_pool())
    assert len(s) == 37 + 5 * clf.PAR_FAMILLE_TECH + clf.N_AUTRE
    assert (s["source"] == "ats_maroc").sum() == 37
    assert "famille_poste" not in s.columns  # l'annotateur ne voit pas les règles
    assert s["num"].tolist() == list(range(1, len(s) + 1))
    assert (s["famille_reelle"] == "").all()
    assert s["description"].iloc[0] == " texte sur plusieurs lignes "
    assert not s["source"].head(37).eq("ats_maroc").all()  # mélangé
    assert s["offre_id"].tolist() == clf.stratified_sample(_pool())["offre_id"].tolist()


def test_tirage_sur_colonnes_legeres_puis_texte_des_seules_retenues():
    pool = _pool()
    light = pool[["offre_id", "source", "famille_poste"]]  # sans texte
    ids = clf.choose_ids(light)
    assert ids == clf.stratified_sample(pool)["offre_id"].tolist()
    details = pool[pool["offre_id"].isin(ids)].sample(frac=1, random_state=1)
    out = clf.format_sample(details, ids)
    assert out["offre_id"].tolist() == ids  # ordre du tirage, pas celui de Spark


def test_annotations_partielles_et_codes_invalides(tmp_path):
    path = tmp_path / "familles.csv"
    pd.DataFrame(
        {
            "num": ["1", "2", "3"],
            "titre_brut": ["Data Engineer", "Chef", "Dev"],
            "description": ["etl", "cuisine", "java"],
            "famille_reelle": [" data_eng ", "", "AUTRE"],
        }
    ).to_csv(path, index=False)
    df = clf.load_annotations(path)
    assert df["famille_reelle"].tolist() == ["DATA_ENG", "AUTRE"]

    pd.DataFrame(
        {
            "num": ["7"],
            "titre_brut": ["x"],
            "description": ["y"],
            "famille_reelle": ["DATA"],
        }
    ).to_csv(path, index=False)
    with pytest.raises(ValueError, match="num"):
        clf.load_annotations(path)


def test_evaluation_regles_et_tfidf():
    titles = {
        "DATA_ENG": "Data Engineer",
        "DEV": "Software Engineer",
        "BI": "BI Developer",
        "AUTRE": "Nurse",
    }
    rows = [
        (fam, t, f"{t.lower()} mission {fam.lower()} {i}")
        for fam, t in titles.items()
        for i in range(12)
    ]
    df = pd.DataFrame(rows, columns=["famille_reelle", "titre_brut", "description"])
    df["texte"] = df["titre_brut"] + " \n " + df["description"]
    rules = clf.evaluate_rules(df)
    assert set(rules) == {"f1_macro", "exactitude", "rapport", "pred"}
    assert len(rules["pred"]) == len(df)
    assert 0 <= rules["f1_macro"] <= 1
    assert clf.evaluate_tfidf(df)["f1_macro"] > 0.95


def test_sous_ensembles_ambigus_et_maroc():
    df = pd.DataFrame(
        {
            "source": ["ats_maroc"] * 12 + ["kaggle_linkedin"] * 20,
            "incertain": ["oui"] * 3 + [""] * 29,
        }
    )
    masks = clf.subsets(df)
    assert int(masks["hors_ambigus"].sum()) == 29
    assert int(masks["maroc"].sum()) == 12
    # moins de 10 offres : sous-ensemble ignoré (score sans signification)
    assert "maroc" not in clf.subsets(df.iloc[3:12].reset_index(drop=True))
