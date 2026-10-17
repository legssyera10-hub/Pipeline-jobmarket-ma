"""DAG manuel du modèle de salaire : entraînement, comparaison MLflow, SHAP, imputation.

Pas de planification : le modèle s'entraîne sur le dataset Kaggle, figé. À relancer
après un retraitement Silver (DAG kaggle_manuel) ou une évolution des variables.
Résultats : interface MLflow (http://localhost:5000) et gold.prime_salariale_tech.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from commun import DEFAULT_ARGS, job

with DAG(
    dag_id="ml_manuel",
    description="Modèle de salaire (LightGBM + SHAP), suivi MLflow",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 17, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["jobmarket", "manuel", "ml"],
    doc_md=__doc__,
) as dag:
    job(
        "salaire",
        "python -m src.ml.salaire --source kaggle_linkedin",
        execution_timeout=timedelta(hours=1),
        pool="spark",  # lit Silver avec Spark
    )
