"""DAG manuel du dataset Kaggle (source de développement) : Bronze -> Silver -> Gold.

Pas de planification : le dataset est figé, le rejouer chaque jour n'aurait pas de
sens. Sert au retraitement complet après une évolution du code (ex. nouvelles règles
de familles de poste). Un traitement complet prend 15 à 20 minutes.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param
from commun import DEFAULT_ARGS, job

with DAG(
    dag_id="kaggle_manuel",
    description="Dataset Kaggle LinkedIn (développement) : retraitement à la demande",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 16, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    params={
        "debut": Param("2023-12-05", type="string", description="AAAA-MM-JJ"),
        "fin": Param("2024-04-20", type="string", description="AAAA-MM-JJ"),
    },
    tags=["jobmarket", "manuel"],
    doc_md=__doc__,
) as dag:
    RANGE = "--start {{ params.debut }} --end {{ params.fin }}"
    bronze = job(
        "bronze", "python -m src.ingestion.run --source kaggle_linkedin " + RANGE
    )
    silver = job(
        "silver",
        "python -m src.transform.silver --source kaggle_linkedin " + RANGE,
        execution_timeout=timedelta(hours=1),
        pool="spark",
    )
    gold = job(
        "gold",
        "python -m src.transform.gold --source kaggle_linkedin",
        execution_timeout=timedelta(minutes=30),
        pool="spark",
    )
    bronze >> silver >> gold
