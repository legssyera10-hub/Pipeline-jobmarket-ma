"""DAG quotidien de la source marocaine : collecte -> Bronze -> Silver -> Gold.

Deux passages par jour, heure du Maroc :
- 14:00 : collecte. Si un employeur échoue, la collecte échoue (après réessais) et la
  suite ne tourne pas : on ne fige pas un Bronze incomplet (il est immuable).
- 18:00 : rattrapage. La collecte ne rappelle QUE les employeurs manquants, puis la
  journée est traitée avec ce qui a été collecté (alerte si incomplet).
  Si 14:00 avait tout réussi, 18:00 ne fait aucun appel réseau et les étapes
  suivantes, idempotentes, réécrivent les mêmes résultats.

Idempotence : la date traitée vient de la date logique ({{ ds }}), jamais de
datetime.now() ; Bronze est immuable ; Silver remplace ses partitions ; Gold bascule
en une transaction. Relancer une exécution produit le même résultat.

Paramètres (déclenchement manuel) :
- source    : "ats_maroc" ou "ats_maroc_demo" (pannes provoquées, données isolées) ;
- collecter : false pour ne pas appeler les API (rejouer raw -> Bronze -> ...) ;
- date      : date à traiter (défaut : date logique).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, task
from airflow.timetables.trigger import CronTriggerTimetable
from commun import DEFAULT_ARGS, envoyer_information, job

TZ = "Africa/Casablanca"
DAY = "{{ params.date or ds }}"

with DAG(
    dag_id="ats_maroc_quotidien",
    description="Offres au Maroc (Greenhouse/Lever) : collecte, Bronze, Silver, Gold",
    schedule=CronTriggerTimetable("0 14,18 * * *", timezone=TZ),
    start_date=pendulum.datetime(2026, 10, 16, tz=TZ),
    catchup=False,  # une API ne rend pas le passé : pas de rattrapage historique
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),  # remplace le `sla` du plan (retiré d'Airflow 3)
    default_args=DEFAULT_ARGS,
    params={
        "source": Param("ats_maroc", enum=["ats_maroc", "ats_maroc_demo"]),
        "collecter": Param(True, type="boolean"),
        "date": Param(None, type=["null", "string"], description="AAAA-MM-JJ"),
    },
    tags=["jobmarket", "quotidien"],
    doc_md=__doc__,
) as dag:

    @task.branch
    def choisir(params=None, ds=None, run_id=None) -> str:
        """Collecte réseau seulement pour la vraie source, si demandée, et si la date
        traitée est AUJOURD'HUI : une API ne rend pas le passé.

        Cas réel (17 octobre) : le PC était éteint à 18:00 ; au redémarrage, Airflow a
        lancé le rattrapage du 16 le 17, la collecte l'a refusé (code 2) et une
        fausse alerte d'échec est partie. Désormais : information, puis parsing rejoué
        (sans effet si la journée avait été collectée à 14:00).
        """
        if not (params["collecter"] and params["source"] == "ats_maroc"):
            return "bronze"
        day = params["date"] or ds
        today = pendulum.now("UTC").to_date_string()
        if day != today:
            envoyer_information(
                f"ℹ️ JobMarket MA : exécution en retard ({run_id}).\n"
                f"La collecte du {day} n'est plus possible le {today} (une API ne "
                "rend que l'état du jour) : collecte sautée, parsing rejoué à partir "
                "des réponses brutes déjà stockées."
            )
            return "bronze"
        return "collecte"

    collecte = job(
        "collecte",
        "python -m src.ingestion.ats.collecte --date " + DAY
        + "{% if logical_date.in_timezone('" + TZ + "').hour >= 18 %}"
        + " --rattrapage{% endif %}",
        execution_timeout=timedelta(minutes=15),
    )  # fmt: skip
    bronze = job(
        "bronze",
        "python -m src.ingestion.run --source {{ params.source }} --date " + DAY,
        execution_timeout=timedelta(minutes=10),
        # Lancé si la collecte a réussi OU a été volontairement sautée
        trigger_rule="none_failed_min_one_success",
    )
    silver = job(
        "silver",
        "python -m src.transform.silver --source {{ params.source }} --date " + DAY,
        execution_timeout=timedelta(minutes=30),
        pool="spark",
    )
    gold = job(
        "gold",
        "python -m src.transform.gold --source {{ params.source }}",
        execution_timeout=timedelta(minutes=20),
        pool="spark",
    )

    choisir() >> [collecte, bronze]
    collecte >> bronze >> silver >> gold
