"""Vérification des DAGs, exécutée DANS l'image Airflow (l'image app n'a pas Airflow).

  docker compose run --rm --no-deps -v ./tests/airflow:/opt/airflow/verif \
      airflow-scheduler python /opt/airflow/verif/verifier_dags.py

Code de sortie 0 si toutes les vérifications passent.
"""

from __future__ import annotations

import sys

import pendulum

# Comme les processus Airflow : le dossier des DAGs est importable (module `commun`)
sys.path.insert(0, "/opt/airflow/dags")

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # emplacement historique
    from airflow.models.dagbag import DagBag

CHECKS: list[tuple[str, bool]] = []


def check(name: str, ok: bool) -> None:
    CHECKS.append((name, bool(ok)))


def render(dag, task, logical_date, params):
    env = dag.get_template_env()
    context = {
        "params": params,
        "ds": logical_date.in_timezone("UTC").to_date_string(),
        "logical_date": logical_date,
    }
    return env.from_string(task.command).render(**context)


def main() -> int:
    bag = DagBag(dag_folder="/opt/airflow/dags")  # exemples désactivés par la config
    check("aucune erreur d'import", not bag.import_errors)
    if bag.import_errors:
        print(bag.import_errors)
    check(
        "3 DAGs chargés",
        set(bag.dag_ids) == {"ats_maroc_quotidien", "kaggle_manuel", "ml_manuel"},
    )
    ml = bag.get_dag("ml_manuel")
    check("ml_manuel : non planifié", type(ml.timetable).__name__ == "NullTimetable")
    check("ml_manuel : commande du modèle de salaire",
          [x.pool for x in ml.tasks] == ["spark"]
          and ml.tasks[0].command.startswith("python -m src.ml.salaire"))  # fmt: skip

    dag = bag.get_dag("ats_maroc_quotidien")
    t = {task.task_id: task for task in dag.tasks}
    check(
        "tâches attendues",
        set(t) == {"choisir", "collecte", "bronze", "silver", "gold"},
    )
    check("choisir -> collecte et bronze",
          t["choisir"].downstream_task_ids == {"collecte", "bronze"})  # fmt: skip
    check("collecte -> bronze -> silver -> gold",
          t["collecte"].downstream_task_ids == {"bronze"}
          and t["bronze"].downstream_task_ids == {"silver"}
          and t["silver"].downstream_task_ids == {"gold"})  # fmt: skip
    check("bronze tourne si la collecte est volontairement sautée",
          t["bronze"].trigger_rule == "none_failed_min_one_success")  # fmt: skip
    tt = dag.timetable
    check("planifié à 14:00 et 18:00, heure du Maroc",
          getattr(tt, "_expression", None) == "0 14,18 * * *"
          and str(getattr(tt, "_timezone", "")) == "Africa/Casablanca")  # fmt: skip
    check("pas de rattrapage historique (catchup)", dag.catchup is False)
    check("une seule exécution à la fois", dag.max_active_runs == 1)
    jobs = [t[k] for k in ("collecte", "bronze", "silver", "gold")]
    check(
        "3 réessais espacés exponentiellement",
        all(j.retries == 3 and j.retry_exponential_backoff for j in jobs),
    )
    check(
        "codes 2 et 3 (erreurs déterministes) jamais réessayés",
        all(type(j).__name__ == "JobOperator" for j in jobs)
        and __import__("commun").NON_RETRYABLE_EXIT_CODES == {2, 3},
    )
    check("alerte sur échec définitif", all(j.on_failure_callback for j in jobs))
    check("Spark : pool à 1 place (silver, gold)",
          t["silver"].pool == "spark" and t["gold"].pool == "spark")  # fmt: skip
    check("conteneurs à 4 coeurs", all(j.cpus == 4.0 for j in jobs))
    check("secrets en environnement privé (non affiché)",
          all("MINIO_ROOT_PASSWORD" not in (j.environment or {}) for j in jobs)
          and all(j._private_environment.get("MINIO_ENDPOINT") == "http://minio:9000"
                  for j in jobs))  # fmt: skip

    params = {"source": "ats_maroc", "collecter": True, "date": None}
    tz = "Africa/Casablanca"
    at14 = render(
        dag, t["collecte"], pendulum.datetime(2026, 10, 16, 14, tz=tz), params
    )
    at18 = render(
        dag, t["collecte"], pendulum.datetime(2026, 10, 16, 18, tz=tz), params
    )
    check(
        "14:00 : collecte normale",
        "--rattrapage" not in at14 and "--date 2026-10-16" in at14,
    )
    check("18:00 : collecte de rattrapage", at18.rstrip().endswith("--rattrapage"))
    demo = render(
        dag,
        t["silver"],
        pendulum.datetime(2026, 10, 16, 14, tz=tz),
        {"source": "ats_maroc_demo", "collecter": False, "date": "2026-10-15"},
    )
    check(
        "paramètres source/date respectés",
        demo
        == "python -m src.transform.silver --source ats_maroc_demo --date 2026-10-15",
    )

    # Branchement : collecte seulement pour la date du jour (rattrapage en retard)
    import commun

    sent: list[str] = []
    commun._send = sent.append  # pas de vrai message Telegram pendant la vérification
    choisir = t["choisir"].python_callable
    today = pendulum.now("UTC").to_date_string()
    late = choisir(params=params, ds="2026-10-16", run_id="scheduled__x")
    check("run en retard : collecte sautée, information envoyée",
          late == "bronze" and len(sent) == 1 and "en retard" in sent[0])  # fmt: skip
    check(
        "run du jour : collecte",
        choisir(params=params, ds=today, run_id="r") == "collecte",
    )
    demo = {"source": "ats_maroc_demo", "collecter": True, "date": None}
    branch = choisir(params=demo, ds=today, run_id="r")
    check("source de démonstration : jamais de collecte",
          branch == "bronze" and len(sent) == 1)  # fmt: skip

    kaggle = bag.get_dag("kaggle_manuel")
    check(
        "kaggle_manuel non planifié", type(kaggle.timetable).__name__ == "NullTimetable"
    )

    for name, ok in CHECKS:
        print(f"[{'OK' if ok else 'KO'}] {name}")
    failed = [n for n, ok in CHECKS if not ok]
    print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} vérifications OK")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
