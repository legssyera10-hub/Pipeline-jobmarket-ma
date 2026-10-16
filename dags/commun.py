"""Briques communes aux DAGs : tâche = conteneur jobmarket-app, alertes Telegram.

Chaque tâche lance un conteneur de l'image jobmarket-app:dev (la même que les tests),
sur le réseau du projet, avec le code monté depuis la machine hôte. Airflow ne fait
qu'ordonnancer : aucune dépendance du pipeline (Spark, GX...) n'est installée dans
l'image Airflow, donc aucun conflit de versions possible.
"""

from __future__ import annotations

import os
from datetime import timedelta

import re

import requests
from airflow.providers.docker.exceptions import DockerContainerFailedException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk.exceptions import AirflowFailException
from docker.types import Mount

IMAGE = "jobmarket-app:dev"
NETWORK = "jobmarket_default"
UI = os.getenv("AIRFLOW__API__BASE_URL", "http://localhost:8080")

# Variables transmises aux conteneurs de jobs. Passées en `private_environment` :
# jamais affichées dans l'interface (contrairement à `environment`, qui est rendu).
FORWARDED = [
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET", "POSTGRES_USER",
    "POSTGRES_PASSWORD", "POSTGRES_DB", "SPARK_MASTER", "SPARK_DRIVER_MEMORY",
    "COLLECTE_CONTACT", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
]  # fmt: skip


def job_environment() -> dict[str, str]:
    env = {k: os.environ[k] for k in FORWARDED if os.environ.get(k)}
    # Dans le réseau Docker, les services se joignent par leur nom
    env.update(MINIO_ENDPOINT="http://minio:9000", POSTGRES_HOST="postgres",
               POSTGRES_PORT="5432")  # fmt: skip
    return env


# Codes de sortie des jobs (voir src/utils/cli.py) :
#   0 succès | 1 erreur passagère (MinIO, réseau...) -> réessayée
#   2 erreur d'usage | 3 données invalides (format d'API, qualité) -> PAS réessayée :
#   rejouer un échec déterministe 3 fois ne fait que retarder l'alerte de ~15 min.
NON_RETRYABLE_EXIT_CODES = {2, 3}


class JobOperator(DockerOperator):
    def execute(self, context):
        try:
            return super().execute(context)
        except DockerContainerFailedException as exc:
            m = re.search(r"'StatusCode': (\d+)", str(exc))
            code = int(m.group(1)) if m else None
            if code in NON_RETRYABLE_EXIT_CODES:
                failure = AirflowFailException(
                    f"échec non récupérable (code {code}) : réessayer ne changerait "
                    "rien, intervention nécessaire"
                )
                failure.logs = exc.logs  # dernières lignes reprises dans l'alerte
                raise failure from exc
            raise


def job(task_id: str, command: str, **kwargs) -> DockerOperator:
    """Une tâche du pipeline = un conteneur `python -m ...` dans /app."""
    host_dir = os.environ["JOBMARKET_HOST_DIR"]
    return JobOperator(
        task_id=task_id,
        image=IMAGE,
        command=command,  # champ templaté : {{ ds }}, {{ params.* }}
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[Mount(source=host_dir, target="/app", type="bind")],
        working_dir="/app",
        private_environment=job_environment(),
        mount_tmp_dir=False,  # Docker Desktop : pas de /tmp de l'hôte à monter
        auto_remove="force",  # le log est conservé par Airflow, le conteneur non
        # Défaut du DockerOperator : cpus=1.0. Spark en local[4] sur 1 seul coeur :
        # 184 s pour construire 37 offres (jour 7). 4 coeurs = SPARK_MASTER.
        cpus=4.0,
        **kwargs,
    )


# --- Alertes Telegram -----------------------------------------------------------------


def _send(text: str) -> None:
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        print("[alerte] Telegram non configuré : alerte non envoyée")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000]},
            timeout=20,
        ).raise_for_status()
    except requests.RequestException as exc:
        # Jamais d'URL dans le message : elle contient le jeton du bot
        print(f"[alerte] envoi Telegram impossible : {type(exc).__name__}")


def _where(context) -> str:
    ti = context["task_instance"]
    return (
        f"DAG : {ti.dag_id}\nTâche : {ti.task_id}\n"
        f"Exécution : {context.get('run_id')}\n"
        f"Journal : {UI}/dags/{ti.dag_id}/runs/{context.get('run_id')}/tasks/{ti.task_id}"
    )


def alerte_echec(context) -> None:
    """Échec DÉFINITIF d'une tâche (tous les réessais épuisés)."""
    exc = context.get("exception")
    logs = getattr(exc, "logs", None) or []  # sortie du conteneur (DockerOperator)
    tail = "\n".join(str(line).rstrip() for line in logs[-8:])
    text = (
        f"❌ JobMarket MA : échec après {context['task_instance'].try_number} "
        f"tentative(s)\n{_where(context)}\n"
        f"Erreur : {str(exc)[:300] if exc else 'inconnue'}"
    )
    if tail:
        text += f"\nDernières lignes :\n{tail[-1200:]}"
    _send(text)


def alerte_retablissement(context) -> None:
    """Succès après au moins un échec : l'incident s'est résolu tout seul."""
    tries = context["task_instance"].try_number
    if tries > 1:
        _send(f"✅ JobMarket MA : rétabli à la tentative {tries}\n{_where(context)}")


DEFAULT_ARGS = {
    "owner": "jobmarket",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,  # 2, 4, 8 min
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": alerte_echec,
    "on_success_callback": alerte_retablissement,
}
