# Airflow 3.3.2 + provider Docker (DockerOperator), versions fixées par le fichier de
# contraintes officiel d'Airflow : jamais de conflit avec les dépendances d'Airflow.
# Les jobs ne tournent PAS ici : chaque tâche lance un conteneur jobmarket-app:dev
# (la même image que les tests), via /var/run/docker.sock.
FROM apache/airflow:3.3.2-python3.11

ARG AIRFLOW_VERSION=3.3.2
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-docker \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
