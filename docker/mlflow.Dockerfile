# Serveur de suivi MLflow : expériences dans PostgreSQL (base `mlflow`), modèles et
# graphiques dans MinIO (s3://jobmarket/mlflow). --serve-artifacts : les clients
# passent par le serveur pour les fichiers, ils n'ont pas besoin des clés MinIO.
FROM python:3.11-slim-bookworm

RUN pip install --no-cache-dir mlflow==3.17.1 psycopg2-binary==2.9.10 boto3==1.41.5

EXPOSE 5000
