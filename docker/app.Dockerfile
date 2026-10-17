# Image d'exécution des jobs : Python 3.11 + Java 17 + PySpark + connecteurs S3/JDBC.
# Raison : Spark sous Windows natif exige winutils/HADOOP_HOME et casse souvent ;
# la même image servira plus tard à Airflow et à la CI.
FROM python:3.11-slim-bookworm

# Check-Date=false : tolère une horloge machine en retard (fichier Release "pas encore
# valide"). Les signatures GPG des paquets restent vérifiées.
RUN apt-get -o Acquire::Check-Date=false update \
 && apt-get install -y --no-install-recommends default-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Jars nécessaires à Spark pour lire/écrire MinIO (s3a://) et PostgreSQL (JDBC).
# hadoop-aws DOIT avoir la même version que le Hadoop embarqué dans PySpark 3.5 (3.3.4),
# et aws-java-sdk-bundle celle dont hadoop-aws 3.3.4 dépend (1.12.262).
ARG MAVEN=https://repo1.maven.org/maven2
ENV SPARK_JARS_DIR=/opt/spark-jars
RUN mkdir -p $SPARK_JARS_DIR && cd $SPARK_JARS_DIR \
 && curl -fsSLO $MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSLO $MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSLO $MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# OpenMP, requis par LightGBM (jour 8). Couche séparée, après pip, pour ne pas
# invalider le cache de l'installation Python (~30 min à reconstruire ici).
RUN apt-get -o Acquire::Check-Date=false update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python
# Great Expectations : pas de télémétrie sortante, pas de barres de progression
ENV GX_ANALYTICS_ENABLED=false \
    TQDM_DISABLE=1
# MLflow cherche le commit git du code ; pas de git dans l'image : pas d'avertissement
ENV GIT_PYTHON_REFRESH=quiet
CMD ["python", "-m", "scripts.check_infra"]
