# JobMarket MA — pipeline data & ML sur le marché de l'emploi tech marocain

Pipeline en architecture médaillon (Bronze → Silver → Gold) qui collecte des offres
d'emploi tech, les nettoie avec PySpark, contrôle leur qualité et publie des agrégats
analytiques dans PostgreSQL.

> 🚧 En construction — jour 6/10 : deux sources (dataset Kaggle de développement +
> offres réelles au Maroc via les API publiques de recrutement), Bronze, Silver,
> qualité des données et Gold. À venir : Airflow, ML.

## Stack

Python 3.11 · PySpark 3.5 · Great Expectations 1.24 · MinIO (stockage objet S3) ·
PostgreSQL 16 · Docker Compose · pytest · ruff · black — à venir : Airflow, MLflow

## Architecture

```
API Greenhouse / Lever ──► raw/ (réponses JSON brutes, immuables, jour même)
                            │  parsing (rejouable sans réseau)
dataset Kaggle ─────────────┤
                            ▼
                         Bronze (MinIO, Parquet brut, immuable, partitionné source=/date=)
             │  PySpark : normalisation, enrichissement, republications
             │  règles ligne à ligne ───────────────► silver/rejets (quarantaine + motifs)
             ▼
           silver/_staging ──► audit Great Expectations ──✗──► non publié, code 1
             │ ✓ (publication)                                   (ops.controles_qualite)
             ▼
           silver/offres (Parquet typé, partitionné source=/date_collecte=)
             │  agrégations (recalcul complet)
             ▼
           gold_staging ──► transaction ──► gold.* (PostgreSQL) ──► Power BI
```

## Démarrage rapide

Prérequis : Docker Desktop. Rien d'autre à installer sur la machine : Python, Java et
Spark tournent dans le conteneur `app`.

```bash
cp .env.example .env              # puis changer les mots de passe
docker compose up -d              # MinIO + PostgreSQL + création du bucket
docker compose --profile tools build app
docker compose run --rm app python -m scripts.check_infra   # 6/6 contrôles OK attendus
docker compose run --rm --no-deps app pytest -v
```

| Service         | Adresse                         | Identifiants        |
|-----------------|---------------------------------|---------------------|
| Console MinIO   | http://localhost:9001           | voir `.env`         |
| API S3 MinIO    | http://localhost:9000           | voir `.env`         |
| PostgreSQL      | `localhost:5432`, base `jobmarket` | voir `.env`      |

## Sources

| Source | Contenu | Volume |
|---|---|---|
| `ats_maroc` | offres **au Maroc** publiées par des employeurs via les API publiques de leurs logiciels de recrutement (Greenhouse, Lever) | 37 offres, 5 employeurs (2026-10-15) |
| `kaggle_linkedin` | dataset « LinkedIn Job Postings 2023-2024 » (États-Unis), source de développement | 123 849 offres |

**Limite assumée :** `ats_maroc` est un échantillon (conseil, startups, quelques
employeurs), **pas** le marché marocain dans son ensemble. Les grands sites marocains
ont été écartés après lecture de leur robots.txt et de leurs conditions d'utilisation
(ReKrute interdit les agrégateurs, Emploi.ma est protégé par Cloudflare…). Analyse
complète : [docs/journal.md](docs/journal.md), jour 6.

### Collecte des offres marocaines

```bash
docker compose run --rm app python -m src.ingestion.ats.collecte          # aujourd'hui
docker compose run --rm app python -m src.ingestion.run --source ats_maroc --date AAAA-MM-JJ
docker compose run --rm --no-deps app python -m scripts.sonder_employeurs <slug>
```

Règles de collecte, appliquées par un client HTTP unique
([src/ingestion/ats/http.py](src/ingestion/ats/http.py)) :

- `robots.txt` lu avec Protego et respecté, `Crawl-delay` compris ;
- au moins 2 s entre deux appels au même hôte, aucun parallélisme ;
- User-Agent identifiable (`COLLECTE_CONTACT` dans `.env`) ;
- réessais espacés sur les erreurs 429 et 5xx.

Les réponses JSON sont stockées **brutes et immuables** (`raw/`). Le parsing se
rejoue sans réseau. Une API donne l'état du jour : la collecte refuse toute date
passée. Chaque jour, l'API renvoie les offres encore ouvertes : Silver ne publie une
offre qu'une fois, et chaque présence quotidienne est conservée dans
`silver/observations` (pour mesurer à terme la durée de mise en ligne).

## Ingestion Bronze (dataset Kaggle)

Dataset à placer dans `data/raw/` (non versionné : on ne republie pas les annonces).

```bash
docker compose run --rm app python -m src.ingestion.run --all              # backfill
docker compose run --rm app python -m src.ingestion.run --date 2024-04-09  # un jour
docker compose run --rm app python -m scripts.check_bronze                 # contrôles
```

`bronze/source=<source>/date=<YYYY-MM-DD>/data.parquet` : colonnes source en texte et
colonnes techniques `_ingested_at`, `_source`, `_run_date`. **Immuable** : une date
déjà ingérée est ignorée (écriture conditionnelle `If-None-Match`).

## Couche Silver (PySpark)

```bash
docker compose run --rm app python -m src.transform.silver --all [--explain]
docker compose run --rm app python -m src.transform.silver --date 2024-04-09
docker compose run --rm app python -m scripts.check_silver                 # contrôles
```

`silver/offres/source=<source>/date_collecte=<YYYY-MM-DD>/` : une ligne par offre,
typée et enrichie.

- `offre_id` : SHA-256 de la source et de l'URL sans paramètre de suivi.
- Titre, entreprise et ville normalisés ; état et région résolus par jointure
  broadcast avec `reference/`.
- Famille de poste (règles provisoires) et technologies (174 entrées dans
  `reference/technologies.yaml`).
- Années d'expérience, salaire annualisé, type de contrat, télétravail.
- `est_republication` : même entreprise, titre et ville dans les 30 jours
  précédents (fenêtre glissante, marquage sans suppression).

| Chiffres (run complet) | |
|---|---|
| Offres Bronze -> Silver | 123 849 -> 123 848 valides + 1 rejet, `offre_id` unique |
| Durée (dataset complet) | 350 s au jour 5, 908-920 s au jour 6 : régression en cours d'analyse (journal, jour 6) ; `ats_maroc` : 43 s |
| Offres tech avec au moins une technologie détectée | 90,2 % |
| Salaire annualisé concordant avec la source | 100 % (29 % des offres ont un salaire) |
| Précision de l'extraction des technologies | *à mesurer : 100 offres à annoter* |

## Qualité des données

Deux niveaux de règles ligne à ligne ([src/quality/rules.py](src/quality/rules.py)) :

- **bloquantes** (identifiant, titre ou date de publication manquants, publication
  après la collecte) : l'offre part en quarantaine dans `silver/rejets` avec **tous**
  ses motifs. Aucune ligne n'est perdue ;
- **anomalies** (salaire hors bornes ou incohérent, devise non USD, expiration avant
  publication) : l'offre reste valide, le motif est listé dans `_anomalies`.

Puis une validation du lot par Great Expectations
([src/quality/expectations.py](src/quality/expectations.py)), selon le motif
**write-audit-publish**. Si un contrôle critique échoue (doublon d'`offre_id`, valeur
hors référentiel, dérive d'un taux de remplissage, taux de rejet supérieur à 5 %),
Silver n'est **pas publié**, la version précédente reste en place et le job sort avec
le code 1. Chaque résultat est historisé dans `ops.controles_qualite`.

```bash
docker compose run --rm app python -m scripts.demo_corruption   # démonstration
```

La démonstration injecte 50 lignes corrompues et une dérive de format dans une source
isolée. Elle vérifie qu'elles sont en quarantaine avec le bon motif, que la
validation échoue et que rien n'est publié, puis elle nettoie.

| Chiffres (run complet) | |
|---|---|
| Bronze -> Silver | 123 849 = 123 848 valides + 1 rejet |
| Contrôles critiques | 18/18 réussis |
| Salaires signalés en anomalie (hors bornes) | 495 (1,4 % des salaires) |

## Couche Gold (PostgreSQL)

```bash
docker compose run --rm app python -m src.transform.gold
docker compose run --rm --no-deps app python -m scripts.check_gold
```

| Table | Contenu |
|---|---|
| `gold.tech_par_mois` | technologie x mois x périmètre (toutes / tech) : offres, annonces, part |
| `gold.offres_par_ville` | mois x ville x état x famille de poste |
| `gold.experience_demandee` | famille de poste x tranche d'expérience, part dans la famille |
| `gold.cooccurrence_tech` | paires de technologies : offres communes, Jaccard, lift |

`nb_offres` compte les offres uniques (hors republications), `nb_annonces` toutes les
annonces. Le recalcul est complet, écrit dans `gold_staging` puis basculé en une seule
transaction : un lecteur ne voit jamais d'état intermédiaire. Relancer le job produit
des tables identiques (empreintes MD5 comparées).

## Organisation du dépôt

```
src/ingestion/    collecteurs, collecte des API (ats/), écriture Bronze
src/transform/    jobs PySpark Silver / Gold
src/quality/      règles de quarantaine, suites Great Expectations
src/utils/        config (.env), SparkSession (s3a + JDBC), client S3, PostgreSQL
sql/              schéma PostgreSQL (gold, gold_staging, ops), idempotent
scripts/          scripts opérationnels (check_*, explore, purge, annotation)
dags/             DAGs Airflow
reference/        référentiels (technologies, états US, villes marocaines, employeurs)
tests/            tests unitaires (+ fixtures)
docker/           Dockerfile de l'image app, init SQL PostgreSQL
docs/             décisions d'architecture, journal
```

## Décisions et compromis

Voir [docs/decisions.md](docs/decisions.md).
