# Journal de bord

Les difficultés rencontrées, au fil de l'eau. Matière première pour la question
d'entretien « quelle a été votre plus grosse difficulté ? ».

## Jour 1 — 2026-10-03

- **Images MinIO officielles indisponibles.** `docker pull minio/minio` → « repository
  does not exist », quay.io → « unauthorized ». Passage au fork `pgsty/minio` (voir D2).
- **`apt-get update` refusé dans le build Docker** : « Release file is not valid yet
  (invalid for another 25min) ». L'horloge du PC est en retard d'environ 26 minutes.
  Contournement : `-o Acquire::Check-Date=false` (les signatures GPG restent vérifiées).
  Vraie correction : resynchroniser l'heure Windows.
- **Timeout JDBC au premier lancement** : Spark → PostgreSQL bloqué pendant la
  négociation SSL lors d'un démarrage à froid ; OK aux lancements suivants. Correctif :
  `sslmode=disable` (réseau Docker local, PostgreSQL sans TLS) et `connectTimeout=30`.
  Revalidé sur 3 exécutions consécutives.
- **Test qui ne testait pas ce qu'il croyait** : `load_dotenv()` cherche le `.env` à
  partir du dossier du *fichier appelant*, pas du dossier courant. Le test
  « secret manquant » lisait donc le vrai `.env` et ne levait rien. Correctif :
  `find_dotenv(usecwd=True)`.

**Résultat :** `check_infra` 6/6, `pytest` 7/7, ruff et black propres.

## Jour 2 — 2026-10-04

### Exploration de `data/raw/postings.csv`

Dataset « LinkedIn Job Postings 2023-2024 » : 493 Mo, **123 849 offres**, 31 colonnes,
offres **américaines** en anglais. Exploration sur les 50 000 premières lignes
(`python -m scripts.explore_postings 50000`), jamais le fichier entier.

**Colonnes et remplissage (échantillon de 50 000 lignes)**

| Groupe | Colonnes (remplissage) |
|---|---|
| Identifiants | `job_id` (100 %, unique), `company_id` (98 %), `company_name` (98 %) |
| Contenu | `title` (100 %), `description` (100 %, 3 300 caractères en médiane, max 21 000, retours à la ligne entre guillemets), `skills_desc` (2 %) |
| Lieu | `location` (100 %, « Ville, ÉTAT » ou « United States »), `zip_code` (83 %), `fips` (78 %) |
| Contrat | `formatted_work_type` / `work_type` (100 %, Full-time 80 %), `formatted_experience_level` (72 %), `remote_allowed` (13 %, uniquement la valeur 1 : l'absence ne veut pas dire « non ») |
| Dates (epoch ms) | `original_listed_time`, `listed_time`, `expiry` (100 %), `closed_time` (2 %) |
| Salaire | `min_salary`/`max_salary` (24 %), `med_salary` (5 %), `pay_period`/`currency`/`compensation_type`/`normalized_salary` (29 %) |
| Candidature | `application_type` (100 %), `application_url` (68 %), `posting_domain` (65 %), `applies` (23 %), `views` (99 %) |
| Inutile | `sponsored` (toujours 0), `compensation_type` (toujours BASE_SALARY) |

**Types** : en laissant pandas deviner, `zip_code` et `fips` deviennent des `float64`
→ **les zéros initiaux sont perdus** (`08540` → `8540.0`, 3 523 cas sur 50 000).
Les dates et salaires arrivent aussi en `float64`. Conclusion : en Bronze, tout est lu
en texte.

**Date de publication** : epoch en millisecondes, stocké en texte sous forme de
flottant (`"1713397508000.0"`), en UTC.
- `original_listed_time` = première mise en ligne. Elle va du 2023-12-05 au
  2024-04-20, sur **61 jours distincts**, avec une longue traîne (1 ou 2 offres par
  jour en décembre et janvier).
- `listed_time` = dernière republication, concentrée du 2024-04-05 au 2024-04-20.
  Elle diffère de `original_listed_time` dans 57 % des cas, mais l'écart médian est de
  0 jour (90e centile : 1 jour, 99e centile : 25 jours).
- **Distribution très déséquilibrée** : les 18 et 19 avril regroupent 61 000 offres
  (49 %), alors que 25 partitions sur 61 ont moins de 10 lignes et 41 moins de 100
  (médiane : 31 lignes). C'est le problème des « petits fichiers » : à
  surveiller quand Spark lira Bronze.

**Structure des salaires** (29 % des offres en ont un) :
- trois formes exclusives : fourchette `min`+`max` (24 %), valeur unique `med` (5 %),
  rien (71 %). Jamais `min > max`.
- `pay_period` : YEARLY 58 %, HOURLY 40 %, MONTHLY 1,4 %, WEEKLY, BIWEEKLY. Les
  montants sont dans l'unité de la période (25 $/heure contre 85 000 $/an) ;
  `normalized_salary` les annualise.
- `currency` : USD à 99,9 %, quelques CAD/EUR/AUD/BBD.
- Les tables annexes `jobs/salaries.csv`, `jobs/job_skills.csv` (compétences codées,
  libellés dans `mappings/skills.csv`), `jobs/benefits.csv`,
  `jobs/job_industries.csv` et `companies/*` se joignent sur `job_id` / `company_id`.
  **Non ingérées pour l'instant.**

**Conséquences pour la suite**
- Les regex d'expérience du plan sont en français : il faudra les motifs anglais
  (« 3+ years ») pour ce dataset.
- Le référentiel des villes marocaines ne s'applique pas : prévoir un référentiel
  par source.
- Les salaires sont en USD par période, pas en MAD mensuel : la normalisation se fera
  en Silver.

### Ingestion Bronze

- **Résultat** : 123 849 lignes → 61 fichiers Parquet
  (`bronze/source=kaggle_linkedin/date=YYYY-MM-DD/data.parquet`) en 54 à 63 s.
  Un 2e passage écrit 0 ligne (61 « skipped »). `check_bronze` : tous les contrôles OK.
- **Bug : des colonnes entièrement vides tapées « null ».** Dans les petites
  partitions (1 à 10 offres), une colonne vide partout, par exemple `zip_code`, était
  inférée par pyarrow en type `null`, écrit `INT32` dans le Parquet. Spark refusait
  alors de lire la source : « Column: [zip_code], Expected: string, Found: INT32 ».
  51 fichiers sur 61 étaient touchés. Le test unitaire avec un faux S3 ne pouvait pas
  le voir : **c'est la relecture par Spark de toute la source qui l'a révélé.**
  Correctif : schéma Parquet imposé (`bronze_schema`), plus un test de régression.
- **L'immuabilité de Bronze a eu une conséquence concrète.** Relancer l'ingestion
  corrigée ne remplaçait rien (« skipped ») : c'est voulu. Il a fallu une
  **purge explicite** (`scripts/purge_bronze.py`, qui exige de retaper le nom de la
  source) puis une réingestion. Leçon : valider le schéma avant d'écrire dans une
  couche immuable.
- **pyarrow ignore les dossiers qui commencent par `_`** (fichiers cachés) : un
  partitionnement en `_pub_date=...` était invisible à la lecture. Renommé en
  `pub_date`.
- **Les timestamps en nanosecondes** (défaut de pandas) ne sont pas lisibles par
  Spark 3.5 : `_ingested_at` est écrit en microsecondes.
- **Vérification qu'If-None-Match est réellement appliqué** par le fork MinIO : une
  2e écriture de la même clé est bien refusée (HTTP 412) et le contenu d'origine est
  conservé. S'il avait été ignoré en silence, l'immuabilité n'aurait été qu'une
  illusion.
- **À surveiller** : des `NullPointerException` du heartbeat Spark
  (`BlockManagerId ... is null`) en mode local, sans effet sur les résultats.
  Hypothèse : de la pression mémoire (4 Go alloués à Docker). À revoir au jour 3,
  quand Spark fera de vrais traitements.

**Résultat :** Bronze complet et immuable, `pytest` 22/22, ruff et black propres.

## Jours 3-4 — 2026-10-12 — Couche Silver (PySpark)

### Ce qui a été construit

- `src/transform/text.py` : normalisation des titres, entreprises, URLs, lieux ;
  famille de poste par règles (provisoire, remplacée par un classifieur au jour 8).
- `src/transform/experience.py` : années d'expérience demandées, en anglais et en
  français.
- `src/transform/technologies.py` + `reference/technologies.yaml` : 174 technologies.
- `src/transform/silver.py` : le job Spark. Une UDF pandas (Arrow) pour tout le
  parsing Python, 2 jointures broadcast (états, zones métropolitaines),
  dédoublonnage par `offre_id`, marquage des republications sur une fenêtre glissante
  de 30 jours, écriture `silver/offres/source=.../date_collecte=.../`.
- `scripts/check_silver.py` (contrôles et diagnostics) et
  `scripts/annotation_technologies.py` (mesure de précision).
- **154 tests** au total dans le projet.

### Résultats (run complet, 61 partitions)

| Indicateur | Valeur |
|---|---|
| Lignes Bronze -> Silver | 123 849 -> 123 849 (aucune perte, `offre_id` unique) |
| Durée du job complet | 352 s (`local[4]`, 1,5 Go de heap) ; une journée : ~90 s |
| Offres tech (famille != AUTRE) | 5,9 % (7 366 offres : DEV 4 826, DATA_ENG 882, BI 774, AI_ML 512, DATA_SCI 372) |
| Offres avec au moins 1 technologie | 29,3 % (toutes) ; **90,2 % des offres tech** |
| Technologies par offre | 0,5 (AUTRE) ; 3,7 à 6,9 (familles tech) |
| Expérience extraite | 54,2 % des offres |
| Salaire annualisé | 29,1 % ; **100 % concordant** avec `normalized_salary` du dataset (tolérance 1 %) |
| Ville / état résolus | 93,4 % / 89,8 % |
| Republications (fenêtre de 30 j) | 7,4 % — dont 56 % le même jour (voir plus bas) |
| Top technologies | excel, sql, python, aws, salesforce, azure, sap, java, oracle, javascript |

**Cohérence de l'expérience extraite avec le niveau déclaré par LinkedIn** (médiane
du minimum) : stage et débutant 2 ans, confirmé 3 ans, directeur et dirigeant 5 ans.
L'ordre est respecté.

**Idempotence vérifiée sur MinIO** : relancer `--date 2024-04-09` remplace
**uniquement** cette partition. Les 60 autres fichiers sont identiques (même ETag,
même date de modification), et le nombre de fichiers reste de 61.

### Difficultés

- **Le job complet a saturé la VM Docker.** Plus de 10 minutes de traitement, des
  heartbeats Spark expirés, et même `docker stats` ne répondait plus. Cause :
  `local[*]` lance 8 workers Python, chacun avec un lot Arrow de 10 000 descriptions
  (~150 Mo en pandas), en plus de 2 Go de heap JVM, de MinIO et de PostgreSQL, le
  tout dans 4 Go : la VM swappait. Diagnostic en deux temps :
  1. le plan physique montre **un seul** `ArrowEvalPython`, donc l'UDF n'est pas
     réévaluée pour chaque champ de la structure (piège classique, ici évité) ;
  2. un benchmark en Python pur donne 4 ms par offre, soit ~8 min CPU au total : le
     calcul n'explique pas le blocage, la mémoire si.

  Correctif : `local[4]`, `spark.sql.execution.arrow.maxRecordsPerBatch=2000` et une
  heap de 1,5 Go. Résultat : 352 s, aucun avertissement de heartbeat. Les
  `NullPointerException` de heartbeat vues au jour 2 avaient la même cause.
- **Un heredoc shell a corrompu une regex.** Lors d'un correctif par script,
  `\b` est devenu le caractère de contrôle `\x08` (retour arrière) dans `text.py`. La
  regex ne matchait plus rien, sans erreur. C'est un test (`S.A.` -> `sa`) qui l'a
  détecté. Leçon : ne pas éditer du code contenant des antislashs à travers un
  heredoc.
- **Des fixtures de test sans schéma** : une colonne vide partout empêche Spark
  d'inférer le type (`CANNOT_DETERMINE_TYPE`). C'est le même piège que le bug Bronze
  du jour 2, rencontré cette fois côté tests. Schémas explicites partout.

### Ce que les données ont appris (pièges intégrés aux règles et aux tests)

- Les URLs portent toutes un paramètre de suivi (`?trk=...`) : il est retiré avant le
  hash, sinon la même offre aurait plusieurs `offre_id`.
- Le HTML des descriptions a été retiré sans espace : `science1+ years`,
  `PythonLinux`, `experienceSQL`. D'où `(?<!\d)` au lieu de `\b` pour l'expérience,
  et la tolérance minuscule->Majuscule pour les technologies.
- Faux positifs d'expérience : `at least 18 years of age`,
  `40+ years of experience` (l'entreprise, pas le poste). D'où l'exigence du mot
  « experience » à proximité, l'exclusion de « of age / old » et le plafond à 20 ans.
- Homonymes des technologies dans les offres non tech : `airflow` (HVAC), `rust`
  (maintenance), `torch` (soudure), `rails` (ferroviaire), `flask` (laboratoire),
  `DBT` (thérapie), `workday`, `excel in`. Soit variante sensible à la casse, soit
  exclusion, soit retrait.
- **Republications le même jour = annonces multi-postes** : « Teacher Science » ×25
  le même jour à Houston, « Line cook » ×29. La clé (entreprise, titre, ville) ne
  distingue pas deux postes identiques ouverts en même temps. 5 185 des 9 165
  republications sont de ce type. **À trancher pour Gold** : compter des
  « annonces » ou des « postes ».

### Reste à faire (à toi)

**Annoter les 100 offres** de `data/annotation/technologies.csv` (colonne
`technologies_reelles`), puis lancer
`python -m scripts.annotation_technologies score`. Tant que ce n'est pas fait, aucun
chiffre de précision ne doit apparaître dans le README ni sur le CV.

## Jour 5 — 2026-10-13 — Qualité des données et couche Gold

### Ce qui a été construit

- `src/quality/rules.py` : règles ligne à ligne. 5 règles **bloquantes** (envoi en
  quarantaine dans `silver/rejets`, avec tous les motifs) et 5 **anomalies**
  (signalées dans `_anomalies`, l'offre reste valide).
- `src/quality/expectations.py` : Great Expectations 1.24.0. Une suite **critique**
  de 17 contrôles plus le taux de rejet (un échec bloque la publication), et une
  suite **avertissement** de 4 contrôles. Les résultats sont historisés dans
  `ops.controles_qualite`.
- Job Silver en **write-audit-publish** : transit -> audit GX -> publication par
  copie côté serveur. Code de sortie 1 si la suite critique échoue.
- `src/transform/gold.py` + `sql/schema.sql` : 4 tables Gold dans PostgreSQL, écrites
  dans `gold_staging` puis basculées en une transaction.
- `scripts/demo_corruption.py` (critère de fin du jour), `scripts/check_gold.py`.
- **189 tests** (+35) : quarantaine, suites GX, calculs Gold, titres non latins.

### Critère de fin atteint : la démonstration de corruption

200 vraies offres sont copiées vers une source isolée `demo_corruption`, puis
corrompues :

| Corruption | Lignes | Résultat |
|---|---|---|
| titre vide | 20 | quarantaine `titre_manquant` |
| publication après la collecte | 20 | quarantaine `publication_apres_collecte` |
| date de publication vide | 10 | quarantaine `date_publication_manquante` |
| salaire min > max | 10 | **valide**, anomalie `salaire_min_superieur_max` |
| lieu = « United States » | 100 | valide ligne à ligne, mais **dérive** : 102 villes vides sur 150 |

La suite critique échoue sur 3 contrôles : taux de rejet (25 %, seuil 5 %), villes
vides, états vides. **Rien n'est publié dans `silver/offres`.** Les 50 rejets sont
en quarantaine avec le bon motif. Les données de la démo sont ensuite nettoyées ; le
résultat reste dans `ops.controles_qualite`.

### Résultats sur les vraies données

| Indicateur | Valeur |
|---|---|
| Bronze -> Silver | 123 849 = **123 848 valides + 1 rejet** (titre « ![ ») |
| Suite critique | 18/18 OK -> publié |
| Avertissements | 4/4 OK (495 salaires hors bornes, 1,4 % des salaires : sous le seuil de 2 %) |
| Durée Silver | 344-350 s (inchangée malgré l'audit) |
| Gold | 4 tables, 19 777 lignes, publié en 28-29 s |
| Idempotence Gold | empreintes MD5 des 4 tables **identiques** après relance |

**Ce que Gold montre (offres uniques)** :
- offres tech d'avril 2024 : SQL 39 %, Python 37 %, AWS 24 %, Java 24 %, Azure 20 % ;
- paires les plus associées (indice de Jaccard) : pytorch-tensorflow 0,65,
  numpy-pandas 0,58, html-css 0,52, docker-kubernetes 0,50, grafana-prometheus 0,46.
  Ce sont des associations attendues, donc une validation qualitative de
  l'extraction ;
- expérience : 18 % des offres DATA_ENG demandent 6 ans ou plus, contre 9 % en BI ;
  35 à 41 % des offres ne précisent rien ;
- villes data/IA : New York 125, San Francisco 64, Austin 58, Atlanta 49.

### Difficultés

1. **Les salaires : le « 100 % concordant » du jour 3 ne prouvait pas la justesse.**
   En calibrant les bornes, j'ai trouvé 442 salaires « YEARLY » de moins de
   10 000 $ (des taux horaires mal étiquetés) et 49 de plus de 1 M$ (par exemple un
   stage infirmier à 41 M$/an : un montant annuel saisi comme horaire puis
   multiplié par 2 080). Le `normalized_salary` du dataset a les mêmes erreurs : la
   concordance prouvait que le calcul était cohérent, pas que les montants étaient
   justes. Ces salaires sont désormais signalés en anomalie (D19).
2. **`OutOfMemoryError: Java heap space`** au premier run complet. Deux DataFrames
   en cache (124 000 lignes avec descriptions, puis leur version filtrée) dans
   1,5 Go de heap ; l'erreur est survenue pendant la construction du cache en
   colonnes. Plutôt que d'augmenter la mémoire : passage au **write-audit-publish**
   (GX relit le Parquet de transit, colonne par colonne), un seul cache sur disque,
   et des lots de cache en colonnes de 2 000 lignes. Même durée qu'avant.
3. **`F.col()` au niveau du module** : en PySpark 3.5, construire une expression
   Spark exige une SparkContext active. Des règles déclarées comme constantes
   auraient fait planter l'`import`. Repéré à la relecture, avant exécution : les
   règles sont des fonctions.
4. **La quarantaine a trouvé un bug du jour 3.** Sur 7 rejets, 6 étaient des titres
   légitimes en chinois, hébreu, grec ou coréen, que `normalize_title` (ne gardant
   que `[a-z0-9]`) vidait entièrement. Avec la source marocaine, **tous les titres
   en arabe** auraient été rejetés. Corrigé (D27), et le rerun a vérifié au passage
   que les partitions de rejets devenues vides (9, 12 et 15 avril) sont bien
   supprimées (D23).
5. **Collation Spark / PostgreSQL** : la contrainte `CHECK (tech_a < tech_b)` a
   refusé (`t_sql`, `terraform`), car PostgreSQL trie selon des règles
   linguistiques, Spark par octets. **La transaction a tout annulé** : les tables de
   transit étaient remplies, les 4 tables `gold.*` restaient à 0 ligne. C'est la
   preuve concrète de l'atomicité de la publication Gold. Corrigé avec
   `COLLATE "C"` (D26), plus une migration idempotente.
6. Deux erreurs de ma part, corrigées : des probabilités entières passées à
   `approxQuantile` (Spark exige des flottants), et une ligne absurde
   (`sum(1 for _ in []) or ...`) repérée en relisant avant l'insertion.

### Points ouverts

- Les seuils de remplissage (D22) ont été calibrés sur le dataset américain : à
  recalibrer pour la source marocaine.
- La publication Silver se fait partition par partition : une partition est absente
  pendant une fraction de seconde (acceptable car Gold s'exécute après Silver).
- pytest affiche 11 avertissements (dépréciations de bibliothèques) que je n'ai pas
  examinés.

## Jour 6 — 2026-10-15 — Source marocaine réelle : API publiques des ATS

### Analyse des sources (avant de coder)

| Source | Constat vérifié | Décision |
|---|---|---|
| ReKrute | robots.txt : `Jooblebot` et `JobBot` (agrégateurs) -> `Disallow: /` ; même les robots autorisés sont exclus de `/offres.html` | exclu sans autorisation écrite |
| Emploi.ma | page Cloudflare « Just a moment… » (403) | exclu, on ne contourne pas |
| MarocAnnonces | `User-agent: *` -> `Disallow: /` | exclu |
| Indeed, LinkedIn | conditions d'utilisation | exclus |
| Bayt, Dreamjob | conditions d'utilisation illisibles (403 aux accès automatisés) | non retenus |
| API Jooble, Careerjet | conçues pour l'affichage, IP du visiteur exigée | non retenues |
| data.gov.ma | 4 536 concours publics 2012-2025 (ODbL), sans spécialité ni description | complément possible, pas la source principale |
| API SmartRecruiters | `User-agent: * / Disallow: /` sur l'hôte de l'API | **exclu** (51 offres Devoteam perdues) |
| **API Greenhouse, Lever** | robots.txt : autorisé (Lever : `Crawl-delay: 1`) | **retenues** |

Découverte des employeurs : 62 identifiants sondés à l'aveugle donnent 1 seul
résultat (Artefact). Les recherches ciblées sur les pages d'offres indexées en
donnent 4 de plus. **5 employeurs, 37 offres au Maroc** : Artefact (10), Yassir
(10), Theodo (8), Nuitée (7), Welo Global (2, à distance).

### Ce qui a été construit

- `src/ingestion/ats/http.py` : client HTTP poli (robots.txt via Protego,
  `Crawl-delay`, au moins 2 s entre deux appels, réessais avec `Retry-After`).
- `src/ingestion/ats/parsers.py` : Greenhouse et Lever -> lignes Bronze aux
  **colonnes fixes**, offres au Maroc seulement.
- `src/ingestion/ats/collecte.py` : réponses JSON brutes vers
  `raw/source=ats_maroc/date=D/`, immuables, jour même seulement.
- `src/ingestion/ats_maroc.py` : raw -> Bronze (rejouable sans réseau).
- `src/transform/maroc.py` + `reference/ma_villes.csv` : 29 villes, variantes en
  français, anglais et arabe, 12 régions.
- `reference/employeurs_ats.yaml`, `scripts/sonder_employeurs.py`.
- Silver multi-source : contrat Bronze commun, dates ISO ou en millisecondes, lieu
  marocain, `workplace_type`, exclusion des offres déjà publiées,
  `silver/observations`.
- Great Expectations : profils par source.
- **255 tests** (+66).

### Résultats

| Étape | Résultat |
|---|---|
| Collecte | 7 requêtes, 48 s, 5/5 employeurs ; 2e exécution : 0 appel réseau |
| Collecte d'une date passée | refusée (code 2), avec la consigne de rejouer le parsing |
| Bronze | 37 lignes ; 2e passage : ignoré (immuable) |
| Silver | 37 = 37 nouvelles + 0 rejet + 0 déjà publiée ; 18/18 contrôles critiques OK ; 43 s |
| Gold | 4 tables pour `ats_maroc` (169, 26, 14, 505 lignes) à côté de Kaggle |
| **Simulation du lendemain** | 37 lues -> 0 nouvelle, 37 déjà publiées, lot vide accepté, 37 observations ; Silver reste à 37 offres distinctes ; données du 16 ensuite supprimées |

Sur les 37 offres : 40 % sont des offres tech (DEV 7, AI_ML 3, DATA_ENG 3,
DATA_SCI 2), 94,6 % ont une ville reconnue (Casablanca, Agadir…), 62 % ont une
expérience extraite, et aucune n'a de salaire. Les technologies les plus citées sont
python, sql, spark, bigquery, gcp, aws, docker, kubernetes et vue.

Seules 66,7 % des offres tech ont au moins une technologie détectée. Vérification
faite : les 3 offres concernées (AI Engineer Manager, Junior Data Scientist, Tech
Lead Web) **ne citent réellement aucune technologie**. Ce n'est pas une faiblesse de
l'extraction.

### Difficultés

1. **Mon analyse de la veille était fausse sur un point.** J'avais compté les 51
   offres de Devoteam via SmartRecruiters sans lire le robots.txt **de l'hôte de
   l'API**, qui interdit tout. Exclu (D29).
2. **`urllib.robotparser` (le code du plan) se trompe** sur le vrai robots.txt de
   ReKrute : il déclare `Jooblebot` autorisé. Remplacé par Protego, vérifié sur le
   même fichier (D31).
3. **Lever** : une offre peut avoir Alger comme lieu principal et Casablanca en lieu
   secondaire. Le filtre Maroc porte sur tous les lieux.
4. **HTML** : `get_text("\n")` coupait les phrases à chaque balise en ligne
   (« Développer en / Kotlin »), ce qui aurait gêné l'extraction des technologies.
   Saut de ligne aux seules balises de bloc. Greenhouse envoie en plus du HTML
   **échappé** (`&lt;p&gt;`), décodé d'abord.
5. **Colonnes Bronze variables selon les employeurs du jour** : risque de perte
   silencieuse de colonnes à la lecture par Spark. Repéré en relisant, avant
   exécution : liste de colonnes fixe (D33).
6. **« IA » = Iowa** : la règle française `\bia\b` aurait classé « Nurse - Des
   Moines, IA » en AI_ML. « IA » n'est reconnu qu'après un mot de métier.
7. **Affichage** : le bilan Silver plantait sur une valeur `None` (aucun salaire).
   Corrigé, après publication, donc sans conséquence sur les données.
8. **Régression de performance, non résolue.** Le job Silver complet Kaggle est
   passé de **350 s (jour 5) à 908-920 s**. Une première hypothèse (cache placé avant
   la fenêtre de déduplication) a été corrigée **sans effet** (908 s). Le
   chronométrage par étape (D35) montre que **toutes** les étapes ont ralenti :

   | Étape | Durée |
   |---|---|
   | construction + cache (UDF) | 421 s |
   | dédoublonnage + règles + cache | 105 s |
   | audit Great Expectations | 87 s |
   | republications + écriture transit | 82 s |
   | publication (copie côté serveur MinIO) | 141 s |
   | observations / rejets | 33 s / 26 s |

   Même la copie côté serveur, non modifiée, est lente. La VM Docker utilise
   **540 Mo de swap** et le disque C: est plein à 96 %. Le benchmark en Python pur,
   sur un code inchangé, est aussi 20 à 35 % plus lent qu'au jour 3. Hypothèse
   principale : la pression sur l'environnement (mémoire, disque), à laquelle
   s'ajoute peut-être l'UDF exécutée dans la même étape que la construction du
   cache. **Non tranché.** Sans historique Git, impossible de relancer la version du
   jour 5 pour comparer. Les exécutions quotidiennes ne sont pas concernées
   (`ats_maroc` : 43 s ; une journée Kaggle : environ 6 min).

### À faire de ton côté

- **Définir `COLLECTE_CONTACT`** dans `.env` : une adresse de contact pour le
  User-Agent. Une adresse dédiée au projet est préférable à ton adresse
  personnelle. Sans elle, la collecte fonctionne mais affiche un avertissement.
- **Faire un premier commit Git**, pour pouvoir comparer les versions la prochaine
  fois.
- Facultatif, pour la régression : allouer plus de mémoire à Docker Desktop,
  redémarrer Docker (cela vide le swap), libérer de l'espace disque. Les 16,7 Go
  d'images Docker récupérables appartiennent à d'autres projets ; je n'y ai pas
  touché, j'ai seulement supprimé 2 anciennes images de ce projet.
- **Annoter les 100 offres** (jour 3), toujours en attente.

## Jour 7 — 2026-10-16 — Orchestration Airflow, alertes, pannes provoquées

### Préparation (la veille et le matin)

- **Docker : 4 -> 5,4 Go** via `C:\Users\lenovo\.wslconfig` (5,5 Go + 2 Go de
  swap). Le premier redémarrage n'a pas suffi : l'ancienne instance de Docker
  Desktop tournait encore. J'ai fermé ses processus puis je l'ai relancé.
- **Régression du jour 6 : hypothèse « mémoire » réfutée.** Avec un swap vide, la
  même journée (18 avril) prend **430 s** de construction (245 s la veille). Le PC
  est sur secteur, en mode « Utilisation normale ». Cause toujours inconnue.
- **Telegram** : bot `@jobmarket_ma_alertes_bot`, script
  `scripts/configurer_telegram.py` (recherche de l'identifiant de conversation,
  message de test reçu). Les premiers messages avaient déjà été consommés : il a
  fallu un nouveau message.
- **Git** : premier commit poussé sur `legssyera10-hub/Pipeline-jobmarket-ma`
  (identité Git locale au projet).

### Ce qui a été construit

- `docker-compose.yml` : Airflow 3.3.2 (api-server, scheduler, dag-processor,
  initialisations), base `airflow` dans le PostgreSQL existant, interface sur
  `127.0.0.1:8080`, `restart: unless-stopped` partout.
- `docker/airflow.Dockerfile` : provider Docker 4.5.9 via les contraintes
  officielles.
- `dags/commun.py` : tâche = conteneur `jobmarket-app` (`JobOperator`), alertes
  Telegram (échec définitif, rétablissement), codes de sortie non réessayés.
- `dags/ats_maroc_quotidien.py` : 14:00 et 18:00 (heure du Maroc), choisir ->
  collecte -> bronze -> silver -> gold, paramètres `source`, `collecter`, `date`.
- `dags/kaggle_manuel.py` : retraitement à la demande.
- Code : `SchemaDriftError` (format d'API), collecte `--rattrapage` + alerte,
  source isolée `ats_maroc_demo`, `scripts/preparer_incident.py`,
  `src/utils/cli.py`, codes de sortie 0/1/2/3.
- `tests/airflow/verifier_dags.py` : **19 vérifications** exécutées dans l'image
  Airflow. Tests du projet : **260**.

### Le run planifié réel

**14:00:01** : déclenchement automatique de
`scheduled__2026-10-16T13:00:00+00:00`, terminé en moins de 4 min, une tentative
par tâche.

| Tâche | Résultat |
|---|---|
| collecte | 5/5 employeurs, 37 offres au Maroc (Artefact a 140 offres au total, contre 138 la veille) |
| bronze | 37 lignes, `date=2026-10-16` |
| silver | **0 nouvelle + 37 déjà publiées** : les mêmes offres, encore ouvertes |
| gold | publié en 41 s |

La logique d'instantanés (D32) est ainsi vérifiée **en conditions réelles** : deux
jours de collecte, 37 offres distinctes dans Silver, 37 observations par jour.

### Les pannes provoquées

| Panne | Ce qui s'est passé | Tentatives | Données |
|---|---|---|---|
| **MinIO coupé 4 min pendant `silver`** | la tâche reste « en cours » ; le client S3 de Spark (S3A) réessaie seul ; reprise à la remise en route ; construction 269 s au lieu d'environ 2 min | 1 | 37 lignes, 37 distinctes, rien en transit |
| **Format d'API modifié** (Lever : `text` -> `title`) | `bronze` échoue en 90 s : « 'text' absent de 127/127 offres… rejouer raw -> Bronze » ; silver et gold non exécutés ; alerte | 1 | réponses brutes conservées |
| **Échec de validation** (lieux -> « Morocco ») | `silver` : `ville_normalisee` vide à 100 %, « NON PUBLIÉ » ; gold non exécuté ; alerte ; lot refusé gardé en transit ; journalisé dans `ops.controles_qualite` | 1 | rien publié |
| **Échec passager** (premier run de test : Spark bloqué, conteneur arrêté) | réessai 2 min plus tard avec le code corrigé, succès, alerte « ✅ rétabli à la tentative 2 » | 2 | correctes |

Limite de la panne MinIO : la coupure de 4 min a été absorbée **un niveau plus
bas** (client S3A) ; le niveau Airflow (réessais 2, 4, 8 min puis alerte) n'a pas
été déclenché par MinIO, mais il l'a été par l'échec passager ci-dessus. Les
réessais S3A sont signalés au niveau INFO, masqué par notre niveau WARN :
la preuve est la durée, pas une ligne de log.

Effet de bord de la panne de validation : en remplaçant **tous** les lieux par
« Morocco », la simulation a fait passer 969 offres du monde entier pour
marocaines (au lieu de 37). Sans conséquence : le portillon a bloqué.

### Difficultés et erreurs

1. **Spark ne démarrait pas sous Airflow.** Le driver se sert à lui-même les jars
   déclarés dans `spark.jars` ; l'envoi du jar AWS (280 Mo) a échoué dans le
   conteneur lancé par Airflow. Pire, **le processus ne s'est pas terminé** : un
   fil de la JVM l'a gardé en vie, et la tâche serait restée bloquée 30 min. Deux
   correctifs : jars mis directement sur le classpath (`extraClassPath`, et Spark
   lié à 127.0.0.1) ; `src/utils/cli.py`, qui garantit qu'un job en échec se
   termine toujours, avec un code de sortie.
2. **Conteneurs limités à 1 coeur.** `DockerOperator` a `cpus=1.0` par défaut :
   Spark en `local[4]` sur un seul coeur, 184 s pour construire 37 offres. Avec
   `cpus=4` : silver 452 -> 131 s, gold 131 -> 54 s.
3. **Tout était réessayé**, y compris les erreurs déterministes (format d'API,
   qualité) : le même échec 4 fois, et une alerte ~15 min plus tard. Convention de
   codes de sortie et `JobOperator` (D39).
4. **Ma propre erreur de date.** Le script de préparation avait copié les
   réponses du jour le plus récent (le 16, après la vraie collecte) ; il affichait
   la bonne date, mais j'ai déclenché le run avec le 15. Bronze vide, réessayé 3
   fois avec le code 1. Cela a révélé un vrai défaut : **une date vide doit sortir
   avec le code 3** (réessayer ne crée pas de données). Corrigé ; la 4e tentative,
   avec le code corrigé, est bien sortie avec le code 3, sans réessai.
5. **Ma propre erreur de script : MinIO coupé 12 minutes par erreur.** La commande
   d'arrêt était enchaînée par `;` après des attentes qui ont expiré : elle s'est
   exécutée quand même. Aucun impact sur les vraies données (run réel de 14:00
   déjà terminé, données vérifiées ensuite : 10 réponses brutes, 2 Bronze, 37
   offres distinctes, 37 observations par jour). La panne contrôlée a ensuite été
   faite avec `&&` et des conditions vérifiées.
6. Airflow 3 : `DagBag(include_examples=...)` n'existe plus ; `private_environment`
   est stocké sous `_private_environment` ; le PATCH de l'état d'un run par l'API
   REST a renvoyé une erreur de base de données (non creusée : contourné).
7. Git Bash convertit `/t/...` en `T:/...` dans les arguments de `docker run` :
   `MSYS_NO_PATHCONV=1`.

### Points ouverts

- **Le run de 18:00** (rattrapage) n'a pas été observé : il aura lieu si le PC est
  allumé et Docker lancé. À vérifier dans l'interface (http://localhost:8080).
- `COLLECTE_CONTACT` toujours vide : avertissement à chaque collecte.
- Régression de performance du jour 6 : cause inconnue (voir plus haut).
- Une alerte Telegram d'échec a été envoyée pour le run de démonstration raté
  (date vide) : ce n'était pas un incident sur les vraies données.

### Enquête de performance : ce qui a été établi au jour 7

- Pendant la dernière suite de tests (11 min 38, contre 2 min 47 au jour 6), le
  seul démarrage de la session Spark a pris **152 s**. Mesuré à froid juste après :
  **5,4 s** (session) + 7,6 s (premier calcul). Ce n'est donc pas Spark en soi.
- Hypothèse « résolution de nom lente » testée : résolution du nom d'hôte du
  conteneur en 0,3 à 1,1 s, résolution inverse instantanée -> **non confirmée**.
- Constat qui pèse : **des conteneurs oubliés** tournaient en parallèle. Une
  commande `docker compose run` coupée par un délai côté client (`timeout`) **laisse
  son conteneur tourner**. J'en ai trouvé un qui exécutait une ancienne suite de
  tests depuis 35 min (et un autre, plus tôt, un job Silver). Deux suites de tests
  se partageaient la machine. C'est probablement une cause des lenteurs observées,
  sans preuve que ce soit la seule (ni que c'était le cas au jour 6).
- Leçon : après un délai côté client, vérifier `docker ps` ; préférer des commandes
  longues en arrière-plan, sans délai qui coupe le client.

## Jour 8 — 2026-10-17 — Couche ML (MLflow), et la vraie cause des lenteurs

### Correction : run en retard

Le run de 18:00 du 16 est parti le 17 à 13:39 (PC éteint la veille) : la collecte a
refusé la date passée (code 2) et une fausse alerte d'échec est partie. `choisir`
compare maintenant la date du run à aujourd'hui : en retard -> message
d'information, collecte sautée, Bronze rejoué depuis `raw/`. Validé par un vrai
run (`retard_16`) : Silver « 0 nouvelle + 37 déjà publiées », Gold en succès.

### Serveur MLflow

Service `mlflow` (métadonnées : base PostgreSQL `mlflow` ; artefacts :
`s3://jobmarket/mlflow`, servis par le serveur). Deux problèmes rencontrés :

- `postgresql://` -> SQLAlchemy 2.1 choisit `psycopg` 3, absent : adresse
  `postgresql+psycopg2://` (D43).
- Le serveur MLflow 3 lance 7 processus `huey` (jobs GenAI, scoring en ligne)
  qui occupaient **1,35 Go** sur les 1,65 Go du conteneur. Désactivés
  (`MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`) : 421 Mo.

### Modèle de salaire

Entraîné sur le dataset américain (aucune offre marocaine n'a de salaire, D44),
découpage par entreprise (D45), 5 configurations dans MLflow (D46).

| Configuration | MAE | Erreur médiane | R² (log) |
|---|---|---|---|
| Médiane globale | 42 341 $ | 33 280 $ (40 %) | 0,00 |
| Médiane famille × niveau | 34 342 $ | 23 420 $ (29 %) | 0,26 |
| LightGBM sans titre | 30 289 $ | 19 859 $ (26 %) | 0,44 |
| LightGBM avec titre | 25 588 $ | 15 459 $ (20 %) | 0,58 |
| **LightGBM avec titre, régularisé** | **25 198 $** | **15 178 $ (19 %)** | **0,59** |

26 598 offres d'entraînement, 7 523 de test (2 033 entreprises jamais vues). Deux
exécutions complètes donnent exactement les mêmes chiffres (graines fixées).

- −27 % d'erreur par rapport à la meilleure référence naïve ; le titre apporte le
  plus (MAE 30 289 -> 25 588 $).
- SHAP (D47) : Python +8,9 % (1 153 offres), GCP +8,3 %, SAS +5,8 % ; Excel
  −3,2 % (3 994 offres), WordPress −6,7 %. Association, pas causalité. 74
  technologies dans `gold.prime_salariale_tech`.
- 80 940 salaires manquants estimés (médiane 74 636 $, 90 % entre 37 000 et
  158 000 $) -> `ml/predictions/salaire/`.
- Registre MLflow : `salaire_annuel_usd` version 1, rattachée au run retenu.

Défauts trouvés en exécution réelle, corrigés :

1. `libgomp1` (OpenMP) manquait pour LightGBM -> couche Docker dédiée (D49).
2. Le premier entraînement complet a échoué **à la toute fin** : MLflow 3
   sauvegarde LightGBM au format `skops` (sans pickle), paquet absent. Testé
   d'abord dans un conteneur jetable (sauvegarde puis rechargement identique),
   puis ajouté. Les 5 runs de cet essai ont été supprimés de MLflow (suppression
   logique, récupérable) pour garder une seule comparaison lisible.
3. L'exemple d'entrée du modèle déclenchait un échec de déduction de signature
   (colonnes catégorielles) : retiré.
4. MLflow passait ~3 min à déduire les dépendances du modèle (rechargement dans
   un sous-processus) et en oubliait : dépendances déclarées explicitement.

### Classification des familles de poste

Outil livré, **résultats en attente d'annotation** (D48) : `echantillon` (402
offres stratifiées puis mélangées, prédiction des règles masquée), guide
`docs/guide_annotation.md`, `evaluer` (règles vs TF-IDF + régression logistique,
F1 macro dans MLflow, sentence-transformers si installé). Pas d'étiquette inventée :
mesurer les règles contre leurs propres sorties n'aurait aucun sens.

Défaut de conception trouvé à la 1re exécution réelle : je rapatriais dans le
driver Spark les descriptions des 115 000 offres pour n'en garder que 402 ;
échec pendant `toPandas()` après plus de 20 min. Corrigé : tirage sur les
colonnes légères (identifiant, source, famille), puis texte des seules offres
retenues. Fichier produit en 5 min 28 s : 402 offres (37 marocaines, 365 du
dataset US), identifiants uniques, mélangées, `famille_reelle` vide. Fichier
non versionné (`data/`).

### Tests et vérifications

- 10 tests ML sur données synthétiques : exclusions, découpage sans entreprise
  commune, métriques en dollars, références, catégorie inconnue, et un test où
  la prime simulée de `spark` (+20 %) doit être retrouvée par SHAP (et pas
  attribuée à `python`, toujours cité avec lui).
- DAG `ml_manuel` ; vérification des DAGs : 24/24.
- Lint étendu à `dags/` et `scripts/` : il ne couvrait pas `dags/`, où 3 lignes
  trop longues traînaient depuis le jour 7.

### La vraie cause des lenteurs : l'hôte Windows pagine

Symptômes du jour : construction de l'image 28 min, lecture Spark de Silver
13 min, scheduler Airflow figé **6 min 17 s** sur un simple import Python
(« Heartbeat recovered after 376.79 seconds », pile obtenue avec py-spy), puis
le run de validation de `ml_manuel` **échoué deux fois** : le superviseur de
tâche n'obtient pas de réponse de l'API d'Airflow à temps (`httpx.ReadTimeout`)
et est tué avant même de lancer le conteneur.

Mesures côté **Windows** (pas dans la VM, ce que j'avais fait au jour 7) :

- RAM physique 7,8 Go, **0,4 à 0,6 Go libres** ; mémoire engagée 14,4 Go ;
  fichier d'échange : 3,5 Go utilisés (pic 4,7 Go) ;
- **1 100 à 3 900 pages lues sur disque par seconde**, disque occupé à ~51 % ;
- la VM Docker n'a que 2,1 Go résidents alors qu'elle en utilise ~5 Go : le
  reste est paginé par Windows. Chaque accès mémoire de la VM peut devenir une
  lecture disque.
- VS Code : 1,46 Go à lui seul.

Conclusion : 5,5 Go donnés à Docker sur un PC de 7,8 Go ne laissent pas assez à
Windows. C'est l'explication la plus solide, à ce jour, de la régression des
jours 6-7 (920 s contre 350 s) : l'hypothèse mémoire avait été « réfutée » en
regardant seulement **dans** la VM. Leçon : mesurer la pression mémoire de
l'hôte, pas seulement celle de l'invité.

Mesures prises : MLflow allégé de 1,35 Go ; relecture des DAGs toutes les 120 s
au lieu de 30 (mesuré : ~32 % d'un coeur en continu, D50). Le reste est un choix
d'utilisation du PC (voir le compte rendu du jour 8).

### Arrêts de processus PostgreSQL : ce que la journalisation a montré

10 arrêts au total (2 le 16 ; le 17 à 13:50, 14:13, 14:18, 14:41, 16:20, 16:25,
16:27 et 16:32 UTC), tous pendant des pics de charge ;
PostgreSQL redémarre seul, le scheduler et le processeur de DAGs Airflow le
suivent. Avec la journalisation des connexions (D51), les 4 arrêts de 16:20 à
16:32 UTC ont montré que **les processus fautifs n'avaient même pas journalisé
« connection received »** : ils meurent dans les premières millisecondes, avant
l'authentification ; l'un d'eux sur `SIGPIPE` (écriture vers un client parti).

Hypothèse la plus cohérente (non démontrée) : sur un hôte qui pagine, un processus
serveur naissant démarre plus lentement que le client n'attend ; le contrôle de
santé `pg_isready`, tué par Docker après 5 s, en est le premier candidat (toutes
les 10 s). Le client parti, le processus naissant meurt, et PostgreSQL traite
cette mort comme un crash. Atténuation : contrôle toutes les 30 s avec 30 s de
délai. **Elle n'a pas suffi** : nouvel arrêt à 19:01 UTC (processus mort sur
`SIGPIPE`, là encore avant d'avoir journalisé sa connexion), et le scheduler
s'est de nouveau figé (redémarré à la main ~24 min après). L'hypothèse « contrôle
de santé » est donc affaiblie ; le point commun de tous les arrêts reste l'hôte
qui pagine. La journalisation reste active.

### Conséquence vécue : le run de 18:00 parti avec 13 min de retard

Après l'arrêt de 16:27, le scheduler a redémarré puis s'est figé **35 minutes**
(aucun battement de coeur) : à 17:08 UTC, le run de 18:00 n'existait pas.
Scheduler redémarré à la main ; il a aussitôt créé le run manqué, qui a réussi
(collecte 23 s, Silver 193 s, Gold 44 s, tout à la 1re tentative). Sans
intervention, ce run aurait été perdu : le contrôle de santé Docker voit le
scheduler « unhealthy » mais **Docker ne redémarre pas un conteneur malsain**
(seulement un conteneur arrêté). Piste pour le jour 9 : un redémarrage
automatique sur santé dégradée.

### Validation finale par Airflow

Le run `ml_manuel` (après allègement de MLflow et reprise de PostgreSQL) a réussi
à la 1re tentative en **8 min 32 s** (contre 23 min en exécution directe dans
l'après-midi, sur un hôte plus saturé) : mêmes métriques pour la 3e fois, version
2 du modèle au registre, `gold.prime_salariale_tech` rattachée au nouveau run, et
plus aucun avertissement de signature ni de déduction des dépendances.
