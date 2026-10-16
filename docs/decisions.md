# Décisions et compromis

Format : décision · alternative écartée · raison · coût accepté.
Rédigé au moment de la décision, pas après.

## D1 — Spark tourne dans un conteneur, pas sous Windows (jour 1)

- **Alternative écartée :** PySpark installé directement sous Windows.
- **Raison :** Spark sous Windows exige `winutils.exe`/`HADOOP_HOME` et casse souvent sur
  les écritures de fichiers. La machine n'a que Python 3.9/3.12/3.13 et Java 8, alors que
  la cible est Python 3.11 + Java 17. Une image Docker fixe tout cela une fois, et la
  même image servira à Airflow (jour 7) et à la CI (jour 9).
- **Coût :** image de 1,4 Go, premier build d'environ 8 minutes ; démarrage de Spark un
  peu plus lent ; l'IDE n'a pas l'interpréteur du conteneur sans configuration.

## D2 — MinIO via le fork communautaire `pgsty/minio` (jour 1)

- **Alternative écartée :** `minio/minio` (image officielle), SeaweedFS, Garage.
- **Raison :** les images officielles MinIO ne sont plus téléchargeables (Docker Hub et
  quay.io refusent l'accès). `pgsty/minio` recompile le même code source AGPL : même API
  S3, même client `mc`, même console. Aucun code du pipeline ne dépend de MinIO en
  particulier — seulement de l'API S3 (`s3a://` côté Spark, `s3fs`/`boto3` côté Python).
- **Coût :** dépendance à un mainteneur tiers. Mitigation : versions figées
  (`RELEASE.2026-08-04T00-00-00Z`), et remplacement par tout autre stockage S3 en
  changeant une ligne du `docker-compose.yml`.

## D3 — Jars S3A/JDBC pré-téléchargés dans l'image (jour 1)

- **Alternative écartée :** `spark.jars.packages` (téléchargement Maven à chaque démarrage).
- **Raison :** builds reproductibles et démarrage hors ligne. `hadoop-aws` doit être
  **exactement** de la version du Hadoop embarqué dans PySpark 3.5 (3.3.4), sinon on
  obtient des `NoSuchMethodError` au runtime ; `aws-java-sdk-bundle` 1.12.262 est la
  version dont `hadoop-aws` 3.3.4 dépend.
- **Coût :** une montée de version de Spark impose de mettre à jour ces trois versions
  à la main (documentées dans `docker/app.Dockerfile`).

## D4 — Overwrite *dynamique* des partitions par défaut (jour 1)

- **Alternative écartée :** overwrite statique (comportement par défaut de Spark).
- **Raison :** en statique, `mode("overwrite")` efface **toute** la table avant d'écrire.
  En dynamique, seules les partitions présentes dans le DataFrame sont remplacées :
  relancer le run du 12 réécrit la partition du 12 et ne touche à rien d'autre. C'est la
  base de l'idempotence. Vérifié par `tests/test_spark_local.py` et `check_infra`.
- **Coût :** un run qui produit un DataFrame vide ne nettoie pas sa partition (aucune
  partition à remplacer). Traité au jour 2 : l'écriture Bronze refuse un lot vide.

## D5 — Secrets uniquement dans `.env`, compose qui échoue s'ils manquent (jour 1)

- **Alternative écartée :** identifiants par défaut écrits en dur dans le compose et le code.
- **Raison :** le dépôt sera public. `${VAR:?message}` dans le compose et `_require()`
  dans `src/utils/config.py` font échouer immédiatement et clairement un démarrage sans
  secret, plutôt que de tomber silencieusement sur un mot de passe par défaut.
- **Coût :** une étape de plus au démarrage (`cp .env.example .env`).

## D6 — Great Expectations reporté au jour 5 (jour 1)

- **Raison :** l'API a radicalement changé entre 0.18 et 1.x et la plupart des
  tutoriels sont obsolètes ; on choisira et figera la version (ou Pandera) au moment de
  l'utiliser, plutôt que de traîner une dépendance lourde et non utilisée.

## D7 — Bronze stocke toutes les colonnes source en texte (jour 2)

- **Alternative écartée :** laisser pandas ou pyarrow deviner les types.
- **Raison :** l'inférence détruit des données. `zip_code` « 08540 » devient `8540.0`
  (9 271 codes concernés), et une colonne vide dans une petite partition devient de
  type `null`, incompatible avec les autres fichiers. Bronze doit pouvoir rejouer
  n'importe quel traitement à l'identique : il garde la valeur exacte. Le typage est
  fait en Silver, une seule fois, à un seul endroit.
- **Coût :** des fichiers Bronze un peu plus gros, et un cast obligatoire en Silver.

## D8 — Partition Bronze = date de première publication, `original_listed_time` (jour 2)

- **Alternative écartée :** `listed_time` (date de dernière republication).
- **Raison :** c'est la date de publication de l'offre, demandée pour le
  partitionnement, et elle correspond au champ `date_publication` de Silver.
  `listed_time` reste disponible en colonne brute pour la déduplication. L'écart est
  faible (médiane de 0 jour, 90e centile à 1 jour). Conversion de l'epoch ms en date
  **UTC**.
- **Coût :** une distribution très inégale. 49 % des offres sont sur 2 jours et 25
  partitions ont moins de 10 lignes : des petits fichiers que Spark lira mal. Quand
  la vraie source sera branchée (jour 6), la partition sera la date de **collecte**
  (`{{ ds }}`), ce qui donnera des partitions régulières.

## D9 — Immuabilité garantie par le stockage : `If-None-Match: *` (jour 2)

- **Alternatives écartées :** (a) écraser la partition, comme dans le plan initial ;
  (b) vérifier si la clé existe puis écrire.
- **Raison :** (a) viole l'immuabilité, car une recollecte du même jour remplacerait
  silencieusement les données brutes d'origine. (b) laisse une fenêtre où deux runs
  concurrents s'écrasent. Avec l'écriture conditionnelle, c'est MinIO qui refuse
  atomiquement (HTTP 412). Un run déjà fait devient « skipped » : l'ingestion est
  **idempotente et immuable**. Comportement vérifié sur le vrai MinIO, pas seulement
  dans les tests.
- **Coût :** corriger une écriture fautive exige une purge explicite
  (`scripts/purge_bronze.py`). C'est arrivé dès le jour 2 (voir le journal), et c'est
  voulu : Bronze ne se modifie jamais par accident.

## D10 — Cache local Parquet partitionné pour simuler la source (jour 2)

- **Alternative écartée :** relire le CSV de 493 Mo à chaque `fetch(date)`.
- **Raison :** le CSV n'est pas découpé par jour, et 61 relectures complètes
  prendraient des dizaines de minutes. Une passe unique en **streaming** avec pyarrow
  (Docker n'a que 4 Go de RAM) produit `data/staging/` (235 Mo). Chaque date se lit
  ensuite en moins d'une seconde. L'interface `Collector.fetch(run_date)` reste celle
  d'une vraie source quotidienne, compatible avec Airflow. Le cache est reconstruit
  si le CSV change.
- **Coût :** 235 Mo de disque, et une étape de plus à expliquer. Ce cache n'est
  **pas** Bronze : c'est un simulateur de source, jamais lu par Silver.

## D11 — `Collector.fetch` renvoie un DataFrame plutôt qu'une `List[Dict]` (jour 2)

- **Raison :** jusqu'à 33 000 offres de 3 à 20 Ko par jour. Une liste de
  dictionnaires multiplie la mémoire et perd le schéma. Le DataFrame de textes passe
  directement à pyarrow.

## D12 — Republications marquées, jamais supprimées (jour 3)

- **Alternative écartée :** la déduplication du plan (filtre `current_date() - 30 j`
  puis `row_number() = 1`).
- **Raison :** le code du plan avait trois défauts. `current_date()` rend le
  résultat dépendant du jour d'exécution, ce qui casse l'idempotence. Sur un dataset
  de 2024, le filtre ne gardait **aucune** ligne. Et il supprimait tout ce qui avait
  plus de 30 jours, y compris les postes rouverts qu'on voulait garder. Version
  retenue : `lag(date_publication)` sur la clé (entreprise, titre, ville). Une offre
  est une republication si l'occurrence précédente date de 30 jours ou moins. Rien
  n'est supprimé : `est_republication` et `date_occurrence_precedente` permettent à
  Gold de choisir. Le job relit 30 jours de Bronze en amont pour que la fenêtre voie
  les occurrences antérieures.
- **Coût :** les annonces multi-postes du même jour sont comptées comme
  republications (56 % des cas). Deux shuffles de lignes complètes (descriptions
  comprises).

## D13 — Une seule UDF pandas (Arrow) pour tout le parsing Python (jour 3)

- **Alternatives écartées :** une UDF Python par champ, ou des UDF ligne à ligne.
- **Raison :** la description (3,6 Ko en moyenne) ne traverse la frontière JVM ->
  Python qu'une fois, par lots Arrow, au lieu d'une fois par champ et par ligne. Les
  fonctions restent pures et testables sans Spark (110 tests en 1 s). Le plan
  physique confirme une seule évaluation (`ArrowEvalPython`). Les transformations
  simples (dates, salaires, contrat, télétravail) restent en expressions Spark
  natives, plus rapides et optimisables par Catalyst.
- **Coût :** ~4 ms par offre (dont 3,4 ms pour les technologies), soit ~8 min CPU
  pour 124 000 offres. Piste : tokeniser une fois par offre au lieu de 174
  recherches de sous-chaînes.

## D14 — Référentiels joints par broadcast (jour 3)

- **Raison :** les états (104 clés : code et nom) et les zones métropolitaines (43
  lignes) font quelques Ko. Les diffuser à chaque exécuteur évite de redistribuer les
  124 000 offres sur le réseau (shuffle). Vérifié dans le plan :
  `BroadcastHashJoin LeftOuter BuildRight` ×2, aucun `SortMergeJoin`, et un test
  l'impose (`test_reference_joins_are_broadcast`). Plan complet :
  `docs/plans/silver_plan.txt`.
- **Coût :** aucun à cette taille. Un référentiel de plusieurs centaines de Mo ne
  devrait pas être diffusé.

## D15 — Expérience : contexte obligatoire, plus faible mention retenue (jour 3)

- **Alternative écartée :** les motifs du plan sans contexte, et un repli sur des
  mots-clés (« senior » -> 5 ans).
- **Raison :** sans le mot « experience » à proximité, on extrait l'âge minimum ou
  l'ancienneté de l'entreprise. Quand il y a plusieurs mentions, la plus faible
  correspond aux alternatives (« 14 ans ; OU master et 12 ans » -> 12). Les
  mots-clés ont été écartés : « senior management » ou « work with senior leaders »
  apparaissent partout. Le niveau déclaré par la source est gardé à part
  (`niveau_experience`).
- **Coût :** « 1+ an de SDLC, 5 ans de développement » donne 1. Couverture de 54 %.

## D16 — Salaires annualisés en devise d'origine, pas en MAD mensuel (jour 3)

- **Raison :** le dataset de développement est américain (USD, 5 périodes de
  paiement). Multiplicateurs : heure ×2080, semaine ×52, quinzaine ×26, mois ×12.
  Validation : 100 % de concordance avec `normalized_salary`, calculé
  indépendamment par le dataset. La conversion en MAD mensuel viendra avec la source
  marocaine (jour 6) : colonne `devise` déjà présente.
- **Coût :** le schéma du plan (`salaire_min` en MAD net mensuel) n'est pas encore
  respecté.

## D17 — Silver partitionné par source puis date de collecte (jour 3)

- **Alternative écartée :** `date_collecte` seule.
- **Raison :** avec l'overwrite dynamique, relancer la source A pour le 12 avril
  effacerait les offres de la source B du même jour si les deux partageaient la
  partition. `source=.../date_collecte=...` isole les sources. Un `repartition` sur
  la date avant l'écriture produit **un fichier par partition** (61 fichiers, pas
  61 × 8).

## D18 — Spark dimensionné pour 4 Go : `local[4]`, lots Arrow de 2 000 (jour 3)

- **Raison :** voir le journal du jour 3. Chaque coeur lance un worker Python dont
  la mémoire est hors heap JVM. Avec `local[*]` (8 coeurs) et des lots de 10 000,
  la VM swappait et le job ne finissait pas.
- **Coût :** moins de parallélisme. À réévaluer si on alloue plus de mémoire à
  Docker.

## D19 — Deux niveaux de règles : quarantaine (bloquant) et anomalies (signalées) (jour 5)

- **Alternative écartée :** tout rejeter dès qu'un champ est suspect, comme le plan
  le suggérait pour les salaires (« salaire entre 0 et 100 000 »).
- **Raison :** une offre au salaire aberrant reste exploitable : son titre, ses
  technologies et sa ville sont justes. La rejeter fausserait les statistiques de
  technologies pour un défaut qui ne les concerne pas. Seuls les défauts qui rendent
  la ligne inexploitable partout sont bloquants : identifiant, titre, date de
  publication manquants, ou publication après la collecte. Les autres vont dans
  `_anomalies` et chaque usage filtre (le modèle salarial du jour 8 exclura
  `salaire_hors_bornes`).
- **Coût :** les consommateurs doivent penser à filtrer `_anomalies`.

## D20 — Corrections du code de quarantaine du plan (jour 5)

- `filter(valide)` / `filter(~valide)` : si `valide` vaut NULL (date absente), la
  ligne n'est dans **aucun** des deux. Le motif est maintenant un tableau, et le test
  porte sur sa taille, qui n'est jamais NULL. Un test de régression le prouve.
- `when(...).when(...).otherwise("date future")` : un seul motif, le premier, et une
  date NULL étiquetée « date future ». Les motifs sont maintenant **cumulés**.
- `date_publication <= current_date()` : dépend du jour d'exécution. Le contrôle
  compare désormais à `date_collecte` : une offre ne peut pas avoir été publiée après
  qu'on l'a collectée.

## D21 — Great Expectations comme portillon : write-audit-publish (jour 5)

- **Alternatives écartées :** valider après publication (on constate le problème,
  mais Gold a déjà consommé la donnée) ; valider un DataFrame mis en cache en
  mémoire (essayé d'abord : `OutOfMemoryError`, voir le journal).
- **Raison :** le lot valide est **écrit** dans `silver/_staging/`, **audité** par
  GX en relisant ce Parquet (chaque contrôle ne lit que ses colonnes, jamais les
  descriptions), puis **publié** par copie côté serveur vers `silver/offres`. Un échec
  critique (doublons, valeur hors référentiel, dérive d'un taux de remplissage, taux
  de rejet supérieur à 5 %) empêche la publication : la version précédente reste en
  place, le job sort avec le code 1 (Airflow alertera au jour 7). Les rejets, eux,
  sont toujours écrits, pour diagnostiquer. Un lot refusé reste dans `_staging`
  pour être inspecté. Les résultats de chaque contrôle sont historisés dans
  `ops.controles_qualite`.
- **Coût :** une écriture de plus (le lot passe par le transit), et une publication
  partition par partition (supprimer puis copier) : une partition est absente une
  fraction de seconde. Acceptable puisque Gold s'exécute après Silver dans le DAG.
  Le temps total n'a pas augmenté (350 s).
- **Pourquoi GX et pas des `assert` maison :** un vocabulaire standard de contrôles,
  le seuil `mostly` pour tolérer un bruit attendu, et un résultat structuré
  (nombre et exemples de valeurs inattendues).
- **Coût :** une dépendance lourde. Version figée (1.24.0) après un test de
  compatibilité avec PySpark 3.5 et numpy 2 : aucune version existante n'a été
  modifiée. Contexte éphémère : les suites sont du code Python versionné, pas des
  JSON générés.

## D22 — Seuils de remplissage = valeur observée moins une marge (jour 5)

- **Raison :** un seuil arbitraire (« 90 % ») échoue sur des données saines ou laisse
  passer une dérive. Seuils retenus à partir des mesures du jour 3 : ville 85 %
  (observé 93,4 %), état 80 % (89,8 %), entreprise 95 % (98,6 %).
- **Coût :** à recalibrer pour la source marocaine (jour 6), dont les taux seront
  différents.

## D23 — Partitions devenues vides supprimées explicitement (jour 5)

- **Raison :** l'overwrite dynamique ne remplace que les partitions présentes dans le
  DataFrame écrit. Si une relance déplace toutes les lignes d'une date de la
  quarantaine vers les valides, l'ancienne partition de rejets resterait en place.
  Le job supprime donc les partitions cibles qui n'ont plus de lignes, dans les deux
  tables. C'est le coût identifié en D4, traité ici.

## D24 — Gold : recalcul complet, transit puis bascule transactionnelle (jour 5)

- **Alternatives écartées :** `mode("overwrite")` en JDBC directement dans `gold.*`
  (Spark supprime puis recrée la table : Power BI peut lire une table vide, et les
  clés primaires disparaissent) ; un calcul incrémental.
- **Raison :** Spark écrit dans `gold_staging`, puis **une** transaction PostgreSQL
  remplace les lignes de la source dans `gold.*`. Grâce au MVCC de PostgreSQL, un
  lecteur voit l'ancienne version ou la nouvelle, jamais un état intermédiaire. Les
  tables sont définies en SQL (`sql/schema.sql`) avec des clés primaires : un
  doublon d'agrégat fait échouer la transaction au lieu de passer inaperçu. Le
  recalcul complet porte sur quelques milliers de lignes : l'incrémental ajouterait
  de la complexité sans gain.
- **Coût :** le recalcul relit tout Silver (une projection de 9 colonnes, sans les
  descriptions).

## D25 — Gold compte les offres uniques ET les annonces (jour 5)

- **Raison :** tranche la question ouverte au jour 3 (annonces multi-postes). Chaque
  table porte `nb_offres` (hors republications) et `nb_annonces` (toutes). Le
  tableau de bord choisit et affiche explicitement l'unité. Chaque table porte aussi
  `source` : le dataset américain et la future source marocaine ne seront jamais
  agrégés ensemble.

## D26 — Comparaisons de texte en COLLATE "C" entre Spark et PostgreSQL (jour 5)

- **Raison :** Spark compare les chaînes octet par octet, PostgreSQL (`en_US.utf8`)
  selon des règles linguistiques qui ignorent presque `_`. La contrainte
  `tech_a < tech_b` acceptée par Spark pour (`t_sql`, `terraform`) était refusée
  par PostgreSQL. Toute règle d'ordre partagée entre les deux moteurs utilise
  `COLLATE "C"` côté PostgreSQL.
- **Coût :** aucun. La contrainte a fait son travail : la transaction entière a été
  annulée, et Gold n'a jamais été publié dans un état incohérent.

## D27 — Normalisation Unicode, pas seulement ASCII (jour 5)

- **Raison :** `[^a-z0-9]` effaçait tout titre en écriture non latine. 6 offres
  (chinois, hébreu, grec, coréen) sont parties en quarantaine pour « titre
  manquant ». La source marocaine aura des titres en **arabe** : tous auraient été
  rejetés. Correction : `\w` Unicode, et recomposition NFC après le retrait des
  accents. Tests en arabe (avec et sans voyelles courtes), chinois, hébreu, coréen.

## D28 — Source marocaine : API publiques des ATS (Greenhouse, Lever) (jour 6)

- **Alternatives écartées** (analyse du 2026-10-15, voir le journal) : ReKrute (son
  robots.txt interdit le site aux agrégateurs d'offres), Emploi.ma (protection
  Cloudflare, qu'on ne contourne pas), MarocAnnonces (`Disallow: /`), Indeed et
  LinkedIn (conditions d'utilisation), API Jooble et Careerjet (conçues pour
  l'affichage), data.gov.ma (secteur public sans spécialité ni description).
- **Raison :** ce sont les API que les employeurs publient pour diffuser leurs propres
  offres. Elles fournissent du JSON stable, des descriptions complètes (donc
  l'extraction des technologies fonctionne), des identifiants stables, et leur
  robots.txt autorise l'accès.
- **Coût :** le volume est faible (37 offres, 5 employeurs) et l'échantillon est
  biaisé vers le conseil et les startups. Ce n'est **pas** « le marché marocain ».
  Le CV doit le dire.

## D29 — SmartRecruiters exclu malgré une API publique (jour 6)

- **Raison :** le robots.txt de `api.smartrecruiters.com` contient
  `User-agent: * / Disallow: /` ; seul LinkedInBot est autorisé. La règle du projet
  (respecter robots.txt) prime sur le caractère public de l'API. Coût : 51 offres
  de Devoteam au Maroc non collectées. Mon analyse de la veille les comptait : je
  l'ai corrigée en vérifiant le robots.txt de l'hôte de l'API lui-même.

## D30 — Collecte et parsing séparés ; collecte limitée au jour même (jour 6)

- **Raison :** `raw/` stocke chaque réponse JSON **complète et immuable**
  (`If-None-Match`), avec l'URL, l'heure et le User-Agent. Le parsing
  (raw -> Bronze) est une étape distincte : si une API change de format, on corrige
  le parseur et on rejoue, sans aucun appel réseau. Une API donne l'état du
  **moment** : collecter « le 12 octobre » le 15 produirait des données fausses
  étiquetées comme justes. La collecte refuse donc toute autre date que le jour même
  (code de sortie 2), et c'est le parsing qui se rejoue.
- **Coût :** un jour manqué est perdu pour toujours. D'où l'importance de
  l'orchestration et des alertes (jour 7).

## D31 — Un seul client HTTP « poli » ; robots.txt lu avec Protego (jour 6)

- **Alternative écartée :** `urllib.robotparser`, utilisé par le code du plan.
- **Raison :** testé sur le vrai robots.txt de ReKrute, il déclare `Jooblebot`
  autorisé alors que le fichier dit `Disallow: /` (il gère mal les jokers et les
  groupes). Protego (le parseur de Scrapy) donne la bonne réponse. Toutes les
  requêtes passent par `PoliteClient` : User-Agent identifiable, au moins 2 s
  entre deux appels au même hôte (ou le `Crawl-delay` s'il est plus long), réessais
  espacés sur 429/5xx avec `Retry-After`, pas de réessai sur une erreur 4xx, aucun
  parallélisme. Une collecte complète fait 7 requêtes en 48 s.

## D32 — Source d'instantanés : dédoublonnage contre tout Silver + journal des observations (jour 6)

- **Raison :** l'API renvoie chaque jour toutes les offres encore ouvertes. Une offre
  ouverte 45 jours apparaîtrait dans 45 collectes, et la fenêtre relue (30 jours) ne
  suffit pas à la reconnaître. Silver exclut donc tout `offre_id` **déjà publié**
  (anti-jointure sur une seule colonne), et compte ces lignes à part
  (`lignes_deja_publiees`). Chaque présence quotidienne est conservée dans
  `silver/observations` (offre, jour). Cela permettra de mesurer la **durée de mise
  en ligne** des offres, ce que le dataset Kaggle ne pouvait pas offrir. Prouvé
  par une simulation du lendemain : 37 lues, 0 nouvelle, 37 déjà publiées, 37
  observations pour le jour suivant.
- **Coût :** relire les `offre_id` de toute la source à chaque exécution. Une seule
  colonne, cela reste négligeable longtemps.

## D33 — Contrat Bronze -> Silver commun à toutes les sources (jour 6)

- **Raison :** les champs consommés par Silver portent les mêmes noms dans toutes les
  sources (`title`, `description`, `location`, `original_listed_time`...).
  `conform_bronze` ajoute, vides, ceux qu'une source ne fournit pas. Les dates
  acceptent les millisecondes (Kaggle, Lever) comme l'ISO 8601 avec fuseau
  (Greenhouse). Le lieu est résolu par le référentiel marocain (ville, région) quand
  la source déclare `pays_code = MA`. Les colonnes Bronze d'une source sont **fixes**,
  quels que soient les employeurs du jour : sinon Spark perdrait silencieusement des
  colonnes en lisant plusieurs jours.
- **Coût :** les noms hérités du dataset Kaggle (`job_posting_url`...) sont en
  anglais. C'est un contrat interne, documenté ici.

## D34 — Profils de qualité par source (jour 6)

- **Raison :** applique D22. Pour `ats_maroc` : pas de contrôle d'état américain,
  lot vide accepté (un jour sans nouvelle offre est normal pour des instantanés),
  seuil de ville provisoire à 60 % (observé : 94,6 % sur 37 offres, trop peu pour
  calibrer), devises MAD, EUR ou USD. À recalibrer après deux semaines de collecte.

## D35 — Chronométrage de chaque étape du job Silver (jour 6)

- **Raison :** le job complet est passé de 350 s à 915 s. Une première hypothèse,
  testée sans mesure (deuxième cache), n'a rien changé (908 s). Le chronométrage par
  étape (`[silver] étape ...`) a montré que **toutes** les étapes avaient ralenti,
  y compris celles qui n'avaient pas été modifiées (copie côté serveur : 141 s). Cela
  oriente vers l'environnement (swap de la VM Docker, disque) plutôt que vers le
  code. Les durées restent dans les logs : elles serviront dans Airflow.

## D36 — Airflow 3.3.2 plutôt que la 2.9 du plan (jour 7)

- **Raison :** Airflow 3 est la version courante. La 3.3.2 est le dernier correctif
  de la branche 3.3, et l'image est prise avec le **fichier de contraintes officiel**
  (provider Docker 4.5.9, docker-py 7.2.0). Différences assumées par rapport au
  plan : `sla` n'existe plus (remplacé par `dagrun_timeout` et `execution_timeout`
  par tâche, plus les alertes) ; les composants sont séparés (api-server, scheduler,
  dag-processor) ; les imports viennent de `airflow.sdk`.
- **Coût :** les tutoriels Airflow 2 ne s'appliquent pas tels quels ; certaines API
  ont changé (`DagBag` n'accepte plus `include_examples`, par exemple).

## D37 — Chaque tâche = un conteneur `jobmarket-app` (DockerOperator) (jour 7)

- **Alternative écartée :** installer Spark, Great Expectations et le code dans
  l'image Airflow.
- **Raison :** Airflow ne fait qu'ordonnancer ; les jobs tournent dans **la même
  image que les tests**. Aucun conflit possible entre les dépendances d'Airflow et
  celles du pipeline (GX, PySpark, numpy 2...). Le code est monté depuis la machine
  hôte : une correction est prise en compte au réessai suivant, sans reconstruire
  d'image (c'est arrivé : voir le journal).
- **Coût :** Airflow accède au socket Docker (`/var/run/docker.sock`), ce qui revient
  à un accès administrateur à Docker. Acceptable en local, interface liée à
  127.0.0.1 ; **à ne pas reproduire tel quel en production**. Chaque tâche paie
  aussi le démarrage d'un conteneur et d'une JVM (~1 min).

## D38 — Collecte à 14:00, rattrapage à 18:00 (jour 7)

- **Raison :** une API ne rend que l'état du jour : un jour manqué est perdu.
  14:00 : un employeur en échec arrête la suite, car on ne fige pas un Bronze
  incomplet (il est immuable). 18:00 : seule la collecte des employeurs manquants
  est retentée, puis la journée est traitée avec ce qui a été collecté, avec alerte.
  Le comportement est déterminé par l'heure de la **date logique** (pas par
  `now()`) : un run relancé plus tard se comporte comme à son heure prévue.
- **Coût :** si le PC est éteint à 14:00 **et** à 18:00, la journée est perdue.

## D39 — Codes de sortie : seules les pannes passagères sont réessayées (jour 7)

- **Alternative écartée :** 3 réessais pour tout (configuration naïve).
- **Raison :** un changement de format d'API ou un lot refusé par la qualité donnera
  le même résultat 4 fois ; réessayer ne fait que retarder l'alerte d'environ
  15 minutes. Convention : 0 succès ; 1 panne passagère (MinIO, réseau) -> 3
  réessais espacés (2, 4, 8 min) ; 2 erreur d'usage et 3 données invalides ->
  `AirflowFailException`, échec immédiat et alerte. Mesuré : la panne de format
  échoue en 90 s, une seule tentative.

## D40 — Alertes Telegram : échec définitif et rétablissement (jour 7)

- **Raison :** une alerte par **échec définitif** (après réessais) avec les
  dernières lignes du log du conteneur, et une alerte quand une tâche **réussit
  après un échec** (l'incident s'est résolu seul). Pas d'alerte à chaque tentative,
  pour ne pas noyer les vraies. Les secrets passent en `private_environment`
  (jamais affichés dans l'interface d'Airflow). Une alerte qui échoue n'empêche
  jamais de voir l'erreur d'origine.

## D41 — Pool `spark` à 1 place, parallélisme 2 (jour 7)

- **Raison :** Docker dispose de 5,4 Go ; deux jobs Spark simultanés (par exemple
  le retraitement Kaggle et le run quotidien) ne tiennent pas en mémoire. Silver et
  Gold de tous les DAGs passent par le pool `spark` (1 place) : ils s'attendent au
  lieu de se faire tuer par manque de mémoire.

## D42 — Pannes provoquées sur une source isolée (`ats_maroc_demo`) (jour 7)

- **Raison :** provoquer une panne sur les vraies données reviendrait à les
  corrompre. `scripts/preparer_incident.py` copie les vraies réponses brutes vers
  `ats_maroc_demo` en les altérant, le DAG est déclenché avec
  `{"source": "ats_maroc_demo", "collecter": false}`, puis tout est nettoyé
  (MinIO et Gold). Le même code et le même DAG sont exercés que pour la vraie
  source.
