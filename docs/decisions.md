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

## D43 — Serveur MLflow dédié, métadonnées PostgreSQL, artefacts MinIO (jour 8)

- **Raison :** un fichier `mlruns/` local ne survit pas aux conteneurs jetables
  (`docker compose run --rm`) et n'est pas visible depuis Airflow. Le service
  `mlflow` (image `jobmarket-mlflow:3.17.1`) stocke les runs dans la base
  PostgreSQL `mlflow` (créée par `airflow-db-init`) et les artefacts (modèles,
  graphiques) dans `s3://jobmarket/mlflow`. `--serve-artifacts` : les clients
  passent par le serveur, ils n'ont pas besoin des identifiants MinIO pour cela.
  Les jobs n'embarquent que `mlflow-skinny` (client léger).
- **Piège rencontré :** avec SQLAlchemy 2.1, `postgresql://` désigne le pilote
  `psycopg` 3, absent de l'image -> adresse explicite `postgresql+psycopg2://`.
- **Sécurité :** port publié sur `127.0.0.1` seulement ; `--allowed-hosts`
  restreint les en-têtes Host acceptés (protection contre le DNS rebinding).

## D44 — Modèle de salaire entraîné sur le dataset américain, dit tel quel (jour 8)

- **Constat :** 0 offre marocaine sur 37 publie un salaire. Impossible d'entraîner
  ou d'évaluer un modèle de salaire marocain.
- **Choix :** entraîner sur `kaggle_linkedin` (USD), le documenter partout
  (docstring, table Gold `source`, graphique titré « dataset US ») et ne jamais
  appliquer ce modèle aux offres marocaines : des dollars américains n'ont aucun
  sens pour le marché marocain.
- **Variables :** seulement celles disponibles aussi quand le salaire manque
  (sinon l'imputation serait impossible) ; les salaires en anomalie (jour 5) et
  les republications sont exclus ; cible = log du milieu de fourchette (erreurs
  relatives, distribution asymétrique).

## D45 — Découpage entraînement/test par entreprise (jour 8)

- **Raison :** une même entreprise publie des dizaines d'annonces quasi
  identiques au même salaire. Un découpage aléatoire en mettrait des deux côtés
  et le modèle « reconnaîtrait » l'annonce : score gonflé. `GroupShuffleSplit`
  par `entreprise_normalisee` mesure la capacité à estimer le salaire d'une
  entreprise jamais vue.

## D46 — Deux références naïves avant tout modèle (jour 8)

- **Raison :** une MAE seule ne dit rien. Les 5 configurations suivies dans
  MLflow : médiane globale, médiane par famille × niveau, puis LightGBM sans
  titre, avec titre (TF-IDF -> SVD 30), avec titre et régularisation. Le gain de
  LightGBM se lit par rapport à la médiane par famille × niveau.

## D47 — SHAP : effet ASSOCIÉ des technologies, pas un effet causal (jour 8)

- **Méthode :** valeur SHAP moyenne de l'indicatrice `tech_<nom>` sur les offres
  de test qui citent la technologie ; `effet_pct = exp(shap) - 1`. Une
  technologie citée par moins de 50 offres d'entraînement est écartée (bruit).
- **Limite assumée :** « Spark +8 % » signifie que, dans ce modèle, à famille,
  niveau, lieu et titre comparables, les offres citant Spark sont estimées 8 %
  plus haut que l'offre moyenne. Cela ne veut pas dire qu'apprendre Spark fait
  gagner 8 %. Le graphique et la table Gold le rappellent.
- **Traçabilité :** chaque ligne de `gold.prime_salariale_tech` porte le
  `mlflow_run_id` du modèle qui l'a produite.

## D48 — Classification : vérité terrain annotée à la main, pas les règles (jour 8)

- **Raison :** entraîner un classifieur sur les étiquettes des règles regex lui
  apprendrait à imiter ces règles ; mesurer les règles contre elles-mêmes
  donnerait 100 %. Il faut des étiquettes humaines.
- **Outil :** `python -m src.ml.classification echantillon` tire 402 offres
  (les 37 marocaines + 65 par famille tech + 40 « AUTRE » du dataset US),
  stratifiées sur la prédiction des règles, puis mélangées ; le fichier ne montre
  pas la prédiction (pas d'ancrage). `evaluer` compare règles, TF-IDF +
  régression logistique (validation croisée stratifiée) et, si installé,
  sentence-transformers multilingue ; F1 macro suivi dans MLflow.
- **Limite :** l'échantillon est stratifié, pas représentatif. Les scores
  mesurent la séparation des familles, pas l'exactitude sur la population.
- **Différé :** `sentence-transformers` (+ PyTorch, ~1 Go) n'est pas installé
  tant qu'il n'y a pas d'annotations pour l'évaluer.

## D49 — LightGBM et OpenMP dans une couche Docker séparée (jour 8)

- **Raison :** LightGBM exige `libgomp1`. L'ajouter à la première commande
  `apt-get` aurait invalidé le cache de la couche pip (28 min de reconstruction
  sur cette machine) ; une couche dédiée après pip s'est construite en 11 s.

## D50 — Relecture des DAGs toutes les 120 s (jour 8)

- **Mesure :** sur 60 s (compteur CPU du cgroup, pas l'instantané de
  `docker stats`, trompeur : il affichait 450 à 1 200 % lors des pics), le
  processeur de DAGs consommait ~32 % d'un coeur en continu pour relire 3
  fichiers toutes les 30 s.
- **Choix :** `min_file_process_interval=120`. Contrepartie : une modification
  de DAG apparaît dans l'interface après 2 min au plus.

## D51 — Journalisation des connexions PostgreSQL, à titre de diagnostic (jour 8)

- **Constat :** un processus serveur PostgreSQL s'arrête parfois avec le code 2,
  sans message (2 fois le 16/10, 8 fois le 17/10, toujours sous forte charge).
  PostgreSQL redémarre seul en moins d'une minute ; le scheduler et le processeur
  de DAGs Airflow perdent leur base et redémarrent (le planificateur reprend
  ensuite normalement). La mémoire n'est pas en cause (2,1 Go disponibles,
  swap quasi vide au moment de l'arrêt).
- **Choix :** `log_connections=on` et le nom de l'application dans chaque ligne,
  pour relier le PID du prochain arrêt à son client. Résultat : les processus
  fautifs meurent **avant** même de journaliser leur connexion (l'un sur
  `SIGPIPE`). Hypothèse retenue, non démontrée : un client (d'abord le contrôle
  de santé, tué après 5 s) raccroche avant que le processus naissant ait démarré
  sur un hôte qui pagine. Atténuation : contrôle de santé à 30 s / 30 s,
  **insuffisante** (un arrêt de plus à 19:01) : l'hypothèse est affaiblie, la
  pression mémoire de l'hôte reste le seul point commun établi. À retirer une
  fois les arrêts disparus (une ligne par connexion).

## D52 — Jobs serveur de MLflow désactivés ; la mémoire de l'hôte d'abord (jour 8)

- **Constat :** le PC a 7,8 Go de RAM ; Docker en reçoit 5,5 Go. Côté Windows :
  0,4 à 0,6 Go libres, 3,5 Go dans le fichier d'échange, 1 100 à 3 900 pages lues
  par seconde. La VM Docker est elle-même paginée par Windows : un import Python
  a figé le scheduler 6 min, et le superviseur des tâches Airflow a dépassé ses
  délais (run `ml_manuel` échoué deux fois avant de lancer son conteneur).
- **Choix :** `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false` (7 processus `huey` pour
  des fonctions GenAI inutilisées : 1,65 Go -> 421 Mo).
- **Hors du code :** fermer les applications lourdes pendant les traitements, ou
  réduire la mémoire de Docker ; c'est un arbitrage d'utilisation du PC, laissé à
  son propriétaire. Leçon de méthode : la pression mémoire se mesure sur l'hôte,
  pas seulement dans la VM.

## D53 — Contrôle de santé auto-réparateur pour le scheduler et l'api-server (avant jour 9)

- **Constat :** Docker ne redémarre jamais un conteneur « unhealthy », seulement un
  conteneur arrêté. Le scheduler figé 35 min au jour 8 serait resté figé
  indéfiniment, et les runs planifiés avec lui.
- **Écarté :** un conteneur tiers « autoheal » piloté par le socket Docker (image
  externe de plus, avec tout pouvoir sur Docker).
- **Choix :** `docker/airflow/autoheal.py`, exécuté comme contrôle de santé
  (bibliothèque standard uniquement). 6 échecs consécutifs (~3 min) après 5 min
  de grâce au démarrage -> `SIGTERM` au processus 1, puis `SIGKILL` des processus
  du service au contrôle suivant ; `restart: unless-stopped` relance le conteneur ;
  alerte Telegram une fois le service de nouveau sain. État dans le volume des
  journaux (survit au redémarrage), compteur remis à zéro à chaque nouvelle vie
  du conteneur.
- **Vérifié en réel :** scheduler gelé (`SIGSTOP`) à 20:04:52 -> `SIGTERM` à
  20:07:53 (sans effet sur un processus gelé, comme prévu) -> `SIGKILL` ->
  redémarré et sain à 20:08:56 : **4 min au lieu de 35**, sans intervention.
- **Non couvert :** le processeur de DAGs (il se termine de lui-même quand il perd
  sa base, il ne s'est pas figé) ; MLflow (non critique pour les runs planifiés).

## D54 — CI GitHub Actions en deux jobs, sans secret ni service (jour 9)

- **`qualite-tests`** : ruff et black sur `src tests dags scripts docker/airflow`,
  puis tous les tests avec couverture (Java 17 pour PySpark, comme l'image app).
  Les tests n'ont besoin d'aucun service : fixtures synthétiques, paramètres
  factices.
- **`dags`** : construit l'image Airflow du projet et lance la vérification des
  DAGs **sans base ni réseau** (`--network none`). Il a fallu lire les DAGs
  analysés (`bag.dags`) au lieu de `bag.get_dag()`, qui interroge la base de
  métadonnées en Airflow 3 : la vérification ne dépend plus d'une base.
- **Couverture :** 63 % ; seuil `--cov-fail-under=60` pour interdire le recul.
  Non couvert : les `main()`/`run()` d'entrées-sorties (MinIO, PostgreSQL, Spark
  réel), vérifiés par les vrais runs ; viser 90 % avec des simulacres de MinIO et
  PostgreSQL testerait surtout les simulacres.
- Permissions minimales (`contents: read`), un push annule la CI encore en cours.

## D55 — Salaires : la couverture accompagne toujours la médiane (jour 9)

- `gold.salaires_par_famille` : par famille et niveau, offres uniques, nombre
  avec salaire (hors anomalies), **taux de couverture**, quartiles en USD.
- 29,5 % des offres du dataset US publient un salaire, 0 % au Maroc : une médiane
  sans sa couverture laisserait croire qu'elle décrit toute la famille.
- Une médiane calculée sur moins de 30 salaires est marquée non fiable
  (`bi.salaires.mediane_fiable`) et masquée par le tableau de bord (exemple
  réel : « Data engineering, directeur », 7 salaires).
- Défaut évité : `run()` ne lisait qu'une projection de Silver sans les colonnes
  de salaire. Trouvé en lançant Gold avant le run planifié de 14:00, qui aurait
  sinon échoué.

## D56 — Couche `bi` et rôle PostgreSQL en lecture seule pour Power BI (jour 9)

- Vues `bi.*` (offres mensuelles, classement des technologies, salaires,
  couverture) : recalculées à la lecture, toujours alignées sur Gold.
- `powerbi_lecteur` (`scripts/creer_lecteur_powerbi.py`) : SELECT sur `gold` et
  `bi` seulement, 5 connexions au plus ; mot de passe généré dans `.env`, jamais
  affiché. Prouvé : suppression, création de table et lecture de `ops` refusées
  (code 42501). Un `.pbix` partagé avec des identifiants mémorisés ne peut donc
  rien détruire.

## D57 — PostgreSQL et MinIO accessibles depuis ce PC seulement (jour 9)

- **Constat :** ports publiés sur `0.0.0.0` : PostgreSQL et MinIO étaient
  joignables depuis tout le réseau local (Wi-Fi public compris), protégés par
  leur seul mot de passe. Airflow et MLflow étaient déjà limités.
- **Choix :** `127.0.0.1` pour tous les ports. Power BI, sur le même PC, se
  connecte par `localhost` ; les conteneurs se parlent par le réseau Docker.

## D58 — Figures du README générées par script ; pas de courbe trompeuse (jour 9)

- Power BI Desktop n'est pas installé sur le PC (et consomme 1 à 2 Go de RAM sur
  une machine qui en manque, D52). Le tableau de bord est **spécifié**
  (`docs/powerbi.md`) et ses données prêtes ; `scripts/figures_readme.py` produit
  des aperçus reproductibles des mêmes vues (palette validée pour le
  daltonisme, typographie française).
- **Pas d'« évolution mensuelle » dans le README :** le dataset Kaggle est un
  instantané (98,5 % des offres en avril 2024) et les dates marocaines sont
  celles d'offres encore ouvertes. Une courbe mensuelle montrerait un artefact
  de collecte, pas une tendance.

## D59 — Adresse e-mail des commits : à trancher avant publication (jour 9)

- L'historique ne contient aucun secret (valeurs réelles de `.env` recherchées
  dans tous les commits : 0 occurrence). Mais l'e-mail personnel de l'auteur
  figure dans les métadonnées des commits et deviendrait public avec le dépôt.
- Option : adresse « noreply » de GitHub et réécriture de l'historique
  (force-push). Décision laissée au propriétaire du dépôt.

## D60 — Docker limité à 4,5 Go ; effet mesuré : aucun gain côté Windows (avant jour 10)

- **Fait :** `memory=4608MB` (au lieu de 5,5 Go) dans `.wslconfig`, à la demande
  du propriétaire du PC. VM : 4 418 Mo ; pile saine ; suite de tests passée (pic
  3,93 Go + 346 Mo de swap dans la VM).
- **Mesuré :** côté Windows, toujours 0,4 Go libres et 1 400 à 2 000 pages lues
  par seconde ; la VM remplit sa nouvelle limite (4,1 Go réservés) et swappe en
  interne ; suite de tests 14 min 51 s (contre 9 min 47 s à 5,5 Go, une seule
  mesure dans chaque cas, charges différentes). La demande mémoire totale de
  Windows (14,4 Go engagés pour 7,8 Go de RAM) dépasse Docker.
- **Conclusion :** réglage conservé, sans gain démontré. Le levier décisif est la
  mémoire des autres applications (ou du matériel) ; l'auto-réparation (D53)
  protège les runs planifiés en attendant.

## D61 — Annotations posées par un modèle de langage, dit tel quel (avant jour 10)

- **Raison :** les deux mesures annoncées (précision de l'extraction des
  technologies, F1 des familles de poste) attendaient des annotations depuis les
  jours 3 et 8 ; le propriétaire du projet a confié l'annotation à Claude.
- **Protocole :** familles : 402 offres, titre et début de description lus, sans
  la prédiction des règles, guide `docs/guide_annotation.md`, 58 cas ambigus
  marqués (`incertain`). Technologies : 100 descriptions complètes lues **sans**
  la colonne extraite, identifiants du référentiel, règle stricte (seules les
  technologies nommées comptent : « shell » n'est pas `bash`, « Azure OpenAI »
  n'est pas `openai_api`).
- **Résultats :** technologies : précision 97,1 %, rappel 92,9 %, F1 95,0 %.
  Familles : règles F1 macro 0,843 (0,904 hors ambigus, 0,891 au Maroc) contre
  0,770 pour TF-IDF + régression logistique ; l'annotateur est tracé dans MLflow.
- **Limites :** pas d'annotateur humain ; un modèle qui lit d'abord le titre,
  comme les règles, peut surestimer l'accord. Les erreurs révélées (classe non
  tech, `HTML5`, `Springboot`, `Lambda`) ne sont pas corrigées sur ce même jeu :
  ce serait régler l'outil sur sa propre épreuve.

## D62 — Tableau de bord web autonome, généré depuis Gold (avant jour 10)

- **Raison :** Power BI n'est pas installé et coûte 1 à 2 Go de RAM ; un fichier
  HTML unique se consulte sans serveur et se publiera avec le dépôt (GitHub
  Pages).
- **Construction :** `scripts/tableau_de_bord.py` lit les vues `bi.*` et les
  tables `gold.*` (environ 20 Ko d'agrégats, jamais d'annonce) et les injecte
  dans `scripts/tableau_de_bord.html` (JSON neutralisé : aucune donnée ne peut
  fermer la balise `<script>`, testé). SVG sans bibliothèque, à l'échelle réelle
  du conteneur ; palette validée daltonisme dans les deux thèmes ; info-bulles
  au survol et au clavier ; données de chaque graphique en tableau.
- **Règles :** une source à la fois ; couverture des salaires affichée en tête ;
  médiane masquée sous 30 salaires ; pas de courbe mensuelle (D58).

## D63 — Publication : dépôt public, GitHub Pages, licence MIT (jour 10)

- **E-mail des commits (suite de D59) :** le propriétaire a choisi de le laisser
  visible ; l'historique n'est pas réécrit.
- **Dépôt public** après un audit du contenu : aucun secret (valeurs réelles de
  `.env` recherchées dans tout l'historique), aucune annonce (données de test
  fictives, `data/` exclu), aucune donnée personnelle hors le nom du compte et
  l'e-mail d'auteur.
- **GitHub Pages** sur `main` / `docs` : le tableau de bord web est en ligne.
  `docs/.nojekyll` (fichiers servis tels quels) et `docs/index.html` (redirige
  vers le tableau de bord, sinon la racine du site serait une page 404).
- **Licence MIT** : un projet de portfolio se lit et se réutilise ; le dataset
  Kaggle n'est pas redistribué (seuls des agrégats le sont).
- **README réécrit pour une lecture en 90 secondes** (présentation, schéma,
  chiffres, décisions, incidents, limites) ; le détail technique est passé dans
  `docs/fonctionnement.md` sans perte.
