# Guide d'annotation — familles de poste

Fichier : `data/annotation/familles.csv` (non versionné : il contient des annonces).
Remplir la colonne **`famille_reelle`** avec **un seul** des 6 codes ci-dessous, en
lisant le titre **et** le début de la description. Une ligne laissée vide est
ignorée : on peut annoter en plusieurs fois.

| Code | Le cœur du poste est… | Exemples |
|---|---|---|
| `DATA_ENG` | construire et faire tourner les données : pipelines, ETL, entrepôts, plateformes data, bases de données | Data Engineer, ETL Developer, Architecte Data, DBA, Analytics Engineer |
| `DATA_SCI` | analyser et modéliser statistiquement, expérimenter | Data Scientist, Statisticien, Quantitative Analyst |
| `AI_ML` | concevoir ou déployer des modèles d'apprentissage automatique ou d'IA | ML Engineer, AI Engineer, MLOps, ingénieur NLP / vision |
| `BI` | restituer les données pour la décision : tableaux de bord, reporting | Data Analyst, BI Developer, Consultant décisionnel |
| `DEV` | développer ou exploiter du logiciel (hors données) | Software Engineer, Développeur Full Stack, DevOps, SRE, Tech Lead |
| `AUTRE` | tout le reste, même si l'offre cite des outils techniques | Commercial, RH, infirmier, chef de produit, support client, annotateur de données |

Règles de décision :

1. **Le métier, pas les outils.** Un commercial qui vend une solution cloud reste
   `AUTRE`.
2. **En cas d'hésitation entre deux familles, choisir celle de la majorité des
   missions décrites.** Un « Data Scientist » qui construit surtout des pipelines est
   `DATA_ENG`.
3. **`DATA_SCI` ou `AI_ML`** : modèles en production, MLOps, LLM -> `AI_ML` ;
   analyse, statistiques, expérimentation -> `DATA_SCI`.
4. **Les managers suivent leur équipe** (Data Engineering Manager -> `DATA_ENG`),
   sauf si le poste est purement commercial ou RH.
5. **Annoter sans chercher à deviner ce que diraient les règles** : le fichier ne
   montre volontairement pas leur prédiction.

Comptez environ 2 à 3 heures pour 400 offres. À partir de 150 offres annotées,
`python -m src.ml.classification evaluer` fonctionne déjà.
