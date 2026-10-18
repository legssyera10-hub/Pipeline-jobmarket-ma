# Tableau de bord Power BI — guide de construction

Le tableau de bord lit **uniquement** les agrégats Gold (schéma `gold`) et les vues
préparées pour lui (schéma `bi`), jamais les annonces. Aperçu des mêmes données :
figures de `docs/img/`, régénérées par `python -m scripts.figures_readme`.

## 1. Connexion (une fois)

Prérequis : la pile tourne (`docker compose up -d`) et le compte en lecture seule
existe :

```bash
docker compose run --rm --no-deps app python -m scripts.creer_lecteur_powerbi
```

Dans Power BI Desktop : **Obtenir les données → Base de données PostgreSQL**

| Champ | Valeur |
|---|---|
| Serveur | `localhost:5432` |
| Base de données | `jobmarket` |
| Mode | **Importer** (quelques milliers de lignes ; le rafraîchissement relit Gold) |
| Authentification | Base de données ; utilisateur `powerbi_lecteur`, mot de passe `POWERBI_PASSWORD` de `.env` |

Ne jamais utiliser le compte administrateur : `powerbi_lecteur` ne peut que lire
`gold` et `bi` (vérifié : suppression, création de table et lecture de `ops`
refusées).

Tables à charger :

| Table / vue | Page |
|---|---|
| `bi.offres_mensuelles` | 1 |
| `gold.offres_par_ville` | 1 |
| `bi.tech_classement`, `gold.tech_par_mois`, `gold.cooccurrence_tech` | 2 |
| `bi.salaires`, `bi.couverture_salaires`, `gold.experience_demandee`, `gold.prime_salariale_tech` | 3 |

Pas de relations à créer : chaque visuel lit une seule table. **Toutes portent la
colonne `source`.**

## 2. Règle n° 1 : une source à la fois

Ajouter sur chaque page un segment **`source`** en **sélection unique**
(`kaggle_linkedin` par défaut). Le dataset américain (115 573 offres) et la source
marocaine (37 offres) ne doivent jamais être additionnés : ce ne sont ni les mêmes
pays, ni les mêmes périodes, ni les mêmes volumes.

## 3. Mesures DAX

```dax
Offres = SUM ( bi_offres_mensuelles[nb_offres] )

Offres tech =
CALCULATE ( [Offres], bi_offres_mensuelles[famille_poste] <> "AUTRE" )

Couverture salaires =
DIVIDE ( SUM ( bi_couverture_salaires[nb_avec_salaire] ),
         SUM ( bi_couverture_salaires[nb_offres] ) )

Salaire médian (fiable) =
VAR m = MAX ( bi_salaires[salaire_median_usd] )
RETURN IF ( MAX ( bi_salaires[nb_salaires_usd] ) >= 30, m, BLANK () )
```

(Power BI nomme les tables `bi_offres_mensuelles`, etc. à l'import.)

## 4. Les trois pages

### Page 1 — Vue d'ensemble
- Cartes : **Offres**, **Offres tech**, part tech (`DIVIDE([Offres tech], [Offres])`).
- Barres horizontales : offres par `famille_poste` (hors AUTRE).
- Barres horizontales : 10 premières villes (`gold.offres_par_ville`, filtre
  `ville <> "Non précisée"`) ; afficher à côté le nombre d'offres sans ville.
- **Évolution mensuelle** (courbe sur `mois`) : à n'interpréter qu'avec
  prudence. Le dataset Kaggle est un instantané (98,5 % des offres en avril 2024)
  et les dates marocaines sont celles d'offres encore ouvertes. La courbe aura du
  sens quand la collecte quotidienne aura accumulé plusieurs mois.

### Page 2 — Technologies
- Barres : `bi.tech_classement[part_offres_tech]`, 15 premières.
- Courbe : `gold.tech_par_mois[part_relative]`, `perimetre = "tech"` (même réserve
  sur la période).
- Matrice de co-occurrence : `gold.cooccurrence_tech`, lignes `tech_a`, colonnes
  `tech_b`, valeur `lift` (> 1 : associées plus que le hasard), mise en forme
  conditionnelle en dégradé d'une seule teinte ; filtre `nb_offres_communes >= 30`
  pour écarter les paires anecdotiques.

### Page 3 — Salaires et expérience
- **Carte « Couverture salaires » en tête de page, toujours visible** (29,5 % des
  offres du dataset US ; 0 % pour la source marocaine, qui n'affiche donc pas de
  salaire).
- Tableau ou barres : **Salaire médian (fiable)** par famille et niveau, avec
  `salaire_p25_usd`, `salaire_p75_usd` et `nb_salaires_usd` en info-bulle. Une
  médiane sur moins de 30 salaires est masquée par la mesure.
- Barres empilées 100 % : `gold.experience_demandee`, `part_famille` par `tranche`
  (trier par `ordre_tranche`).
- Barres divergentes : `gold.prime_salariale_tech[effet_pct]`, sous-titre
  obligatoire : « association à profil comparable, pas un effet causal ».

## 5. Captures pour le README

Exporter chaque page en PNG (Fichier → Exporter → PDF, ou capture) dans
`docs/img/powerbi_page1.png`, `..._page2.png`, `..._page3.png`.
