-- Schéma PostgreSQL du pipeline. Idempotent (IF NOT EXISTS) : appliqué au début de
-- chaque job qui écrit en base, donc aussi sur un volume déjà initialisé (init.sql ne
-- s'exécute qu'à la création du volume).

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_staging;  -- tables écrites par Spark avant bascule
CREATE SCHEMA IF NOT EXISTS ops;

-- Résultat de chaque contrôle qualité de chaque exécution du job Silver
CREATE TABLE IF NOT EXISTS ops.controles_qualite (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    date_debut      DATE        NOT NULL,
    date_fin        DATE        NOT NULL,
    execute_le      TIMESTAMPTZ NOT NULL DEFAULT now(),
    suite           TEXT        NOT NULL,   -- critique | avertissement
    controle        TEXT        NOT NULL,
    colonne         TEXT,
    succes          BOOLEAN     NOT NULL,
    nb_inattendus   BIGINT,
    pct_inattendus  DOUBLE PRECISION,
    valeur_observee TEXT,
    exemples        TEXT
);

-- Les tables Gold portent toutes `source` : une source marocaine et le dataset
-- américain ne doivent jamais être agrégés ensemble.
-- nb_offres   = offres uniques (hors republications, voir D12)
-- nb_annonces = toutes les annonces publiées

CREATE TABLE IF NOT EXISTS gold.tech_par_mois (
    source        TEXT    NOT NULL,
    perimetre     TEXT    NOT NULL,   -- toutes | tech (famille_poste != AUTRE)
    mois          DATE    NOT NULL,   -- premier jour du mois de publication
    technologie   TEXT    NOT NULL,
    nb_offres     INTEGER NOT NULL,
    nb_annonces   INTEGER NOT NULL,
    part_relative DOUBLE PRECISION NOT NULL,  -- nb_offres / offres du périmètre ce mois
    PRIMARY KEY (source, perimetre, mois, technologie)
);

CREATE TABLE IF NOT EXISTS gold.offres_par_ville (
    source        TEXT    NOT NULL,
    mois          DATE    NOT NULL,
    ville         TEXT    NOT NULL,   -- 'Non précisée' si inconnue
    etat_code     TEXT    NOT NULL,   -- 'NA' si inconnu
    famille_poste TEXT    NOT NULL,
    nb_offres     INTEGER NOT NULL,
    nb_annonces   INTEGER NOT NULL,
    PRIMARY KEY (source, mois, ville, etat_code, famille_poste)
);

CREATE TABLE IF NOT EXISTS gold.experience_demandee (
    source        TEXT    NOT NULL,
    famille_poste TEXT    NOT NULL,
    tranche       TEXT    NOT NULL,
    ordre_tranche INTEGER NOT NULL,   -- pour trier les tranches dans Power BI
    nb_offres     INTEGER NOT NULL,
    nb_annonces   INTEGER NOT NULL,
    part_famille  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (source, famille_poste, tranche)
);

CREATE TABLE IF NOT EXISTS gold.cooccurrence_tech (
    source             TEXT    NOT NULL,
    tech_a             TEXT    NOT NULL,  -- tech_a < tech_b : chaque paire une fois
    tech_b             TEXT    NOT NULL,
    nb_offres_communes INTEGER NOT NULL,
    nb_offres_a        INTEGER NOT NULL,
    nb_offres_b        INTEGER NOT NULL,
    jaccard            DOUBLE PRECISION NOT NULL,  -- communes / (a + b - communes)
    lift               DOUBLE PRECISION NOT NULL,  -- > 1 : associées plus que le hasard
    PRIMARY KEY (source, tech_a, tech_b),
    -- COLLATE "C" = ordre des octets, celui de Spark. Avec la collation linguistique
    -- par défaut (en_US.utf8), PostgreSQL ignore presque "_" : 't_sql' < 'terraform'
    -- pour Spark mais pas pour PostgreSQL (rencontré au jour 5).
    CONSTRAINT cooccurrence_tech_ordre CHECK (tech_a COLLATE "C" < tech_b COLLATE "C")
);

-- Migration des bases créées avant cette correction (idempotente)
ALTER TABLE gold.cooccurrence_tech DROP CONSTRAINT IF EXISTS cooccurrence_tech_check;
ALTER TABLE gold.cooccurrence_tech DROP CONSTRAINT IF EXISTS cooccurrence_tech_ordre;
ALTER TABLE gold.cooccurrence_tech
    ADD CONSTRAINT cooccurrence_tech_ordre CHECK (tech_a COLLATE "C" < tech_b COLLATE "C");

-- Couverture et distribution des salaires publiés (jour 9). Le taux de couverture
-- accompagne TOUJOURS les salaires dans le tableau de bord : une médiane calculée
-- sur 3 % des offres ne décrit pas toute la famille.
CREATE TABLE IF NOT EXISTS gold.salaires_par_famille (
    source             TEXT    NOT NULL,
    famille_poste      TEXT    NOT NULL,
    niveau_experience  TEXT    NOT NULL,   -- 'Non précisé' si inconnu
    nb_offres          INTEGER NOT NULL,   -- offres uniques
    nb_avec_salaire    INTEGER NOT NULL,   -- salaire publié, hors anomalies
    taux_couverture    DOUBLE PRECISION NOT NULL,
    nb_salaires_usd    INTEGER NOT NULL,   -- base des quartiles ci-dessous
    salaire_p25_usd    DOUBLE PRECISION,   -- NULL si aucun salaire en USD
    salaire_median_usd DOUBLE PRECISION,
    salaire_p75_usd    DOUBLE PRECISION,
    PRIMARY KEY (source, famille_poste, niveau_experience)
);

-- Effet associé de chaque technologie sur le salaire (modèle ML, jour 8).
-- SHAP moyen sur les offres qui citent la technologie, à profil comparable :
-- une ASSOCIATION, pas un effet causal. Dataset US : salaires en USD.
CREATE TABLE IF NOT EXISTS gold.prime_salariale_tech (
    source         TEXT    NOT NULL,
    technologie    TEXT    NOT NULL,
    nb_offres      INTEGER NOT NULL,   -- offres d'entraînement citant la technologie
    effet_pct      DOUBLE PRECISION NOT NULL,  -- +10 = salaire estimé ~10 % plus élevé
    shap_moyen_log DOUBLE PRECISION NOT NULL,
    mlflow_run_id  TEXT    NOT NULL,   -- traçabilité : quel modèle a produit ces chiffres
    PRIMARY KEY (source, technologie)
);

-- Couche de lecture du tableau de bord (jour 9) : vues sur gold.*, recalculées à la
-- lecture, donc toujours alignées sur la dernière publication Gold. Power BI se
-- connecte avec le rôle en lecture seule `powerbi_lecteur`
-- (scripts/creer_lecteur_powerbi.py), jamais avec le compte administrateur.
CREATE SCHEMA IF NOT EXISTS bi;

-- Page 1 : volume d'offres par mois et famille
CREATE OR REPLACE VIEW bi.offres_mensuelles AS
SELECT source, mois, famille_poste,
       SUM(nb_offres)::INTEGER AS nb_offres, SUM(nb_annonces)::INTEGER AS nb_annonces
FROM gold.offres_par_ville
GROUP BY source, mois, famille_poste;

-- Page 2 : classement des technologies sur toute la période (offres tech)
CREATE OR REPLACE VIEW bi.tech_classement AS
WITH totaux AS (
    SELECT source, SUM(nb_offres) AS offres_tech
    FROM bi.offres_mensuelles WHERE famille_poste <> 'AUTRE' GROUP BY source
)
SELECT t.source, t.technologie, SUM(t.nb_offres)::INTEGER AS nb_offres,
       SUM(t.nb_offres)::DOUBLE PRECISION / NULLIF(MAX(o.offres_tech), 0) AS part_offres_tech
FROM gold.tech_par_mois t JOIN totaux o USING (source)
WHERE t.perimetre = 'tech'
GROUP BY t.source, t.technologie;

-- Page 3 : la couverture accompagne toujours les salaires ; une médiane calculée
-- sur moins de 30 salaires est marquée non fiable (le visuel la masque)
CREATE OR REPLACE VIEW bi.salaires AS
SELECT s.*, s.nb_salaires_usd >= 30 AS mediane_fiable
FROM gold.salaires_par_famille s;

CREATE OR REPLACE VIEW bi.couverture_salaires AS
SELECT source, SUM(nb_offres)::INTEGER AS nb_offres,
       SUM(nb_avec_salaire)::INTEGER AS nb_avec_salaire,
       SUM(nb_avec_salaire)::DOUBLE PRECISION / NULLIF(SUM(nb_offres), 0) AS taux_couverture
FROM gold.salaires_par_famille
GROUP BY source;
