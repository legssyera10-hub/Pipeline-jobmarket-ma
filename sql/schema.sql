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
