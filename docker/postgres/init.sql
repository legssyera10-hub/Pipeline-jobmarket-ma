-- Exécuté une seule fois, à la création du volume PostgreSQL.
-- gold : tables analytiques consommées par Power BI.
-- ops  : métadonnées d'exécution du pipeline (runs, contrôles qualité).
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id       BIGSERIAL PRIMARY KEY,
    dag_id       TEXT        NOT NULL,
    run_date     DATE        NOT NULL,
    step         TEXT        NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_in      BIGINT,
    rows_out     BIGINT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    message      TEXT
);
