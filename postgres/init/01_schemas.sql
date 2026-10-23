CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.osha_inspections (
    activity_nr  BIGINT      PRIMARY KEY,
    payload      JSONB       NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.osha_violations (
    activity_nr  BIGINT      NOT NULL,
    citation_id  TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_nr, citation_id)
);

CREATE INDEX IF NOT EXISTS idx_osha_insp_open_date
    ON raw.osha_inspections (((payload ->> 'open_date')));
SELECT 'CREATE DATABASE metabase' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
