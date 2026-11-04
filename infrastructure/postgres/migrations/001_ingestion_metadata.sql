CREATE TABLE data_sources (
    source_name TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    base_url TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE assets (
    asset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    timezone TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ticker, exchange)
);

CREATE TABLE ingestion_runs (
    ingestion_run_id UUID PRIMARY KEY,
    source_name TEXT NOT NULL REFERENCES data_sources(source_name),
    entity_key TEXT NOT NULL,
    interval_name TEXT NOT NULL,
    request_parameters JSONB NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('pending','success','failed','no_data','abandoned')),
    error_class TEXT,
    error_message TEXT,
    payload_uri TEXT,
    sha256 CHAR(64),
    byte_count BIGINT CHECK (byte_count >= 0),
    row_count BIGINT CHECK (row_count >= 0),
    schema_version TEXT NOT NULL,
    code_revision TEXT NOT NULL,
    backfill BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX ingestion_runs_entity_idx
    ON ingestion_runs (source_name, entity_key, interval_name, requested_at DESC);

CREATE TABLE ingestion_watermarks (
    source_name TEXT NOT NULL REFERENCES data_sources(source_name),
    entity_key TEXT NOT NULL,
    interval_name TEXT NOT NULL,
    max_observation_date DATE NOT NULL,
    last_advancing_run_id UUID NOT NULL REFERENCES ingestion_runs(ingestion_run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (source_name, entity_key, interval_name)
);
