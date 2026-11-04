CREATE TABLE dataset_versions (
    dataset_id UUID PRIMARY KEY,
    layer TEXT NOT NULL CHECK (layer = 'silver'),
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    build_cutoff_at TIMESTAMPTZ NOT NULL,
    manifest_uri TEXT NOT NULL,
    manifest_sha256 CHAR(64) NOT NULL,
    code_revision TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    status TEXT NOT NULL CHECK (status = 'success'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (layer, dataset_name, dataset_version)
);

CREATE TABLE dataset_source_runs (
    dataset_id UUID NOT NULL REFERENCES dataset_versions(dataset_id),
    source_run_id UUID NOT NULL REFERENCES ingestion_runs(ingestion_run_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    input_row_count BIGINT NOT NULL CHECK (input_row_count > 0),
    output_row_count BIGINT NOT NULL CHECK (output_row_count >= 0),
    PRIMARY KEY (dataset_id, source_run_id),
    UNIQUE (dataset_id, ordinal)
);

CREATE TABLE quality_runs (
    quality_run_id UUID PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    dataset_version TEXT,
    dataset_id UUID REFERENCES dataset_versions(dataset_id),
    build_cutoff_at TIMESTAMPTZ NOT NULL,
    rule_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
    fatal_count INTEGER NOT NULL DEFAULT 0 CHECK (fatal_count >= 0),
    warning_count INTEGER NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
    report_uri TEXT,
    report_sha256 CHAR(64),
    failure_summary TEXT,
    diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (status = 'pending' OR completed_at IS NOT NULL)
);
