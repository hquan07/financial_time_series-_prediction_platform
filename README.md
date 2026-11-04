# Financial Time Series Prediction Platform

Research and paper-trading foundation for daily market and macroeconomic data.
Phase 2 ingests Yahoo daily OHLCV and FRED observations into immutable MinIO
Bronze objects with PostgreSQL run audit and watermarks. Phase 3 validates
successful Bronze runs and publishes versioned Parquet Silver datasets with
quality reports and PostgreSQL lineage. Gold processing, models and trading
remain later-phase work.

## Local setup

Requires Python 3.12. From the repository root:

```bash
make setup
.venv/bin/python -c "import financial_time_series; print(financial_time_series.__version__)"
make format-check lint test
```

`requirements.lock` pins the resolved Python dependencies. Recreate it only
after reviewing dependency changes. `.env.example` lists future connection
settings; do not put credentials into tracked files.

## Phase 2 local ingestion

Copy `.env.example` to ignored `.env`, supply project-only MinIO/PostgreSQL
credentials, and restrict the file to your user (`chmod 600 .env`). A FRED API
key is needed only for live FRED ingestion; deterministic tests mock providers.
The Compose project is `financial-ts-phase2` and binds only to localhost:
MinIO API `19000`, console `19001`, PostgreSQL `15432`. It uses its own named
volumes and never reuses CloudVault services or data.

```bash
make setup
make format-check lint test
make infra-up
make migrate
make integration-test
make backfill ARGS='--source yahoo_finance --entity AAPL --start 2024-01-01 --end 2024-01-05'
make backfill ARGS='--source fred --entity VIXCLS --start 2024-01-01 --end 2024-01-05'
make ingest ARGS='--source fred --entity DGS10'
make infra-down
```

`make ingest ARGS='--all'` processes the configured universe. Backfill bounds
are inclusive and required; requests are split into at most 366-day chunks.
`make infra-down` retains volumes. A stale pending run can be examined and
explicitly marked abandoned after the entity lock is free:

```bash
make reconcile ARGS='--run-id UUID --source fred --entity VIXCLS --older-than-minutes 60'
```

The local MinIO image is built from pinned upstream source by
`infrastructure/minio/Dockerfile`; see [infrastructure notes](infrastructure/README.md)
for image availability and setup. Integration tests use a unique temporary
database and bucket, and clean up only those test resources.

## Phase 3 local Silver builds

Phase 3 reads successful Bronze runs already stored in the project bucket. It
does not call Yahoo or FRED. Run builds from a clean Git revision after applying
the additive migration:

```bash
make infra-up
make migrate
make integration-test
make build-silver DATASET=market BUILD_CUTOFF_AT=2026-11-04T12:00:00Z
make build-silver DATASET=macro BUILD_CUTOFF_AT=2026-11-04T12:00:00Z
make infra-down
```

Omit `BUILD_CUTOFF_AT` to use the current UTC instant; the CLI prints the exact
cutoff and resulting version. Pass `ARGS='--source-run-id UUID --source-run-id
UUID'` to build from a bounded, explicit list of eligible runs. A repeat build
with the same cutoff, inputs and code revision verifies and returns an existing
version. A broken successful Bronze run blocks publication. Fatal attempts
leave a report under `quality/phase_03/` and do not publish a Silver manifest.
`make silver-integration-test` uses isolated synthetic database and bucket resources.
The reader accepts both Phase 2 Bronze v1 manifest shapes: explicit
`requested_observation_range` and the earlier inclusive `start`/`end` request
parameters. It verifies the range without changing Bronze objects.

## Architecture and scope

The intended pipeline has Bronze source snapshots, Silver validated records,
and Gold point-in-time feature and target datasets. The package directories
reserve boundaries for processing, features, models, evaluation, training,
serving and monitoring. Their implementations belong to later phases. See
[data dictionary](documentation/data_dictionary.md) and
[source policy](documentation/data_sources.md).
Branch conventions for each phase are in
[development workflow](documentation/development_workflow.md).

Research output is not investment advice and does not guarantee returns.
