# Financial Time Series Prediction Platform

Research and paper-trading foundation for daily market and macroeconomic data.
Phase 1 establishes the package, configuration, data contract and deterministic
fixtures. It contains no live ingestion, storage service, model or trading code.

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

## Architecture and scope

The intended pipeline has Bronze source snapshots, Silver validated records,
and Gold point-in-time feature and target datasets. The package directories
reserve boundaries for ingestion, storage, processing, features, models,
evaluation, training, serving and monitoring. Their implementations belong to
later phases. See [data dictionary](documentation/data_dictionary.md) and
[source policy](documentation/data_sources.md).

Research output is not investment advice and does not guarantee returns.
