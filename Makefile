PYTHON := .venv/bin/python

.PHONY: setup format-check lint test infra-up infra-down migrate ingest backfill reconcile integration-test build-silver silver-integration-test build-gold gold-integration-test notebook-check eda

COMPOSE := docker compose --project-name financial-ts-phase2 --env-file .env -f infrastructure/docker-compose.yml

setup:
	python3.12 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e '.[dev]' -c requirements.lock

format-check:
	$(PYTHON) -m ruff format --check .

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest -m 'not integration'

infra-up:
	$(COMPOSE) build minio
	$(COMPOSE) up -d --wait postgres minio
	$(COMPOSE) run --rm minio-init

infra-down:
	$(COMPOSE) down

migrate:
	$(PYTHON) scripts/apply_migrations.py

ingest:
	$(PYTHON) -m financial_time_series.cli.ingest $(ARGS)

backfill:
	$(PYTHON) -m financial_time_series.cli.backfill $(ARGS)

reconcile:
	$(PYTHON) -m financial_time_series.cli.reconcile $(ARGS)

integration-test:
	$(PYTHON) -m pytest -m integration --tb=line

build-silver:
	$(PYTHON) -m financial_time_series.cli.build_silver --dataset $(DATASET) $(if $(BUILD_CUTOFF_AT),--build-cutoff-at $(BUILD_CUTOFF_AT),) $(ARGS)

silver-integration-test:
	$(PYTHON) -m pytest tests/integration/test_bronze_to_silver.py --tb=line

build-gold:
	$(PYTHON) -m financial_time_series.cli.build_gold --dataset $(DATASET) --market-version $(MARKET_VERSION) --macro-version $(MACRO_VERSION) $(if $(FEATURES_VERSION),--features-version $(FEATURES_VERSION),) --build-cutoff-at $(BUILD_CUTOFF_AT)

gold-integration-test:
	$(PYTHON) -m pytest tests/integration/test_silver_to_gold.py --tb=line

notebook-check:
	$(PYTHON) -m financial_time_series.cli.eda --check

eda:
	$(PYTHON) -m financial_time_series.cli.eda $(ARGS)
