"""Real local PostgreSQL/MinIO, deterministic provider boundaries."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import psycopg
import pytest
from psycopg import sql

from financial_time_series.ingestion.fred_client import FredClient
from financial_time_series.ingestion.models import DateRange, FetchResult
from financial_time_series.ingestion.service import IngestionService, resolve_entity
from financial_time_series.ingestion.yahoo_finance import _export
from financial_time_series.settings import Settings
from financial_time_series.storage.bronze import BronzePublisher, object_prefix
from financial_time_series.storage.minio_client import MinioStore
from financial_time_series.storage.postgres_metadata import MetadataStore
from scripts.apply_migrations import apply_migrations

pytestmark = pytest.mark.integration
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class YahooFixture:
    def fetch(self, ticker, bounds):
        frame = pd.read_csv(
            FIXTURES / "yahoo_aapl_download.csv", index_col="Date", parse_dates=True
        )
        frame = frame.loc[bounds.start.isoformat() : bounds.end.isoformat()]
        return _export(frame, ticker, bounds)


class FredResponse:
    status_code = 200
    headers = {}

    def __init__(self, body):
        self.content = body


class FredSession:
    def get(self, _url, params, timeout):
        assert timeout == 10
        assert params["api_key"] == "fixture-key"
        name = (
            "fred_vixcls_response.json"
            if params["series_id"] == "VIXCLS"
            else "fred_dgs10_response.json"
        )
        document = json.loads((FIXTURES / name).read_bytes())
        document["observations"] = [
            row
            for row in document["observations"]
            if params["observation_start"] <= row["date"] <= params["observation_end"]
        ]
        document["count"] = len(document["observations"])
        return FredResponse(json.dumps(document).encode())


@pytest.fixture(scope="module")
def isolated_services():
    base = Settings()
    base.require_storage()
    suffix = uuid4().hex[:12]
    db_name = f"fts_phase2_test_{suffix}"
    bucket_name = f"fts-phase2-test-{suffix}"
    admin = base.postgres_kwargs()
    admin["dbname"] = "postgres"
    with psycopg.connect(**admin, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
        )
    settings = Settings(postgres_db=db_name, minio_bucket=bucket_name)
    store = MinioStore(settings)
    store.client.make_bucket(bucket_name)
    try:
        apply_migrations(settings)
        yield settings, store
    finally:
        for item in store.client.list_objects(bucket_name, recursive=True):
            store.client.remove_object(bucket_name, item.object_name)
        store.client.remove_bucket(bucket_name)
        with psycopg.connect(**admin, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name))
            )


def _service(settings, store):
    return IngestionService(
        settings,
        metadata=MetadataStore(settings),
        publisher=BronzePublisher(store),
        yahoo=YahooFixture(),
        fred=FredClient("fixture-key", FredSession()),
    )


def _version(settings, source, entity):
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        return connection.execute(
            """SELECT version FROM ingestion_watermarks
               WHERE source_name=%s AND entity_key=%s AND interval_name='1d'""",
            (source, entity),
        ).fetchone()[0]


def test_three_sources_publish_and_same_range_does_not_move_watermark(
    isolated_services,
):
    settings, store = isolated_services
    service = _service(settings, store)
    bounds = DateRange(date(2024, 1, 1), date(2024, 1, 5))
    for source, key, expected in (
        ("yahoo_finance", "AAPL", date(2024, 1, 5)),
        ("fred", "VIXCLS", date(2024, 1, 5)),
        ("fred", "DGS10", date(2024, 1, 5)),
    ):
        entity = resolve_entity(source, key)
        first = service.ingest(entity, backfill=True, bounds=bounds)[0]
        run_id = UUID(first["ingestion_run_id"])
        assert first["status"] == "success"
        assert service.metadata.watermark(entity) == expected
        assert _version(settings, source, key) == 1
        assert service.verify_consumable(run_id, entity)
        manifest = json.loads(
            store.read(f"{object_prefix(entity, run_id)}/manifest.json")
        )
        assert manifest["payload"]["row_count"] == first["row_count"]
        assert "api_key" not in json.dumps(manifest)
        with psycopg.connect(**settings.postgres_kwargs()) as connection:
            request_parameters = connection.execute(
                "SELECT request_parameters FROM ingestion_runs WHERE ingestion_run_id=%s",
                (run_id,),
            ).fetchone()[0]
        assert request_parameters == manifest["request_parameters"]
        assert "api_key" not in json.dumps(request_parameters)
        if source == "fred":
            assert request_parameters["series_id"] == key
            assert request_parameters["file_type"] == "json"
        else:
            assert request_parameters["end_exclusive"] is True
            assert request_parameters["end"] == "2024-01-06"
        again = service.ingest(entity, backfill=True, bounds=bounds)[0]
        assert again["ingestion_run_id"] != first["ingestion_run_id"]
        assert _version(settings, source, key) == 1
        older = service.ingest(
            entity,
            backfill=True,
            bounds=DateRange(date(2024, 1, 1), date(2024, 1, 4)),
        )[0]
        assert older["status"] == "success"
        assert _version(settings, source, key) == 1


def test_failed_commit_leaves_unconsumable_orphan_and_no_data_keeps_state(
    isolated_services, monkeypatch
):
    settings, store = isolated_services
    service = _service(settings, store)
    entity = resolve_entity("fred", "VIXCLS")
    before = service.metadata.watermark(entity)
    real_publish = service.publisher.publish
    published_ids = []

    def publish(*args, **kwargs):
        published_ids.append(args[0])
        return real_publish(*args, **kwargs)

    def fail_commit(*_args, **_kwargs):
        raise psycopg.OperationalError("injected")

    monkeypatch.setattr(service.publisher, "publish", publish)
    monkeypatch.setattr(service.metadata, "finish_success", fail_commit)
    with pytest.raises(RuntimeError, match="OperationalError"):
        service.ingest(
            entity, backfill=True, bounds=DateRange(date(2024, 1, 1), date(2024, 1, 5))
        )
    orphan = published_ids[0]
    assert store.exists(f"{object_prefix(entity, orphan)}/manifest.json")
    assert service.metadata.run(orphan)["status"] == "failed"
    assert not service.verify_consumable(orphan, entity)
    assert service.metadata.watermark(entity) == before

    class EmptyFred:
        def fetch(self, _key, _bounds):
            return FetchResult(
                b"",
                0,
                None,
                "json",
                "json_http_body",
                "fred_observations_v1",
                "fred_api_v1",
            )

    empty = _service(settings, store)
    empty.fred = EmptyFred()
    summary = empty.ingest(
        entity, backfill=True, bounds=DateRange(date(2024, 1, 1), date(2024, 1, 5))
    )[0]
    assert summary["status"] == "no_data"
    assert empty.metadata.watermark(entity) == before


def test_store_failure_leaves_failed_audit_and_no_progress(
    isolated_services, monkeypatch
):
    settings, store = isolated_services
    service = _service(settings, store)
    entity = resolve_entity("fred", "DGS10")
    before = service.metadata.watermark(entity)

    def fail_store(*_args, **_kwargs):
        raise OSError("injected object failure")

    monkeypatch.setattr(service.publisher, "publish", fail_store)
    with pytest.raises(RuntimeError, match="OSError"):
        service.ingest(
            entity, backfill=True, bounds=DateRange(date(2024, 1, 1), date(2024, 1, 5))
        )
    assert service.metadata.watermark(entity) == before
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        assert (
            connection.execute(
                """SELECT count(*) FROM ingestion_runs
                   WHERE source_name='fred' AND entity_key='DGS10'
                     AND status='failed'"""
            ).fetchone()[0]
            == 1
        )


def test_incremental_starts_at_configured_date_and_repeats_overlap(isolated_services):
    settings, store = isolated_services
    service = _service(settings, store)
    entity = resolve_entity("yahoo_finance", "MSFT")
    now = datetime(2024, 1, 8, 12, tzinfo=UTC)
    first = service.ingest(entity, now=now)[0]
    assert first["status"] == "success"
    assert (first["start"], first["end"]) == ("2024-01-01", "2024-01-07")
    assert service.metadata.watermark(entity) == date(2024, 1, 5)
    second = service.ingest(entity, now=now)[0]
    assert second["status"] == "success"
    assert second["start"] == "2024-01-01"
    assert _version(settings, "yahoo_finance", "MSFT") == 1


def test_same_entity_workers_are_serialized_and_stale_pending_can_be_abandoned(
    isolated_services,
):
    settings, store = isolated_services
    active = 0
    max_active = 0
    guard = threading.Lock()

    class SlowYahoo(YahooFixture):
        def fetch(self, ticker, bounds):
            nonlocal active, max_active
            with guard:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.15)
                return super().fetch(ticker, bounds)
            finally:
                with guard:
                    active -= 1

    def run_one():
        service = _service(settings, store)
        service.yahoo = SlowYahoo()
        return service.ingest(
            resolve_entity("yahoo_finance", "AAPL"),
            backfill=True,
            bounds=DateRange(date(2024, 1, 1), date(2024, 1, 5)),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_one) for _ in range(2)]
        assert all(future.result()[0]["status"] == "success" for future in futures)
    assert max_active == 1

    metadata = MetadataStore(settings)
    entity = resolve_entity("yahoo_finance", "AAPL")
    pending_id = uuid4()
    metadata.insert_pending(
        pending_id,
        entity,
        {"start": "2024-01-01", "end": "2024-01-05"},
        datetime.now(UTC),
        "yahoo_daily_v1",
        "test",
        True,
    )
    assert metadata.abandon_stale(pending_id, entity, timedelta(seconds=0))
    assert metadata.run(pending_id)["status"] == "abandoned"
