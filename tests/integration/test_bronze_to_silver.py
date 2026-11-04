"""Synthetic Bronze runs through real local PostgreSQL and MinIO."""

import json
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pyarrow.parquet as pq
import pytest
from psycopg import sql

from financial_time_series.ingestion.models import DateRange, FetchResult
from financial_time_series.ingestion.service import IngestionService, resolve_entity
from financial_time_series.processing.quality import sha256
from financial_time_series.processing.silver_service import SilverService
from financial_time_series.settings import Settings
from financial_time_series.storage.bronze import BronzePublisher
from financial_time_series.storage.minio_client import MinioStore
from financial_time_series.storage.postgres_metadata import MetadataStore
from scripts.apply_migrations import apply_migrations

pytestmark = pytest.mark.integration
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
BOUNDS = DateRange(date(2024, 1, 1), date(2024, 1, 5))


class YahooFixture:
    def __init__(self, payload: bytes):
        self.payload = payload

    def fetch(self, _ticker, _bounds):
        return FetchResult(
            self.payload,
            4,
            date(2024, 1, 5),
            "csv",
            "csv_dataframe_export",
            "yahoo_daily_v1",
            "test",
            ["Open", "High", "Low", "Close", "Adj Close", "Volume"],
        )


class FredFixture:
    def fetch(self, series, _bounds):
        payload = (FIXTURES / f"fred_{series.lower()}_response.json").read_bytes()
        row_count = len(json.loads(payload)["observations"])
        return FetchResult(
            payload,
            row_count,
            date(2024, 1, 5),
            "json",
            "json_http_body",
            "fred_observations_v1",
            "test",
        )


@pytest.fixture
def isolated_services():
    base = Settings()
    base.require_storage()
    suffix = uuid4().hex[:12]
    db_name = f"fts_phase3_test_{suffix}"
    bucket_name = f"fts-phase3-test-{suffix}"
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


def _ingestion(settings, store, yahoo_payload):
    return IngestionService(
        settings,
        metadata=MetadataStore(settings),
        publisher=BronzePublisher(store),
        yahoo=YahooFixture(yahoo_payload),
        fred=FredFixture(),
    )


def _manifest(store, dataset, version):
    key = f"silver/{dataset}/{version}/manifest.json"
    return json.loads(store.read(key))


def test_publish_reuse_revision_and_fatal_report(isolated_services):
    settings, store = isolated_services
    original = (FIXTURES / "yahoo_aapl_download.csv").read_bytes()
    ingestion = _ingestion(settings, store, original)
    ids = {}
    for source, key in (
        ("yahoo_finance", "AAPL"),
        ("fred", "VIXCLS"),
        ("fred", "DGS10"),
    ):
        result = ingestion.ingest(
            resolve_entity(source, key), backfill=True, bounds=BOUNDS
        )[0]
        assert result["status"] == "success"
        ids[key] = UUID(result["ingestion_run_id"])
    cutoff = datetime.now(UTC) + timedelta(minutes=5)
    silver = SilverService(settings, store=store, revision="phase3-test")
    market = silver.build("market", cutoff, [ids["AAPL"]])
    macro = silver.build("macro", cutoff, [ids["VIXCLS"], ids["DGS10"]])
    assert market["row_count"] == 4
    assert macro["row_count"] == 9
    assert silver.build("market", cutoff, [ids["AAPL"]])["reused"] is True
    for result in (market, macro):
        manifest = _manifest(store, result["dataset"], result["version"])
        assert manifest["row_count"] == result["row_count"]
        assert manifest["source_run_ids"] == sorted(manifest["source_run_ids"])
        assert (
            sum(part["row_count"] for part in manifest["parts"]) == result["row_count"]
        )
        for part in manifest["parts"]:
            payload = store.read(part["uri"].removeprefix(f"s3://{store.bucket}/"))
            assert len(payload) == part["byte_count"]
            assert sha256(payload) == part["sha256"]
            assert pq.read_table(BytesIO(payload)).num_rows == part["row_count"]
        report = manifest["quality_report"]
        report_bytes = store.read(report["uri"].removeprefix(f"s3://{store.bucket}/"))
        assert sha256(report_bytes) == report["sha256"]
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        assert (
            connection.execute("SELECT count(*) FROM dataset_versions").fetchone()[0]
            == 2
        )
        assert (
            connection.execute("SELECT count(*) FROM dataset_source_runs").fetchone()[0]
            == 3
        )

    corrected = original.replace(b"99.00,101.00,100.50", b"99.00,101.50,100.50")
    correction = _ingestion(settings, store, corrected).ingest(
        resolve_entity("yahoo_finance", "AAPL"), backfill=True, bounds=BOUNDS
    )[0]
    corrected_id = UUID(correction["ingestion_run_id"])
    revised = silver.build("market", cutoff, [ids["AAPL"], corrected_id])
    assert revised["version"] != market["version"]
    assert revised["warning_count"] == 1
    report_uri = _manifest(store, revised["dataset"], revised["version"])[
        "quality_report"
    ]["uri"]
    report = json.loads(store.read(report_uri.removeprefix(f"s3://{store.bucket}/")))
    assert report["warnings"][0]["code"] == "market_revision"
    assert _manifest(store, market["dataset"], market["version"])["row_count"] == 4

    repeated = _ingestion(settings, store, original).ingest(
        resolve_entity("yahoo_finance", "AAPL"), backfill=True, bounds=BOUNDS
    )[0]
    repeated_id = UUID(repeated["ingestion_run_id"])
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        connection.execute(
            "UPDATE ingestion_runs SET completed_at=%s WHERE ingestion_run_id=%s",
            (cutoff - timedelta(days=1), repeated_id),
        )
    nonwinning = silver.build("market", cutoff, [ids["AAPL"], repeated_id])
    assert nonwinning["version"] != market["version"]
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        counts = connection.execute(
            """SELECT source_run_id, output_row_count FROM dataset_source_runs
               WHERE dataset_id=(SELECT dataset_id FROM dataset_versions
                                 WHERE dataset_version=%s)""",
            (nonwinning["version"],),
        ).fetchall()
    assert dict(counts)[repeated_id] == 0

    invalid = original.replace(b"102.00,99.00", b"90.00,99.00")
    invalid_id = uuid4()
    entity = resolve_entity("yahoo_finance", "AAPL")
    metadata = MetadataStore(settings)
    metadata.insert_pending(
        invalid_id,
        entity,
        {"test": True},
        datetime.now(UTC),
        "yahoo_daily_v1",
        "phase3-test",
        True,
    )
    published = BronzePublisher(store).publish(
        invalid_id,
        entity,
        BOUNDS,
        YahooFixture(invalid).fetch(None, None),
        datetime.now(UTC),
        "phase3-test",
        {"test": True},
    )
    with metadata.entity_lock(entity) as lock_connection:
        metadata.finish_success(
            invalid_id,
            entity,
            BOUNDS.end,
            published.payload_uri,
            published.sha256,
            published.byte_count,
            4,
            lock_connection,
        )
    with pytest.raises(RuntimeError, match="market_ohlc_bounds"):
        silver.build("market", cutoff, [invalid_id])
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        assert (
            connection.execute("SELECT count(*) FROM dataset_versions").fetchone()[0]
            == 4
        )
        failed = connection.execute(
            """SELECT report_uri, report_sha256 FROM quality_runs
               WHERE status='failed' ORDER BY started_at DESC LIMIT 1"""
        ).fetchone()
    assert failed[0].startswith(f"s3://{store.bucket}/quality/phase_03/")
    assert (
        sha256(store.read(failed[0].removeprefix(f"s3://{store.bucket}/"))) == failed[1]
    )

    orphan_id = uuid4()
    metadata.insert_pending(
        orphan_id,
        entity,
        {"test": True},
        datetime.now(UTC),
        "yahoo_daily_v1",
        "phase3-test",
        True,
    )
    BronzePublisher(store).publish(
        orphan_id,
        entity,
        BOUNDS,
        YahooFixture(original).fetch(None, None),
        datetime.now(UTC),
        "phase3-test",
        {"test": True},
    )
    metadata.finish_terminal(orphan_id, "failed")
    assert silver.build("market", cutoff, [ids["AAPL"]])["reused"] is True
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        diagnostics = connection.execute(
            """SELECT diagnostics FROM quality_runs WHERE status='success'
               ORDER BY started_at DESC LIMIT 1"""
        ).fetchone()[0]
    assert str(orphan_id) in diagnostics["excluded_status_run_ids"]
    assert str(orphan_id) in diagnostics["orphan_manifest_run_ids"]
