import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from financial_time_series.ingestion.service import resolve_entity
from financial_time_series.processing.bronze_reader import BronzeReader
from financial_time_series.processing.quality import QualityError, sha256
from financial_time_series.storage.bronze import object_prefix


class MemoryStore:
    bucket = "test-bucket"

    def __init__(self):
        self.objects = {}

    def uri(self, key):
        return f"s3://{self.bucket}/{key}"

    def read(self, key):
        return self.objects[key]


def fixture():
    store = MemoryStore()
    entity = resolve_entity("yahoo_finance", "AAPL")
    run_id = uuid4()
    prefix = object_prefix(entity, run_id)
    payload = b"Date,Open,High,Low,Close,Adj Close,Volume\n"
    uri = store.uri(f"{prefix}/payload.csv")
    manifest = {
        "manifest_version": "bronze_manifest_v1",
        "ingestion_run_id": str(run_id),
        "source": "yahoo_finance",
        "entity": entity.manifest_entity,
        "schema_version": "yahoo_daily_v1",
        "source_format": "csv_dataframe_export",
        "ingested_at": "2026-11-04T12:00:00+00:00",
        "source_available_at": None,
        "payload": {
            "format": "csv",
            "uri": uri,
            "sha256": sha256(payload),
            "byte_count": len(payload),
            "row_count": 1,
        },
    }
    row = {
        "ingestion_run_id": run_id,
        "source_name": "yahoo_finance",
        "schema_version": "yahoo_daily_v1",
        "payload_uri": uri,
        "sha256": sha256(payload),
        "byte_count": len(payload),
        "row_count": 1,
        "completed_at": datetime.now(UTC),
    }
    store.objects[f"{prefix}/manifest.json"] = json.dumps(manifest).encode()
    store.objects[f"{prefix}/payload.csv"] = payload
    return store, entity, row, manifest, prefix


def test_valid_manifest_and_payload():
    store, entity, row, _, _ = fixture()
    item = BronzeReader(store).read(row, entity)
    assert item.run_id == row["ingestion_run_id"]
    assert item.payload == store.read(item.manifest["payload"]["uri"].split("/", 3)[3])


def test_missing_manifest_checksum_and_unverified_release():
    store, entity, row, manifest, prefix = fixture()
    store.objects.pop(f"{prefix}/manifest.json")
    with pytest.raises(QualityError, match="missing_bronze_manifest"):
        BronzeReader(store).read(row, entity)
    store.objects[f"{prefix}/manifest.json"] = json.dumps(manifest).encode()
    store.objects[f"{prefix}/payload.csv"] = b"bad"
    with pytest.raises(QualityError, match="bronze_payload_checksum_mismatch"):
        BronzeReader(store).read(row, entity)
    store.objects[f"{prefix}/payload.csv"] = (
        b"Date,Open,High,Low,Close,Adj Close,Volume\n"
    )
    manifest["source_available_at"] = "2024-01-02T00:00:00+00:00"
    store.objects[f"{prefix}/manifest.json"] = json.dumps(manifest).encode()
    with pytest.raises(QualityError, match="unverified_source_availability"):
        BronzeReader(store).read(row, entity)
