import hashlib
import json
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest

from financial_time_series.ingestion.models import DateRange, Entity, FetchResult
from financial_time_series.storage.bronze import BronzePublisher, object_prefix


class MemoryStore:
    bucket = "test-bucket"

    def __init__(self):
        self.objects = {}
        self.corrupt = False

    def exists(self, key):
        return key in self.objects

    def put_new(self, key, payload, _type):
        if key in self.objects:
            raise FileExistsError(key)
        self.objects[key] = payload

    def read(self, key):
        if self.corrupt:
            return b"corrupted"
        return self.objects[key]

    def uri(self, key):
        return f"s3://{self.bucket}/{key}"


def test_manifest_is_last_and_matches_stored_bytes():
    store = MemoryStore()
    publisher = BronzePublisher(store)
    run_id = uuid4()
    entity = Entity(
        "fred",
        "VIXCLS",
        "1d",
        {"series_id": "VIXCLS"},
        availability_policy_version="verified_release_or_retrieval_v1",
    )
    payload = b'{"observations":[]}'
    fetched = FetchResult(
        payload,
        1,
        date(2024, 1, 2),
        "json",
        "json_http_body",
        "fred_observations_v1",
        "fred_api_v1",
    )
    result = publisher.publish(
        run_id,
        entity,
        DateRange(date(2024, 1, 1), date(2024, 1, 2)),
        fetched,
        datetime.now(UTC),
        "abc",
        {"series_id": "VIXCLS", "file_type": "json"},
    )
    assert list(store.objects)[-1].endswith("manifest.json")
    manifest = json.loads(
        store.objects[f"{object_prefix(entity, run_id)}/manifest.json"]
    )
    assert result.sha256 == hashlib.sha256(payload).hexdigest()
    assert manifest["payload"]["sha256"] == result.sha256
    assert "api_key" not in json.dumps(manifest)
    assert publisher.verify_manifest(run_id, entity) == manifest
    with pytest.raises(FileExistsError):
        publisher.publish(
            run_id,
            entity,
            DateRange(date(2024, 1, 1), date(2024, 1, 2)),
            fetched,
            datetime.now(UTC),
            "abc",
            {"series_id": "VIXCLS", "file_type": "json"},
        )


def test_corrupt_readback_prevents_manifest():
    store = MemoryStore()
    store.corrupt = True
    entity = Entity("fred", "DGS10", "1d", {"series_id": "DGS10"})
    run_id = uuid4()
    fetched = FetchResult(
        b"payload", 1, date(2024, 1, 2), "json", "json_http_body", "v1", "v1"
    )
    with pytest.raises(OSError, match="checksum"):
        BronzePublisher(store).publish(
            run_id,
            entity,
            DateRange(date(2024, 1, 2), date(2024, 1, 2)),
            fetched,
            datetime.now(UTC),
            "abc",
            {"series_id": "DGS10", "file_type": "json"},
        )
    assert not store.exists(f"{object_prefix(entity, run_id)}/manifest.json")
