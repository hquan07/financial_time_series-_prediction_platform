"""Immutable payload and final-manifest publication."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from financial_time_series.ingestion.models import DateRange, Entity, FetchResult
from financial_time_series.storage.minio_client import MinioStore


def object_prefix(entity: Entity, run_id: UUID) -> str:
    if entity.source == "yahoo_finance":
        return (
            f"bronze/yahoo_finance/ticker={entity.key}/"
            f"interval={entity.interval}/run_id={run_id}"
        )
    if entity.source == "fred":
        return f"bronze/fred/series={entity.key}/run_id={run_id}"
    raise ValueError("unsupported source")


@dataclass(frozen=True)
class Published:
    payload_uri: str
    manifest_uri: str
    sha256: str
    byte_count: int


class BronzePublisher:
    def __init__(self, store: MinioStore):
        self.store = store

    def publish(
        self,
        run_id: UUID,
        entity: Entity,
        bounds: DateRange,
        fetched: FetchResult,
        requested_at: datetime,
        code_revision: str,
        request_parameters: dict,
    ) -> Published:
        if fetched.row_count < 1 or fetched.max_observation_date is None:
            raise ValueError("cannot publish an empty payload")
        prefix = object_prefix(entity, run_id)
        payload_key = f"{prefix}/payload.{fetched.payload_format}"
        manifest_key = f"{prefix}/manifest.json"
        if self.store.exists(payload_key) or self.store.exists(manifest_key):
            raise FileExistsError("Bronze run prefix already exists")
        digest = hashlib.sha256(fetched.payload).hexdigest()
        self.store.put_new(
            payload_key,
            fetched.payload,
            "text/csv" if fetched.payload_format == "csv" else "application/json",
        )
        stored = self.store.read(payload_key)
        if (
            len(stored) != len(fetched.payload)
            or hashlib.sha256(stored).hexdigest() != digest
        ):
            raise OSError("stored Bronze payload checksum mismatch")
        manifest = {
            "manifest_version": "bronze_manifest_v1",
            "ingestion_run_id": str(run_id),
            "source": entity.source,
            "entity": entity.manifest_entity,
            "request_parameters": request_parameters,
            "requested_observation_range": {
                "start": bounds.start.isoformat(),
                "end": bounds.end.isoformat(),
            },
            "requested_at": requested_at.astimezone(UTC).isoformat(),
            "ingested_at": datetime.now(UTC).isoformat(),
            "source_available_at": None,
            "availability_policy_version": entity.availability_policy_version,
            "schema_version": fetched.schema_version,
            "source_version": fetched.source_version,
            "source_format": fetched.source_format,
            "column_shape": fetched.column_shape,
            "code_revision": code_revision,
            "payload": {
                "uri": self.store.uri(payload_key),
                "format": fetched.payload_format,
                "sha256": digest,
                "byte_count": len(fetched.payload),
                "row_count": fetched.row_count,
            },
        }
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        self.store.put_new(manifest_key, encoded, "application/json")
        if self.store.read(manifest_key) != encoded:
            raise OSError("stored Bronze manifest mismatch")
        return Published(
            self.store.uri(payload_key),
            self.store.uri(manifest_key),
            digest,
            len(stored),
        )

    def verify_manifest(self, run_id: UUID, entity: Entity) -> dict:
        prefix = object_prefix(entity, run_id)
        manifest = json.loads(self.store.read(f"{prefix}/manifest.json"))
        if manifest.get("ingestion_run_id") != str(run_id):
            raise ValueError("manifest run ID mismatch")
        payload = manifest["payload"]
        key = payload["uri"].removeprefix(f"s3://{self.store.bucket}/")
        stored = self.store.read(key)
        if len(stored) != payload["byte_count"] or (
            hashlib.sha256(stored).hexdigest() != payload["sha256"]
        ):
            raise OSError("manifest payload checksum mismatch")
        return manifest
