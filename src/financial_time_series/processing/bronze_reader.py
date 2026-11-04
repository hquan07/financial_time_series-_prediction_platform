"""Read successful PostgreSQL runs and verify their immutable Bronze evidence."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from financial_time_series.ingestion.models import Entity
from financial_time_series.processing.quality import QualityError, sha256
from financial_time_series.storage.bronze import object_prefix
from financial_time_series.storage.minio_client import MinioStore


@dataclass(frozen=True)
class BronzeInput:
    run_id: UUID
    completed_at: datetime
    manifest: dict
    manifest_sha256: str
    payload: bytes


def utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise QualityError("invalid_utc_timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QualityError("invalid_utc_timestamp")
    return parsed.astimezone(UTC)


def observation_bounds(manifest: dict) -> tuple[date, date]:
    try:
        bounds = manifest.get("requested_observation_range")
        if bounds is None:
            parameters = manifest["request_parameters"]
            if "end_exclusive" in parameters:
                raise QualityError("missing_observation_range")
            bounds = {"start": parameters["start"], "end": parameters["end"]}
        start = date.fromisoformat(bounds["start"])
        end = date.fromisoformat(bounds["end"])
        if start > end:
            raise QualityError("invalid_observation_range")
        return start, end
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, QualityError):
            raise
        raise QualityError("invalid_observation_range") from error


class BronzeReader:
    def __init__(self, store: MinioStore):
        self.store = store

    def read(self, row: dict, entity: Entity) -> BronzeInput:
        run_id = row["ingestion_run_id"]
        prefix = object_prefix(entity, run_id)
        try:
            encoded = self.store.read(f"{prefix}/manifest.json")
        except Exception as error:
            raise QualityError("missing_bronze_manifest") from error
        try:
            manifest = json.loads(encoded)
            payload = manifest["payload"]
            expected_format = "csv" if entity.source == "yahoo_finance" else "json"
            expected_schema = (
                "yahoo_daily_v1"
                if entity.source == "yahoo_finance"
                else "fred_observations_v1"
            )
            expected_uri = self.store.uri(f"{prefix}/payload.{expected_format}")
            if (
                manifest["manifest_version"] != "bronze_manifest_v1"
                or manifest["ingestion_run_id"] != str(run_id)
                or manifest["source"] != row["source_name"]
                or manifest["entity"] != entity.manifest_entity
                or manifest["schema_version"] != expected_schema
                or row["schema_version"] != expected_schema
                or manifest["source_format"]
                != (
                    "csv_dataframe_export"
                    if expected_format == "csv"
                    else "json_http_body"
                )
                or payload["format"] != expected_format
                or payload["uri"] != expected_uri
                or row["payload_uri"] != expected_uri
                or payload["sha256"] != row["sha256"]
                or payload["byte_count"] != row["byte_count"]
                or payload["row_count"] != row["row_count"]
                or payload["row_count"] < 1
            ):
                raise QualityError("bronze_metadata_mismatch")
            utc_timestamp(manifest["ingested_at"])
            observation_bounds(manifest)
            if manifest["source_available_at"] is not None:
                raise QualityError("unverified_source_availability")
        except (KeyError, TypeError, ValueError) as error:
            if isinstance(error, QualityError):
                raise
            raise QualityError("invalid_bronze_manifest") from error
        try:
            body = self.store.read(f"{prefix}/payload.{expected_format}")
        except Exception as error:
            raise QualityError("missing_bronze_payload") from error
        if len(body) != payload["byte_count"] or sha256(body) != payload["sha256"]:
            raise QualityError("bronze_payload_checksum_mismatch")
        return BronzeInput(run_id, row["completed_at"], manifest, sha256(encoded), body)
