"""Verified Silver reads and immutable Gold publication."""

from collections import Counter, defaultdict
from datetime import UTC, datetime
from importlib.metadata import version as package_version
from io import BytesIO
from uuid import uuid4

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from financial_time_series.features.compute import (
    MACRO_FEATURES,
    PRICE_FEATURES,
    build_rows,
    build_training_rows,
)
from financial_time_series.ingestion.service import code_revision
from financial_time_series.processing.quality import QualityError, json_bytes, sha256
from financial_time_series.settings import PROJECT_ROOT, Settings
from financial_time_series.storage.minio_client import MinioStore
from financial_time_series.storage.parquet import WRITER_OPTIONS

NAMES = {
    "features": "market_features_v1",
    "training": "training_next_session_return_v1",
}


class GoldService:
    def __init__(self, settings: Settings):
        settings.require_storage()
        self.settings = settings
        self.store = MinioStore(settings)

    def _read_object(self, uri: str, digest: str, size: int | None = None) -> bytes:
        prefix = f"s3://{self.store.bucket}/"
        if not uri.startswith(prefix):
            raise QualityError("parent_uri_mismatch")
        payload = self.store.read(uri[len(prefix) :])
        if sha256(payload) != digest or (size is not None and len(payload) != size):
            raise QualityError("parent_checksum_mismatch")
        return payload

    def _parent(
        self, connection, layer: str, name: str, version: str
    ) -> tuple[dict, list[dict]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """SELECT * FROM dataset_versions WHERE layer=%s AND dataset_name=%s
                   AND dataset_version=%s AND status='success'""",
                (layer, name, version),
            )
            record = cursor.fetchone()
        if record is None:
            raise QualityError("parent_not_published")
        manifest = __import__("json").loads(
            self._read_object(record["manifest_uri"], record["manifest_sha256"])
        )
        if (
            manifest["dataset_name"] != name
            or manifest["dataset_version"] != version
            or manifest["row_count"] != record["row_count"]
        ):
            raise QualityError("parent_manifest_mismatch")
        quality = manifest["quality_report"]
        self._read_object(quality["uri"], quality["sha256"], quality["byte_count"])
        rows = []
        for part in manifest["parts"]:
            payload = self._read_object(part["uri"], part["sha256"], part["byte_count"])
            table = pq.read_table(BytesIO(payload))
            if table.num_rows != part["row_count"]:
                raise QualityError("parent_part_row_count_mismatch")
            rows.extend(table.to_pylist())
        if len(rows) != record["row_count"]:
            raise QualityError("parent_row_count_mismatch")
        return record, rows

    def build(
        self,
        dataset: str,
        market_version: str,
        macro_version: str,
        cutoff: datetime,
        features_version: str | None = None,
    ) -> dict:
        if dataset not in NAMES or cutoff.tzinfo is None:
            raise ValueError("invalid dataset or naive build cutoff")
        if dataset == "training" and not features_version:
            raise ValueError("training needs an explicit features version")
        revision = code_revision()
        if revision == "unknown" or revision.endswith("+dirty"):
            raise ValueError("Gold publication needs a clean Git revision")
        cutoff = cutoff.astimezone(UTC)
        name = NAMES[dataset]
        config = yaml.safe_load(
            (PROJECT_ROOT / "config/feature_config.yaml").read_text()
        )
        self.store.require_bucket()
        with psycopg.connect(**self.settings.postgres_kwargs()) as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"gold:{name}",)
            )
            market, market_rows = self._parent(
                connection, "silver", "market_ohlcv_v1", market_version
            )
            macro, macro_rows = self._parent(
                connection, "silver", "macro_observations_v1", macro_version
            )
            parents = [(market, "market_input"), (macro, "macro_input")]
            if dataset == "features":
                rows, _ = build_rows(market_rows, macro_rows)
            else:
                feature_parent, feature_rows = self._parent(
                    connection, "gold", NAMES["features"], features_version
                )
                feature_manifest = __import__("json").loads(
                    self._read_object(
                        feature_parent["manifest_uri"],
                        feature_parent["manifest_sha256"],
                    )
                )
                upstream = feature_manifest["version_input"]["parents"]
                if [item["id"] for item in upstream] != [
                    str(market["dataset_id"]),
                    str(macro["dataset_id"]),
                ]:
                    raise QualityError("feature_parent_inputs_mismatch")
                parents = [(feature_parent, "feature_input"), (market, "label_input")]
                rows = build_training_rows(feature_rows, market_rows)
            if not rows:
                raise QualityError("empty_gold_dataset")
            version_input = {
                "layer": "gold",
                "dataset_name": name,
                "build_cutoff_at": cutoff.isoformat(timespec="microseconds"),
                "parents": [
                    {
                        "id": str(parent["dataset_id"]),
                        "sha256": parent["manifest_sha256"],
                        "relationship": role,
                    }
                    for parent, role in parents
                ],
                "feature_contract_version": "features_v1",
                "target_contract_version": "adj_open_to_close_v1"
                if dataset == "training"
                else None,
                "feature_config": config,
                "code_revision": revision,
                "pyarrow_version": package_version("pyarrow"),
                "writer_options": WRITER_OPTIONS,
            }
            version = (
                cutoff.strftime("%Y%m%dT%H%M%S%fZ")
                + "-"
                + sha256(json_bytes(version_input))[:16]
            )
            prefix = f"gold/{name}/{version}"
            partitions = defaultdict(list)
            for row in rows:
                session = (
                    row["as_of_session"]
                    if dataset == "training"
                    else row["session_date"]
                )
                partitions[(row["exchange"], row["ticker"], session.year)].append(row)
            schema = pa.Table.from_pylist(rows).schema
            typed = {
                **{field: pa.float64() for field in (*PRICE_FEATURES, *MACRO_FEATURES)},
                **{
                    f"selected_{prefix}_source_run_id": pa.string()
                    for prefix in ("vix", "dgs10")
                },
                **{
                    f"selected_{prefix}_available_at": pa.timestamp("us", tz="UTC")
                    for prefix in ("vix", "dgs10")
                },
                **{
                    f"selected_{prefix}_observation_date": pa.date32()
                    for prefix in ("vix", "dgs10")
                },
            }
            schema = pa.schema(
                [
                    pa.field(field.name, typed.get(field.name, field.type))
                    for field in schema
                ]
            )
            parts, payloads = [], []
            for (exchange, ticker, year), group in sorted(partitions.items()):
                key = (
                    f"{prefix}/exchange={exchange}/ticker={ticker}/"
                    f"session_year={year}/part-00000.parquet"
                )
                table = pa.Table.from_pylist(group, schema=schema)
                output = BytesIO()
                pq.write_table(table, output, **WRITER_OPTIONS)
                payload = output.getvalue()
                part = {
                    "uri": self.store.uri(key),
                    "sha256": sha256(payload),
                    "byte_count": len(payload),
                    "row_count": len(group),
                }
                parts.append(part)
                payloads.append((key, payload))
            reasons = Counter(
                reason for row in rows for reason in row["feature_readiness_reasons"]
            )
            report = {
                "dataset_name": name,
                "row_count": len(rows),
                "ready_counts": {
                    key: sum(bool(row[key]) for row in rows)
                    for key in (
                        "technical_ready",
                        "macro_ready",
                        "feature_ready",
                        "price_only_ready",
                        "market_point_in_time_verified",
                    )
                },
                "reason_counts": dict(sorted(reasons.items())),
            }
            report_payload = json_bytes(report)
            report_key = f"{prefix}/quality_report.json"
            report_part = {
                "uri": self.store.uri(report_key),
                "sha256": sha256(report_payload),
                "byte_count": len(report_payload),
            }
            manifest = {
                "manifest_version": "gold_manifest_v1",
                "dataset_name": name,
                "dataset_version": version,
                "version_input": version_input,
                "row_count": len(rows),
                "parts": parts,
                "quality_report": report_part,
                "columns": [
                    {"name": field.name, "type": str(field.type)} for field in schema
                ],
                "readiness_tier": config["readiness_tier"],
            }
            manifest_payload = json_bytes(manifest)
            manifest_key = f"{prefix}/manifest.json"
            existing = connection.execute(
                """SELECT dataset_id,manifest_sha256 FROM dataset_versions
                   WHERE layer='gold' AND dataset_name=%s AND dataset_version=%s""",
                (name, version),
            ).fetchone()
            if existing:
                if existing[1] != sha256(manifest_payload):
                    raise QualityError("published_gold_mismatch")
                for key, payload in [
                    *payloads,
                    (report_key, report_payload),
                    (manifest_key, manifest_payload),
                ]:
                    self._read_object(
                        self.store.uri(key), sha256(payload), len(payload)
                    )
                return {
                    "dataset": name,
                    "version": version,
                    "row_count": len(rows),
                    "reused": True,
                }
            if any(
                self.store.exists(key)
                for key, _ in [
                    *payloads,
                    (report_key, report_payload),
                    (manifest_key, manifest_payload),
                ]
            ):
                raise QualityError("orphan_or_partial_gold")
            for key, payload in [
                *payloads,
                (report_key, report_payload),
                (manifest_key, manifest_payload),
            ]:
                self.store.put_new(
                    key,
                    payload,
                    "application/vnd.apache.parquet"
                    if key.endswith(".parquet")
                    else "application/json",
                )
                self._read_object(self.store.uri(key), sha256(payload), len(payload))
            dataset_id = uuid4()
            connection.execute(
                """INSERT INTO dataset_versions
                   (dataset_id,layer,dataset_name,dataset_version,build_cutoff_at,
                    manifest_uri,manifest_sha256,code_revision,contract_version,
                    schema_version,row_count,status)
                   VALUES (%s,'gold',%s,%s,%s,%s,%s,%s,%s,%s,%s,'success')""",
                (
                    dataset_id,
                    name,
                    version,
                    cutoff,
                    self.store.uri(manifest_key),
                    sha256(manifest_payload),
                    revision,
                    "features_v1",
                    name,
                    len(rows),
                ),
            )
            for ordinal, (parent, role) in enumerate(parents):
                connection.execute(
                    "INSERT INTO dataset_parents VALUES (%s,%s,%s,%s)",
                    (dataset_id, parent["dataset_id"], ordinal, role),
                )
            connection.execute(
                """INSERT INTO quality_runs
                   (quality_run_id,dataset_name,dataset_version,dataset_id,
                    build_cutoff_at,rule_version,status,warning_count,report_uri,
                    report_sha256,diagnostics,completed_at)
                   VALUES (%s,%s,%s,%s,%s,'gold_quality_v1','success',
                           %s,%s,%s,%s,now())""",
                (
                    uuid4(),
                    name,
                    version,
                    dataset_id,
                    cutoff,
                    sum(reasons.values()),
                    self.store.uri(report_key),
                    sha256(report_payload),
                    Jsonb(report),
                ),
            )
            return {
                "dataset": name,
                "version": version,
                "row_count": len(rows),
                "reused": False,
                "ready_counts": report["ready_counts"],
            }
