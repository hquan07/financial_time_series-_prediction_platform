"""Build and publish deterministic Bronze-to-Silver datasets."""

from collections import defaultdict
from datetime import UTC, datetime
from importlib.metadata import version as package_version
from itertools import islice
from uuid import UUID, uuid4

from financial_time_series.ingestion.service import code_revision, resolve_entity
from financial_time_series.processing.bronze_reader import BronzeReader
from financial_time_series.processing.calendar import missing_sessions
from financial_time_series.processing.canonicalize import (
    canonicalize_market,
    report_macro_revisions,
)
from financial_time_series.processing.macro_normalizer import normalize_macro
from financial_time_series.processing.market_normalizer import normalize_market
from financial_time_series.processing.quality import (
    QualityError,
    QualityReport,
    json_bytes,
    sha256,
)
from financial_time_series.settings import Settings, load_assets, load_source_config
from financial_time_series.storage.minio_client import MinioStore
from financial_time_series.storage.parquet import WRITER_OPTIONS, encode_part
from financial_time_series.storage.postgres_metadata import MetadataStore
from financial_time_series.storage.silver import SilverStore

DATASET_NAMES = {"market": "market_ohlcv_v1", "macro": "macro_observations_v1"}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("build cutoff must have a timezone")
    return value.astimezone(UTC)


def _version_input(
    dataset_name: str, cutoff: datetime, inputs: list, revision: str
) -> dict:
    return {
        "layer": "silver",
        "dataset_name": dataset_name,
        "build_cutoff_at": cutoff.isoformat(timespec="microseconds"),
        "source_manifests": [
            {
                "run_id": str(item.run_id),
                "manifest_sha256": item.manifest_sha256,
                "completed_at": item.completed_at.astimezone(UTC).isoformat(),
            }
            for item in inputs
        ],
        "contract_version": "data_contract_v1",
        "schema_version": dataset_name,
        "canonicalization_version": "market_latest_completion_v1",
        "quality_rule_version": "silver_quality_v1",
        "availability_rule_version": "verified_release_or_retrieval_v1",
        "calendar_version": package_version("exchange-calendars"),
        "calendar_aliases": {"XNAS": "XNYS", "XNYS": "XNYS"},
        "pyarrow_version": package_version("pyarrow"),
        "parquet_writer_options": WRITER_OPTIONS,
        "code_revision": revision,
    }


def dataset_version(inputs: dict, cutoff: datetime) -> str:
    timestamp = cutoff.strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}-{sha256(json_bytes(inputs))[:16]}"


def _partitioned(rows: list[dict], dataset: str) -> list[tuple[str, list[dict]]]:
    groups = defaultdict(list)
    for row in rows:
        if dataset == "market":
            partition = (
                f"exchange={row['exchange']}/ticker={row['ticker']}/"
                f"session_year={row['session_date'].year}"
            )
        else:
            partition = (
                f"series_id={row['series_id']}/"
                f"observation_year={row['observation_date'].year}"
            )
        groups[partition].append(row)
    key = (
        (lambda row: (row["session_date"], str(row["source_run_id"])))
        if dataset == "market"
        else (lambda row: (row["observation_date"], str(row["source_run_id"])))
    )
    return [
        (partition, sorted(group, key=key))
        for partition, group in sorted(groups.items())
    ]


def _add_quality(rows: list[dict], dataset: str, report: QualityReport) -> None:
    report.statistics = {"row_count": len(rows)}
    if dataset == "macro":
        report_macro_revisions(rows, report)
        nulls = [row for row in rows if row["value"] is None]
        report.statistics["null_value_count"] = len(nulls)
        for row in nulls:
            report.warnings.append(
                {
                    "code": "macro_null_value",
                    "series_id": row["series_id"],
                    "observation_date": row["observation_date"].isoformat(),
                    "source_run_id": str(row["source_run_id"]),
                }
            )
        return
    for field in ("close", "volume", "daily_range"):
        values = [
            row["high"] - row["low"] if field == "daily_range" else row[field]
            for row in rows
        ]
        report.statistics[field] = {"min": min(values), "max": max(values)}
    assets = defaultdict(list)
    for row in rows:
        assets[(row["exchange"], row["ticker"])].append(row["session_date"])
    for (exchange, ticker), dates in sorted(assets.items()):
        gaps = missing_sessions(exchange, sorted(dates))
        if gaps:
            report.warnings.append(
                {
                    "code": "market_session_gap",
                    "exchange": exchange,
                    "ticker": ticker,
                    "missing_sessions": gaps,
                }
            )


class SilverService:
    def __init__(
        self,
        settings: Settings,
        *,
        metadata: MetadataStore | None = None,
        store: MinioStore | None = None,
        revision: str | None = None,
    ):
        settings.require_storage()
        self.metadata = metadata or MetadataStore(settings)
        self.store = store or MinioStore(settings)
        self.silver = SilverStore(self.store)
        self.reader = BronzeReader(self.store)
        self.revision = revision

    def build(
        self,
        dataset: str,
        cutoff: datetime,
        source_run_ids: list[UUID] | None = None,
    ) -> dict:
        if dataset not in DATASET_NAMES:
            raise ValueError("dataset must be market or macro")
        cutoff = _utc(cutoff)
        if source_run_ids is not None and (
            not 1 <= len(source_run_ids) <= 100
            or len(source_run_ids) != len(set(source_run_ids))
        ):
            raise ValueError("source-run-id list must contain 1-100 unique IDs")
        revision = self.revision or code_revision()
        if revision == "unknown" or revision.endswith("+dirty"):
            raise ValueError("Silver builds require a clean code revision")
        name = DATASET_NAMES[dataset]
        self.store.require_bucket()
        with self.metadata.dataset_lock(name):
            quality_id = uuid4()
            source, keys = self._source_scope(dataset)
            diagnostics = self._diagnostics(source, keys, cutoff)
            self.metadata.start_quality(quality_id, name, cutoff, diagnostics)
            report = QualityReport(name, [])
            try:
                result = self._build_locked(
                    dataset,
                    name,
                    cutoff,
                    revision,
                    source_run_ids,
                    quality_id,
                    report,
                )
            except Exception as error:
                code = (
                    error.code
                    if isinstance(error, QualityError)
                    else type(error).__name__
                )
                failure = {
                    "quality_run_id": str(quality_id),
                    "dataset_name": name,
                    "build_cutoff_at": cutoff.isoformat(),
                    "failure_code": code,
                    "source_run_ids": report.source_run_ids,
                }
                encoded = json_bytes(failure)
                uri = digest = None
                try:
                    record = self.silver.put_verified(
                        f"quality/phase_03/{quality_id}/report.json",
                        encoded,
                        "application/json",
                    )
                    uri, digest = record["uri"], record["sha256"]
                except Exception:
                    pass
                self.metadata.fail_quality(
                    quality_id, uri, digest, len(report.warnings), code
                )
                raise RuntimeError(f"silver build failed: {code}") from None
            return result

    @staticmethod
    def _source_scope(dataset: str) -> tuple[str, list[str]]:
        if dataset == "market":
            return "yahoo_finance", [asset["ticker"] for asset in load_assets()]
        return "fred", [item["id"] for item in load_source_config()["macro"]["series"]]

    def _diagnostics(self, source: str, keys: list[str], cutoff: datetime) -> dict:
        statuses = self.metadata.silver_run_statuses(source, keys)
        successful = {
            row["ingestion_run_id"] for row in statuses if row["status"] == "success"
        }
        diagnostics = {
            "excluded_status_run_ids": sorted(
                str(row["ingestion_run_id"])
                for row in statuses
                if row["status"] != "success"
            ),
            "excluded_cutoff_run_ids": sorted(
                str(row["ingestion_run_id"])
                for row in statuses
                if row["status"] == "success"
                and (row["completed_at"] is None or row["completed_at"] > cutoff)
            ),
        }
        try:
            objects = islice(
                self.store.client.list_objects(
                    self.store.bucket, prefix=f"bronze/{source}/", recursive=True
                ),
                10001,
            )
            orphan_ids = []
            count = 0
            for item in objects:
                count += 1
                if not item.object_name.endswith("/manifest.json"):
                    continue
                try:
                    run_id = UUID(
                        item.object_name.rsplit("run_id=", 1)[1].split("/", 1)[0]
                    )
                except (IndexError, ValueError):
                    continue
                if run_id not in successful:
                    orphan_ids.append(str(run_id))
            diagnostics["orphan_manifest_run_ids"] = sorted(set(orphan_ids))
            diagnostics["orphan_scan_truncated"] = count > 10000
        except Exception as error:
            diagnostics["orphan_scan_error"] = type(error).__name__
        return diagnostics

    def _build_locked(
        self,
        dataset: str,
        name: str,
        cutoff: datetime,
        revision: str,
        source_run_ids: list[UUID] | None,
        quality_id: UUID,
        report: QualityReport,
    ) -> dict:
        source, keys = self._source_scope(dataset)
        candidates = self.metadata.silver_candidates(source, keys, cutoff)
        if source_run_ids is not None:
            requested = set(source_run_ids)
            candidates = [
                row for row in candidates if row["ingestion_run_id"] in requested
            ]
            if {row["ingestion_run_id"] for row in candidates} != requested:
                raise QualityError("source_run_not_eligible")
        if not candidates:
            raise QualityError("no_eligible_bronze_runs")
        report.source_run_ids = sorted(
            str(row["ingestion_run_id"]) for row in candidates
        )
        inputs = []
        rows = []
        source_counts = []
        for candidate in candidates:
            entity = resolve_entity(source, candidate["entity_key"])
            item = self.reader.read(candidate, entity)
            inputs.append(item)
            normalized = (
                normalize_market(item, entity)
                if dataset == "market"
                else normalize_macro(item, entity)
            )
            rows.extend(normalized)
            source_counts.append((item.run_id, len(normalized)))
        inputs.sort(key=lambda item: str(item.run_id))
        source_counts.sort(key=lambda item: str(item[0]))
        report.source_run_ids = [str(item.run_id) for item in inputs]
        if dataset == "market":
            rows = canonicalize_market(rows, report)
        _add_quality(rows, dataset, report)
        version_input = _version_input(name, cutoff, inputs, revision)
        version = dataset_version(version_input, cutoff)
        prefix = f"silver/{name}/{version}"
        parts = []
        part_payloads = []
        for partition, partition_rows in _partitioned(rows, dataset):
            key = f"{prefix}/{partition}/part-00000.parquet"
            payload = encode_part(partition_rows, dataset)
            parts.append(
                {
                    "uri": self.store.uri(key),
                    "sha256": sha256(payload),
                    "byte_count": len(payload),
                    "row_count": len(partition_rows),
                }
            )
            part_payloads.append((key, payload))
        quality = report.as_dict()
        quality.update(
            {
                "build_cutoff_at": cutoff.isoformat(timespec="microseconds"),
                "dataset_version": version,
            }
        )
        report_payload = json_bytes(quality)
        report_key = f"{prefix}/quality_report.json"
        report_record = {
            "uri": self.store.uri(report_key),
            "sha256": sha256(report_payload),
            "byte_count": len(report_payload),
        }
        output_counts = defaultdict(int)
        for row in rows:
            output_counts[row["source_run_id"]] += 1
        counts = [
            (run_id, input_count, output_counts[run_id])
            for run_id, input_count in source_counts
        ]
        manifest = {
            "manifest_version": "silver_manifest_v1",
            "dataset_name": name,
            "dataset_version": version,
            "version_input": version_input,
            "source_run_ids": report.source_run_ids,
            "row_count": len(rows),
            "parts": parts,
            "quality_report": report_record,
            "source_row_counts": [
                {"run_id": str(run_id), "input": input_count, "output": output_count}
                for run_id, input_count, output_count in counts
            ],
        }
        manifest_payload = json_bytes(manifest)
        manifest_key = f"{prefix}/manifest.json"
        manifest_sha = sha256(manifest_payload)
        existing = self.metadata.publication(name, version)
        if existing is not None:
            self._verify_existing(
                existing,
                manifest_key,
                manifest_payload,
                parts,
                report_record,
                counts,
            )
            self.metadata.reuse_publication(
                quality_id,
                existing["dataset_id"],
                version,
                report_record["uri"],
                report_record["sha256"],
                len(report.warnings),
            )
            return {
                "dataset": name,
                "version": version,
                "row_count": len(rows),
                "warning_count": len(report.warnings),
                "reused": True,
            }
        if self.store.exists(manifest_key):
            raise QualityError("orphan_silver_manifest")
        if self.store.exists(report_key) or any(
            self.store.exists(key) for key, _ in part_payloads
        ):
            raise QualityError("partial_silver_artifacts")
        for key, payload in part_payloads:
            self.silver.put_verified(key, payload, "application/vnd.apache.parquet")
        self.silver.put_verified(report_key, report_payload, "application/json")
        self.silver.put_verified(manifest_key, manifest_payload, "application/json")
        self.metadata.complete_publication(
            dataset_id=uuid4(),
            quality_id=quality_id,
            dataset_name=name,
            version=version,
            cutoff=cutoff,
            manifest_uri=self.store.uri(manifest_key),
            manifest_sha=manifest_sha,
            code_revision=revision,
            row_count=len(rows),
            source_counts=counts,
            report_uri=report_record["uri"],
            report_sha=report_record["sha256"],
            warning_count=len(report.warnings),
        )
        return {
            "dataset": name,
            "version": version,
            "row_count": len(rows),
            "warning_count": len(report.warnings),
            "reused": False,
        }

    def _verify_existing(
        self,
        existing: dict,
        manifest_key: str,
        expected: bytes,
        parts: list[dict],
        report: dict,
        counts: list[tuple[UUID, int, int]],
    ) -> None:
        if (
            existing["manifest_uri"] != self.store.uri(manifest_key)
            or existing["manifest_sha256"] != sha256(expected)
            or existing["row_count"] != sum(item["row_count"] for item in parts)
        ):
            raise QualityError("published_version_mismatch")
        self.silver.verify(manifest_key, sha256(expected), len(expected))
        for item in [*parts, report]:
            self.silver.verify(
                self.silver.key_from_uri(item["uri"]),
                item["sha256"],
                item["byte_count"],
            )
        stored = self.metadata.publication_sources(existing["dataset_id"])
        expected_counts = [
            (run_id, input_count, output_count)
            for run_id, input_count, output_count in counts
        ]
        if [
            (row["source_run_id"], row["input_row_count"], row["output_row_count"])
            for row in stored
        ] != expected_counts:
            raise QualityError("published_lineage_mismatch")
