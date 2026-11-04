"""One-entity batch ingestion with durable audit and monotonic progress."""

import subprocess
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

from financial_time_series.ingestion.backfill import (
    chunk_ranges,
    latest_closed_date,
    validate_backfill,
)
from financial_time_series.ingestion.fred_client import FredClient
from financial_time_series.ingestion.models import DateRange, Entity, FetchResult
from financial_time_series.ingestion.yahoo_finance import YahooFinanceClient
from financial_time_series.settings import (
    PROJECT_ROOT,
    Settings,
    load_assets,
    load_source_config,
)
from financial_time_series.storage.bronze import BronzePublisher
from financial_time_series.storage.minio_client import MinioStore
from financial_time_series.storage.postgres_metadata import MetadataStore


def resolve_entity(source: str, key: str) -> Entity:
    config = load_source_config()
    if source == "yahoo_finance":
        for asset in load_assets():
            if asset["ticker"] == key:
                return Entity(
                    source,
                    key,
                    config["market"]["interval"],
                    {"ticker": key, "exchange": asset["exchange"]},
                    timezone=asset["timezone"],
                )
        raise ValueError("ticker is not in the configured asset universe")
    if source == "fred":
        for series in config["macro"]["series"]:
            if series["id"] == key:
                return Entity(
                    source,
                    key,
                    "1d",
                    {"series_id": key},
                    availability_policy_version=series["availability_policy"],
                )
        raise ValueError("FRED series is not configured")
    raise ValueError("unsupported source")


def code_revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return "unknown"
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    suffix = "+dirty" if status.returncode != 0 or status.stdout else ""
    return result.stdout.strip() + suffix


class IngestionService:
    def __init__(
        self,
        settings: Settings,
        metadata: MetadataStore | None = None,
        publisher: BronzePublisher | None = None,
        yahoo: YahooFinanceClient | None = None,
        fred: FredClient | None = None,
    ):
        settings.require_storage()
        self.settings = settings
        self.metadata = metadata or MetadataStore(settings)
        self.publisher = publisher or BronzePublisher(MinioStore(settings))
        self.yahoo = yahoo or YahooFinanceClient()
        self.fred = fred

    def _fetch(self, entity: Entity, bounds: DateRange) -> FetchResult:
        if entity.source == "yahoo_finance":
            return self.yahoo.fetch(entity.key, bounds)
        if self.fred is None:
            self.fred = FredClient(self.settings.fred_api_key.get_secret_value())
        return self.fred.fetch(entity.key, bounds)

    def ingest(
        self,
        entity: Entity,
        *,
        backfill: bool = False,
        bounds: DateRange | None = None,
        now: datetime | None = None,
    ) -> list[dict]:
        config = load_source_config()[
            "market" if entity.source == "yahoo_finance" else "macro"
        ]
        latest = latest_closed_date(entity.timezone, now)
        if backfill:
            if bounds is None:
                raise ValueError("backfill requires start and end")
            validate_backfill(bounds, latest)
            requested_bounds = bounds
        else:
            if bounds is not None:
                raise ValueError(
                    "incremental ingestion does not accept explicit bounds"
                )
            requested_bounds = None
        self.publisher.store.require_bucket()
        summaries = []
        with self.metadata.entity_lock(entity) as lock_connection:
            if requested_bounds is None:
                initial = date.fromisoformat(config["initial_start_date"])
                watermark = self.metadata.watermark(entity)
                overlap = config["incremental_overlap_calendar_days"]
                start = (
                    max(initial, watermark - timedelta(days=overlap))
                    if watermark
                    else initial
                )
                if start > latest:
                    return []
                requested_bounds = DateRange(start, latest)
            for chunk in chunk_ranges(
                requested_bounds, config["max_request_span_calendar_days"]
            ):
                summaries.append(
                    self._ingest_chunk(entity, chunk, backfill, lock_connection)
                )
        return summaries

    def _ingest_chunk(
        self, entity: Entity, bounds: DateRange, backfill: bool, lock_connection
    ) -> dict:
        run_id = uuid4()
        requested_at = datetime.now(UTC)
        revision = code_revision()
        if entity.source == "yahoo_finance":
            parameters = {
                "ticker": entity.key,
                "start": bounds.start.isoformat(),
                "end": (bounds.end + timedelta(days=1)).isoformat(),
                "end_exclusive": True,
                "requested_end_inclusive": bounds.end.isoformat(),
                "interval": "1d",
                "auto_adjust": False,
                "threads": False,
                "timeout": 10,
                "progress": False,
                "multi_level_index": False,
                "backfill": backfill,
            }
        else:
            parameters = {
                "series_id": entity.key,
                "observation_start": bounds.start.isoformat(),
                "observation_end": bounds.end.isoformat(),
                "file_type": "json",
                "limit": 100000,
                "timeout": 10,
                "backfill": backfill,
            }
        schema_version = (
            "yahoo_daily_v1"
            if entity.source == "yahoo_finance"
            else "fred_observations_v1"
        )
        self.metadata.insert_pending(
            run_id, entity, parameters, requested_at, schema_version, revision, backfill
        )
        try:
            fetched = self._fetch(entity, bounds)
            if fetched.row_count == 0:
                self.metadata.finish_terminal(run_id, "no_data")
                return self._summary(run_id, entity, bounds, "no_data", 0, None)
            if fetched.max_observation_date is None or not (
                bounds.start <= fetched.max_observation_date <= bounds.end
            ):
                raise ValueError("source returned invalid observation range")
            published = self.publisher.publish(
                run_id,
                entity,
                bounds,
                fetched,
                requested_at,
                revision,
                parameters,
            )
            self.metadata.finish_success(
                run_id,
                entity,
                fetched.max_observation_date,
                published.payload_uri,
                published.sha256,
                published.byte_count,
                fetched.row_count,
                lock_connection,
            )
            return self._summary(
                run_id,
                entity,
                bounds,
                "success",
                fetched.row_count,
                published.payload_uri,
            )
        except Exception as error:
            try:
                if self.metadata.run(run_id)["status"] == "pending":
                    self.metadata.finish_terminal(run_id, "failed", error)
            except Exception:
                # The committed pending row remains for explicit reconciliation.
                pass
            raise RuntimeError(f"ingestion failed: {type(error).__name__}") from None

    @staticmethod
    def _summary(
        run_id: UUID,
        entity: Entity,
        bounds: DateRange,
        status: str,
        row_count: int,
        payload_uri: str | None,
    ) -> dict:
        return {
            "ingestion_run_id": str(run_id),
            "source": entity.source,
            "entity": entity.key,
            "start": bounds.start.isoformat(),
            "end": bounds.end.isoformat(),
            "status": status,
            "row_count": row_count,
            "payload_uri": payload_uri,
        }

    def verify_consumable(self, run_id: UUID, entity: Entity) -> bool:
        run = self.metadata.run(run_id)
        if run is None or run["status"] != "success":
            return False
        manifest = self.publisher.verify_manifest(run_id, entity)
        payload = manifest["payload"]
        return (
            payload["uri"] == run["payload_uri"]
            and payload["sha256"] == run["sha256"]
            and payload["byte_count"] == run["byte_count"]
            and payload["row_count"] == run["row_count"]
        )
