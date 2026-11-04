"""PostgreSQL run audit, advisory locks, and monotonic watermarks."""

import hashlib
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from financial_time_series.ingestion.models import Entity
from financial_time_series.ingestion.retry import SourceError
from financial_time_series.settings import Settings


def lock_key(entity: Entity) -> int:
    raw = f"{entity.source}\0{entity.key}\0{entity.interval}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big", signed=True)


def silver_lock_key(dataset_name: str) -> int:
    raw = f"silver\0{dataset_name}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big", signed=True)


class MetadataStore:
    def __init__(self, settings: Settings):
        self._kwargs = settings.postgres_kwargs()

    def _connect(self, *, autocommit: bool = False) -> psycopg.Connection:
        return psycopg.connect(**self._kwargs, autocommit=autocommit)

    @contextmanager
    def entity_lock(self, entity: Entity, timeout_seconds: float = 10):
        connection = self._connect(autocommit=True)
        key = lock_key(entity)
        deadline = time.monotonic() + timeout_seconds
        try:
            while True:
                acquired = connection.execute(
                    "SELECT pg_try_advisory_lock(%s)", (key,)
                ).fetchone()[0]
                if acquired:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("entity advisory lock wait expired")
                time.sleep(0.1)
            yield connection
        finally:
            if not connection.closed:
                try:
                    connection.execute("SELECT pg_advisory_unlock(%s)", (key,))
                finally:
                    connection.close()

    @contextmanager
    def dataset_lock(self, dataset_name: str, timeout_seconds: float = 10):
        connection = self._connect(autocommit=True)
        key = silver_lock_key(dataset_name)
        deadline = time.monotonic() + timeout_seconds
        try:
            while not connection.execute(
                "SELECT pg_try_advisory_lock(%s)", (key,)
            ).fetchone()[0]:
                if time.monotonic() >= deadline:
                    raise TimeoutError("dataset advisory lock wait expired")
                time.sleep(0.1)
            yield
        finally:
            if not connection.closed:
                try:
                    connection.execute("SELECT pg_advisory_unlock(%s)", (key,))
                finally:
                    connection.close()

    def silver_candidates(self, source: str, keys: list[str], cutoff: datetime):
        with self._connect() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """SELECT ingestion_run_id, source_name, entity_key,
                              interval_name, status, completed_at, payload_uri,
                              sha256, byte_count, row_count, schema_version
                       FROM ingestion_runs
                       WHERE source_name=%s AND entity_key=ANY(%s)
                         AND interval_name='1d' AND status='success'
                         AND completed_at<=%s
                       ORDER BY ingestion_run_id""",
                    (source, keys, cutoff),
                )
                return cursor.fetchall()

    def silver_run_statuses(self, source: str, keys: list[str]) -> list[dict]:
        with self._connect() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """SELECT ingestion_run_id, status, completed_at
                       FROM ingestion_runs WHERE source_name=%s
                         AND entity_key=ANY(%s) AND interval_name='1d'""",
                    (source, keys),
                )
                return cursor.fetchall()

    def publication(self, dataset_name: str, version: str) -> dict | None:
        with self._connect() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """SELECT dataset_id, manifest_uri, manifest_sha256,
                              row_count, build_cutoff_at
                       FROM dataset_versions
                       WHERE layer='silver' AND dataset_name=%s
                         AND dataset_version=%s""",
                    (dataset_name, version),
                )
                return cursor.fetchone()

    def publication_sources(self, dataset_id: UUID) -> list[dict]:
        with self._connect() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """SELECT source_run_id, input_row_count, output_row_count
                       FROM dataset_source_runs WHERE dataset_id=%s
                       ORDER BY ordinal""",
                    (dataset_id,),
                )
                return cursor.fetchall()

    def start_quality(
        self, quality_id: UUID, dataset_name: str, cutoff: datetime, diagnostics: dict
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO quality_runs
                   (quality_run_id,dataset_name,build_cutoff_at,rule_version,
                    status,diagnostics)
                   VALUES (%s,%s,%s,'silver_quality_v1','pending',%s)""",
                (quality_id, dataset_name, cutoff, Jsonb(diagnostics)),
            )

    def fail_quality(
        self,
        quality_id: UUID,
        report_uri: str | None,
        report_sha: str | None,
        warning_count: int,
        failure_summary: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE quality_runs SET status='failed', fatal_count=1,
                   warning_count=%s, report_uri=%s, report_sha256=%s,
                   failure_summary=%s, completed_at=now()
                   WHERE quality_run_id=%s AND status='pending'""",
                (warning_count, report_uri, report_sha, failure_summary, quality_id),
            )

    def complete_publication(
        self,
        *,
        dataset_id: UUID,
        quality_id: UUID,
        dataset_name: str,
        version: str,
        cutoff: datetime,
        manifest_uri: str,
        manifest_sha: str,
        code_revision: str,
        row_count: int,
        source_counts: list[tuple[UUID, int, int]],
        report_uri: str,
        report_sha: str,
        warning_count: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO dataset_versions
                   (dataset_id,layer,dataset_name,dataset_version,build_cutoff_at,
                    manifest_uri,manifest_sha256,code_revision,contract_version,
                    schema_version,row_count,status)
                   VALUES (%s,'silver',%s,%s,%s,%s,%s,%s,'data_contract_v1',
                           %s,%s,'success')""",
                (
                    dataset_id,
                    dataset_name,
                    version,
                    cutoff,
                    manifest_uri,
                    manifest_sha,
                    code_revision,
                    dataset_name,
                    row_count,
                ),
            )
            for ordinal, (run_id, input_count, output_count) in enumerate(
                source_counts
            ):
                connection.execute(
                    """INSERT INTO dataset_source_runs
                       (dataset_id,source_run_id,ordinal,input_row_count,
                        output_row_count) VALUES (%s,%s,%s,%s,%s)""",
                    (dataset_id, run_id, ordinal, input_count, output_count),
                )
            changed = connection.execute(
                """UPDATE quality_runs SET status='success', dataset_version=%s,
                   dataset_id=%s, warning_count=%s, report_uri=%s,
                   report_sha256=%s, completed_at=now()
                   WHERE quality_run_id=%s AND status='pending'""",
                (
                    version,
                    dataset_id,
                    warning_count,
                    report_uri,
                    report_sha,
                    quality_id,
                ),
            )
            if changed.rowcount != 1:
                raise RuntimeError("quality run is no longer pending")

    def reuse_publication(
        self,
        quality_id: UUID,
        dataset_id: UUID,
        version: str,
        report_uri: str,
        report_sha: str,
        warning_count: int,
    ) -> None:
        with self._connect() as connection:
            changed = connection.execute(
                """UPDATE quality_runs SET status='success', dataset_version=%s,
                   dataset_id=%s, warning_count=%s, report_uri=%s,
                   report_sha256=%s, completed_at=now()
                   WHERE quality_run_id=%s AND status='pending'""",
                (
                    version,
                    dataset_id,
                    warning_count,
                    report_uri,
                    report_sha,
                    quality_id,
                ),
            )
            if changed.rowcount != 1:
                raise RuntimeError("quality run is no longer pending")

    def watermark(self, entity: Entity) -> date | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT max_observation_date FROM ingestion_watermarks
                   WHERE source_name=%s AND entity_key=%s AND interval_name=%s""",
                (entity.source, entity.key, entity.interval),
            ).fetchone()
            return row[0] if row else None

    def insert_pending(
        self,
        run_id: UUID,
        entity: Entity,
        params: dict,
        requested_at: datetime,
        schema_version: str,
        code_revision: str,
        backfill: bool,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO ingestion_runs
                   (ingestion_run_id, source_name, entity_key, interval_name,
                    request_parameters, requested_at, status, schema_version,
                    code_revision, backfill)
                   VALUES (%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s)""",
                (
                    run_id,
                    entity.source,
                    entity.key,
                    entity.interval,
                    Jsonb(params),
                    requested_at,
                    schema_version,
                    code_revision,
                    backfill,
                ),
            )

    def finish_success(
        self,
        run_id: UUID,
        entity: Entity,
        max_date: date,
        payload_uri: str,
        sha256: str,
        byte_count: int,
        row_count: int,
        lock_connection: psycopg.Connection,
    ) -> None:
        lock_connection.execute("SELECT 1")
        with self._connect() as connection:
            updated = connection.execute(
                """UPDATE ingestion_runs SET status='success', completed_at=%s,
                   payload_uri=%s, sha256=%s, byte_count=%s, row_count=%s
                   WHERE ingestion_run_id=%s AND status='pending'""",
                (
                    datetime.now(UTC),
                    payload_uri,
                    sha256,
                    byte_count,
                    row_count,
                    run_id,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("run is no longer pending")
            connection.execute(
                """INSERT INTO ingestion_watermarks
                   (source_name,entity_key,interval_name,max_observation_date,
                    last_advancing_run_id,version)
                   VALUES (%s,%s,%s,%s,%s,1)
                   ON CONFLICT (source_name,entity_key,interval_name)
                   DO UPDATE SET max_observation_date=EXCLUDED.max_observation_date,
                     last_advancing_run_id=EXCLUDED.last_advancing_run_id,
                     updated_at=now(),
                     version=ingestion_watermarks.version+1
                   WHERE ingestion_watermarks.max_observation_date
                         < EXCLUDED.max_observation_date""",
                (entity.source, entity.key, entity.interval, max_date, run_id),
            )

    def finish_terminal(
        self, run_id: UUID, status: str, error: Exception | None = None
    ) -> None:
        if status not in ("failed", "no_data", "abandoned"):
            raise ValueError("invalid terminal status")
        error_class = type(error).__name__ if error else None
        # Exception strings from HTTP/SDK clients may include secrets.
        error_message = (
            str(error)[:200] if isinstance(error, SourceError) else error_class
        )
        with self._connect() as connection:
            updated = connection.execute(
                """UPDATE ingestion_runs SET status=%s, completed_at=%s,
                   error_class=%s, error_message=%s
                   WHERE ingestion_run_id=%s AND status='pending'""",
                (status, datetime.now(UTC), error_class, error_message, run_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("run is no longer pending")

    def run(self, run_id: UUID) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT source_name, entity_key, interval_name, status,
                   payload_uri, sha256, byte_count, row_count
                   FROM ingestion_runs WHERE ingestion_run_id=%s""",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        keys = (
            "source",
            "entity",
            "interval",
            "status",
            "payload_uri",
            "sha256",
            "byte_count",
            "row_count",
        )
        return dict(zip(keys, row, strict=True))

    def abandon_stale(
        self, run_id: UUID, entity: Entity, older_than: timedelta
    ) -> bool:
        with self.entity_lock(entity):
            with self._connect() as connection:
                changed = connection.execute(
                    """UPDATE ingestion_runs SET status='abandoned', completed_at=now(),
                       error_class='StalePendingRun', error_message='StalePendingRun'
                       WHERE ingestion_run_id=%s AND source_name=%s AND entity_key=%s
                         AND interval_name=%s AND status='pending'
                         AND started_at < %s""",
                    (
                        run_id,
                        entity.source,
                        entity.key,
                        entity.interval,
                        datetime.now(UTC) - older_than,
                    ),
                )
                return changed.rowcount == 1
