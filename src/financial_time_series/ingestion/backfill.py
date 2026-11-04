"""Inclusive date bounds and deterministic request chunking."""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from financial_time_series.ingestion.models import DateRange


def latest_closed_date(
    exchange_timezone: str | None, now: datetime | None = None
) -> date:
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(ZoneInfo(exchange_timezone) if exchange_timezone else UTC)
    return local.date() - timedelta(days=1)


def validate_backfill(bounds: DateRange, latest: date) -> None:
    if bounds.end > latest:
        raise ValueError("end must be no later than the previous local date")


def chunk_ranges(bounds: DateRange, max_days: int = 366) -> list[DateRange]:
    if max_days < 1:
        raise ValueError("max_days must be positive")
    chunks = []
    start = bounds.start
    while start <= bounds.end:
        end = min(start + timedelta(days=max_days - 1), bounds.end)
        chunks.append(DateRange(start, end))
        start = end + timedelta(days=1)
    return chunks
