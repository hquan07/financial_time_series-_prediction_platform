from datetime import UTC, date, datetime

import pytest
from curl_cffi.requests import exceptions as curl_errors

from financial_time_series.ingestion.backfill import (
    chunk_ranges,
    latest_closed_date,
    validate_backfill,
)
from financial_time_series.ingestion.models import DateRange
from financial_time_series.ingestion.retry import (
    PermanentHTTPError,
    RetryableHTTPError,
    fetch_with_retry,
    is_retryable,
)


def test_backfill_chunks_are_inclusive_and_bounded():
    chunks = chunk_ranges(DateRange(date(2024, 1, 1), date(2025, 1, 2)), 366)
    assert chunks == [
        DateRange(date(2024, 1, 1), date(2024, 12, 31)),
        DateRange(date(2025, 1, 1), date(2025, 1, 2)),
    ]
    with pytest.raises(ValueError):
        DateRange(date(2025, 1, 2), date(2025, 1, 1))
    with pytest.raises(ValueError):
        validate_backfill(
            DateRange(date(2024, 1, 1), date(2025, 1, 2)), date(2025, 1, 1)
        )


def test_latest_closed_date_uses_exchange_timezone():
    now = datetime(2024, 1, 3, 1, tzinfo=UTC)
    assert latest_closed_date("America/New_York", now) == date(2024, 1, 1)
    assert latest_closed_date(None, now) == date(2024, 1, 2)


def test_retry_classification():
    assert is_retryable(RetryableHTTPError(429))
    assert is_retryable(RetryableHTTPError(503))
    assert is_retryable(curl_errors.Timeout("timed out"))
    assert not is_retryable(PermanentHTTPError(403))
    assert not is_retryable(ValueError("invalid schema"))


def test_retry_attempts_are_bounded_and_permanent_errors_stop():
    calls = []

    def transient():
        calls.append(1)
        raise RetryableHTTPError(429, retry_after=0)

    with pytest.raises(RetryableHTTPError):
        fetch_with_retry(transient)
    assert len(calls) == 3

    calls.clear()

    def permanent():
        calls.append(1)
        raise PermanentHTTPError(403)

    with pytest.raises(PermanentHTTPError):
        fetch_with_retry(permanent)
    assert len(calls) == 1
