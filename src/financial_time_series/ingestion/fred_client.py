"""Authenticated FRED observations with raw JSON Bronze payload."""

import json
import math
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime

import requests

from financial_time_series.ingestion.models import DateRange, FetchResult
from financial_time_series.ingestion.retry import (
    PermanentHTTPError,
    RetryableHTTPError,
    SourceSchemaError,
    fetch_with_retry,
)

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return None
            return max(0.0, (deadline - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError):
            return None


def _parse_payload(payload: bytes, bounds: DateRange) -> tuple[int, date | None]:
    try:
        document = json.loads(payload)
        observations = document["observations"]
        count = int(document["count"])
        offset = int(document["offset"])
        limit = int(document["limit"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        raise SourceSchemaError("FRED response has invalid JSON metadata") from error
    if not isinstance(observations, list) or offset != 0 or limit < 1:
        raise SourceSchemaError("FRED response has invalid pagination")
    if count != len(observations) or len(observations) > limit:
        raise SourceSchemaError("FRED response is incomplete")
    dates = []
    for observation in observations:
        try:
            item_date = date.fromisoformat(observation["date"])
            date.fromisoformat(observation["realtime_start"])
            date.fromisoformat(observation["realtime_end"])
            value = observation["value"]
            if value != "." and not math.isfinite(float(value)):
                raise ValueError("nonfinite value")
        except (KeyError, TypeError, ValueError) as error:
            raise SourceSchemaError("FRED observation has invalid fields") from error
        if not bounds.start <= item_date <= bounds.end:
            raise SourceSchemaError("FRED observation is outside requested range")
        dates.append(item_date)
    if dates != sorted(set(dates)):
        raise SourceSchemaError("FRED dates are duplicate or out of order")
    return len(dates), dates[-1] if dates else None


class FredClient:
    def __init__(self, api_key: str, session: requests.Session | None = None):
        if not api_key:
            raise ValueError("FRED_API_KEY must be configured")
        self._api_key = api_key
        self._session = session or requests.Session()

    def fetch(self, series_id: str, bounds: DateRange) -> FetchResult:
        params = {
            "series_id": series_id,
            "observation_start": bounds.start.isoformat(),
            "observation_end": bounds.end.isoformat(),
            "file_type": "json",
            "limit": 100000,
            "api_key": self._api_key,
        }

        def get_response() -> bytes:
            response = self._session.get(FRED_URL, params=params, timeout=10)
            if response.status_code in (408, 429) or response.status_code >= 500:
                raise RetryableHTTPError(
                    response.status_code,
                    _retry_after(response.headers.get("Retry-After")),
                )
            if response.status_code >= 400:
                raise PermanentHTTPError(response.status_code)
            return response.content

        payload = fetch_with_retry(get_response)
        row_count, max_date = _parse_payload(payload, bounds)
        return FetchResult(
            payload=payload,
            row_count=row_count,
            max_observation_date=max_date,
            payload_format="json",
            source_format="json_http_body",
            schema_version="fred_observations_v1",
            source_version="fred_api_v1",
        )
