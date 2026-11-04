"""Strict Yahoo CSV to normalized raw OHLCV rows."""

import csv
import math
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO

from financial_time_series.ingestion.models import Entity
from financial_time_series.processing.bronze_reader import BronzeInput, utc_timestamp
from financial_time_series.processing.calendar import require_session
from financial_time_series.processing.contracts import validate_market
from financial_time_series.processing.quality import QualityError

HEADER = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]


def _price(value: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise QualityError("market_invalid_price") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise QualityError("market_invalid_price")
    return parsed


def _volume(value: str) -> int:
    try:
        parsed = Decimal(value)
    except (TypeError, InvalidOperation) as error:
        raise QualityError("market_invalid_volume") from error
    if not parsed.is_finite() or parsed < 0 or parsed != parsed.to_integral_value():
        raise QualityError("market_invalid_volume")
    return int(parsed)


def normalize_market(item: BronzeInput, entity: Entity) -> list[dict]:
    try:
        decoded = item.payload.decode("utf-8-sig")
        if any(not line.strip() for line in decoded.splitlines()):
            raise QualityError("market_blank_csv_line")
        reader = csv.DictReader(StringIO(decoded), strict=True)
        if reader.fieldnames != HEADER:
            raise QualityError("market_invalid_columns")
        rows = []
        previous = None
        ingested_at = utc_timestamp(item.manifest["ingested_at"])
        source_available = item.manifest["source_available_at"]
        source_available_at = (
            utc_timestamp(source_available) if source_available is not None else None
        )
        bounds = item.manifest["requested_observation_range"]
        start, end = (
            date.fromisoformat(bounds["start"]),
            date.fromisoformat(bounds["end"]),
        )
        for source in reader:
            if None in source or any(value is None for value in source.values()):
                raise QualityError("market_invalid_columns")
            session_date = date.fromisoformat(source["Date"])
            if not start <= session_date <= end:
                raise QualityError("market_date_outside_request")
            if previous is not None and session_date <= previous:
                raise QualityError("market_non_monotonic_or_duplicate_date")
            require_session(entity.manifest_entity["exchange"], session_date)
            prices = {
                field: _price(source[column])
                for field, column in (
                    ("open", "Open"),
                    ("high", "High"),
                    ("low", "Low"),
                    ("close", "Close"),
                    ("adj_close", "Adj Close"),
                )
            }
            if (
                prices["high"] < max(prices["open"], prices["close"])
                or prices["low"] > min(prices["open"], prices["close"])
                or prices["high"] < prices["low"]
            ):
                raise QualityError("market_ohlc_bounds")
            rows.append(
                {
                    "ticker": entity.key,
                    "exchange": entity.manifest_entity["exchange"],
                    "session_date": session_date,
                    **prices,
                    "volume": _volume(source["Volume"]),
                    "source": "yahoo_finance",
                    "source_run_id": item.run_id,
                    "ingested_at": ingested_at,
                    "source_available_at": source_available_at,
                    "schema_version": "market_ohlcv_v1",
                    "_completed_at": item.completed_at,
                }
            )
            previous = session_date
        if len(rows) != item.manifest["payload"]["row_count"]:
            raise QualityError("market_row_count_mismatch")
        validate_market(rows)
        return rows
    except (UnicodeError, csv.Error, KeyError, TypeError, ValueError) as error:
        if isinstance(error, QualityError):
            raise
        raise QualityError("market_unreadable_csv") from error
