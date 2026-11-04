"""Daily Yahoo Finance DataFrame export to reproducible Bronze CSV."""

from datetime import timedelta
from io import StringIO

import pandas as pd
import requests
import yfinance as yf
from curl_cffi.requests import exceptions as curl_errors

from financial_time_series.ingestion.models import DateRange, FetchResult
from financial_time_series.ingestion.retry import (
    RetryableHTTPError,
    SourceError,
    SourceSchemaError,
    fetch_with_retry,
)

REQUIRED_COLUMNS = ("Open", "High", "Low", "Close", "Adj Close", "Volume")


def _flatten_columns(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    frame = frame.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        if frame.columns.nlevels != 2:
            raise SourceSchemaError("unexpected Yahoo column levels")
        if ticker in frame.columns.get_level_values(0):
            frame = frame[ticker]
        elif ticker in frame.columns.get_level_values(1):
            frame = frame.xs(ticker, axis=1, level=1)
        else:
            raise SourceSchemaError("Yahoo response does not contain requested ticker")
    if frame.columns.has_duplicates or any(
        col not in frame for col in REQUIRED_COLUMNS
    ):
        raise SourceSchemaError("Yahoo response has missing or duplicate columns")
    return frame.loc[:, REQUIRED_COLUMNS]


def _export(frame: pd.DataFrame, ticker: str, bounds: DateRange) -> FetchResult:
    if frame.empty:
        return FetchResult(
            b"",
            0,
            None,
            "csv",
            "csv_dataframe_export",
            "yahoo_daily_v1",
            yf.__version__,
        )
    frame = _flatten_columns(frame, ticker)
    if not isinstance(frame.index, pd.DatetimeIndex) or frame.index.hasnans:
        raise SourceSchemaError("Yahoo response requires a date index")
    dates = [item.date() for item in frame.index]
    if dates != sorted(set(dates)):
        raise SourceSchemaError("Yahoo dates are duplicate or out of order")
    if any(item < bounds.start or item > bounds.end for item in dates):
        raise SourceSchemaError("Yahoo response date is outside requested range")
    for column in REQUIRED_COLUMNS:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any():
            raise SourceSchemaError(f"Yahoo {column} contains a nonnumeric value")
        frame[column] = values
    if (frame[["Open", "High", "Low", "Close", "Adj Close"]] <= 0).any().any():
        raise SourceSchemaError("Yahoo prices must be positive")
    if (frame["Volume"] < 0).any():
        raise SourceSchemaError("Yahoo volume must be nonnegative")
    if (frame["High"] < frame[["Open", "Close"]].max(axis=1)).any() or (
        frame["Low"] > frame[["Open", "Close"]].min(axis=1)
    ).any():
        raise SourceSchemaError("Yahoo OHLC bounds are invalid")
    output = StringIO(newline="")
    frame.to_csv(
        output, index_label="Date", date_format="%Y-%m-%d", lineterminator="\n"
    )
    return FetchResult(
        payload=output.getvalue().encode("utf-8"),
        row_count=len(frame),
        max_observation_date=dates[-1],
        payload_format="csv",
        source_format="csv_dataframe_export",
        schema_version="yahoo_daily_v1",
        source_version=yf.__version__,
        column_shape=list(frame.columns),
    )


class YahooFinanceClient:
    def fetch(self, ticker: str, bounds: DateRange) -> FetchResult:
        def download() -> pd.DataFrame:
            try:
                return yf.download(
                    ticker,
                    start=bounds.start.isoformat(),
                    end=(bounds.end + timedelta(days=1)).isoformat(),
                    interval="1d",
                    auto_adjust=False,
                    threads=False,
                    timeout=10,
                    progress=False,
                    multi_level_index=False,
                )
            except Exception as error:
                if type(error).__name__ == "YFRateLimitError":
                    raise RetryableHTTPError(429) from None
                if isinstance(
                    error,
                    (
                        requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                        curl_errors.Timeout,
                        curl_errors.ConnectionError,
                    ),
                ):
                    raise
                if isinstance(error, SourceError):
                    raise
                raise SourceError(
                    f"Yahoo fetch failed: {type(error).__name__}"
                ) from None

        return _export(fetch_with_retry(download), ticker, bounds)
