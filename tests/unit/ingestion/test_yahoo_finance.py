from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from financial_time_series.ingestion.models import DateRange
from financial_time_series.ingestion.retry import SourceSchemaError
from financial_time_series.ingestion.yahoo_finance import YahooFinanceClient, _export

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/yahoo_aapl_download.csv"


def _frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE, index_col="Date", parse_dates=True)


def test_yahoo_export_is_stable_and_end_is_exclusive_for_provider(monkeypatch):
    calls = []

    def fake_download(*args, **kwargs):
        calls.append((args, kwargs))
        return _frame()

    monkeypatch.setattr("yfinance.download", fake_download)
    bounds = DateRange(date(2024, 1, 2), date(2024, 1, 5))
    first = YahooFinanceClient().fetch("AAPL", bounds)
    second = YahooFinanceClient().fetch("AAPL", bounds)
    assert first.payload == second.payload
    assert first.row_count == 4
    assert first.max_observation_date == bounds.end
    assert first.payload.startswith(b"Date,Open,High,Low,Close,Adj Close,Volume\n")
    assert calls[0][1]["end"] == "2024-01-06"
    assert calls[0][1]["auto_adjust"] is False


def test_yahoo_multiindex_and_duplicate_dates():
    frame = _frame()
    frame.columns = pd.MultiIndex.from_product([frame.columns, ["AAPL"]])
    bounds = DateRange(date(2024, 1, 2), date(2024, 1, 5))
    assert _export(frame, "AAPL", bounds).row_count == 4
    duplicate = pd.concat([_frame(), _frame().iloc[[-1]]])
    with pytest.raises(SourceSchemaError, match="duplicate"):
        _export(duplicate, "AAPL", bounds)


def test_yahoo_missing_required_column():
    with pytest.raises(SourceSchemaError, match="missing"):
        _export(
            _frame().drop(columns="Adj Close"),
            "AAPL",
            DateRange(date(2024, 1, 2), date(2024, 1, 5)),
        )
