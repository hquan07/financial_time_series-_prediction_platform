"""Gold feature and label contracts on deterministic synthetic rows."""

from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pandas as pd

from financial_time_series.features.compute import (
    build_rows,
    build_training_rows,
    select_macro,
)


def market_rows(count=45, ticker="AAPL"):
    sessions = pd.bdate_range("2024-01-02", periods=count)
    return [
        {
            "ticker": ticker,
            "exchange": "XNAS",
            "session_date": day.date(),
            "open": 100.0 + index,
            "high": 102.0 + index,
            "low": 99.0 + index,
            "close": 101.0 + index,
            "adj_close": 101.0 + index,
            "volume": 1000 + index * 10,
            "source_run_id": uuid4(),
            "ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        for index, day in enumerate(sessions)
        if day.date() not in {date(2024, 1, 15), date(2024, 2, 19)}
    ]


def test_calendar_gap_does_not_become_next_session_label():
    rows = market_rows()
    features, labels = build_rows(rows, [])
    assert len(features) == len(rows)
    assert all(not row["feature_ready"] for row in features)
    assert any(row["price_only_ready"] for row in features)
    assert all(row["target_session"] > row["as_of_session"] for row in labels)
    assert not any(row["as_of_session"] == date(2024, 1, 12) for row in labels)


def test_macro_selects_observation_date_then_revision_and_ignores_future():
    now = datetime(2024, 1, 10, 21, tzinfo=UTC)
    rows = pd.DataFrame(
        [
            {
                "series_id": "VIXCLS",
                "observation_date": date(2024, 1, 8),
                "available_at": now - timedelta(days=1),
                "retrieved_at": now,
                "source_run_id": "a",
                "value": 10.0,
            },
            {
                "series_id": "VIXCLS",
                "observation_date": date(2024, 1, 9),
                "available_at": now - timedelta(days=1),
                "retrieved_at": now,
                "source_run_id": "b",
                "value": None,
            },
            {
                "series_id": "VIXCLS",
                "observation_date": date(2024, 1, 10),
                "available_at": now + timedelta(days=1),
                "retrieved_at": now,
                "source_run_id": "c",
                "value": 12.0,
            },
        ]
    )
    chosen = select_macro(rows, "VIXCLS", date(2024, 1, 10), now)
    assert chosen.source_run_id == "b"
    assert pd.isna(chosen.value)


def test_training_uses_published_feature_rows_and_exact_next_session():
    rows = market_rows()
    features, original = build_rows(rows, [])
    rebuilt = build_training_rows(features, rows)
    assert [(row["as_of_session"], row["target_session"]) for row in rebuilt] == [
        (row["as_of_session"], row["target_session"]) for row in original
    ]
    assert all(
        row["target_return"] == original[index]["target_return"]
        for index, row in enumerate(rebuilt)
    )
    assert all(
        row["source_run_id"] == original[index]["source_run_id"]
        for index, row in enumerate(rebuilt)
    )
