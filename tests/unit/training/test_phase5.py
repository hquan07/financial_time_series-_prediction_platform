"""Chronological split and candidate isolation on invented observations."""

from datetime import date
from math import sin

import exchange_calendars as xcals
import pytest

from financial_time_series.features.compute import PRICE_FEATURES
from financial_time_series.settings import PROJECT_ROOT
from financial_time_series.training.runner import load_config, run
from financial_time_series.training.splits import make_splits


def invented_rows(count=580):
    calendar = xcals.get_calendar("XNAS")
    days = [x.date() for x in calendar.sessions_in_range("2024-01-02", "2026-11-04")]
    rows = []
    for index, day in enumerate(days[:count]):
        for ticker, offset in (("AAPL", 0), ("MSFT", 1)):
            rows.append(
                {
                    "ticker": ticker,
                    "exchange": "XNAS",
                    "as_of_session": day,
                    "target_session": calendar.next_session(day.isoformat()).date(),
                    "target_return": sin(index / 3 + offset) * 0.01,
                    **{
                        name: float(index % 30 + offset + position / 10)
                        for position, name in enumerate(PRICE_FEATURES)
                    },
                }
            )
    return rows


def test_full_session_split_and_insufficient_history():
    config = load_config(PROJECT_ROOT / "config/model_config.yaml")
    rows = invented_rows()
    folds, holdout = make_splits(rows, config)
    assert len(holdout) == 116
    assert [len(f.train_dates) for f in folds] == [275, 338, 401]
    assert all(len(f.validation_dates) == 63 for f in folds)
    assert all(f.train_dates[-1] < f.validation_dates[0] for f in folds)
    with pytest.raises(ValueError, match="insufficient_history"):
        make_splits(invented_rows(400), config)
    with pytest.raises(ValueError, match="complete session grid"):
        make_splits(rows[:-1], config)


def test_all_candidates_use_validation_before_single_final_holdout():
    config = load_config(PROJECT_ROOT / "config/model_config.yaml")
    rows = invented_rows()
    predictions, metrics, folds, result = run(rows, config)
    assert len(folds) == 3
    assert metrics["selected_regression_candidate"] in metrics["development_oof"]
    assert (
        len(
            [
                p
                for p in predictions
                if p["candidate"] == "zero_return" and p["phase"] == "holdout"
            ]
        )
        == 232
    )
    assert (
        len(
            [
                p
                for p in predictions
                if p["candidate"] == "logistic_regression"
                and p["phase"] == "validation"
            ]
        )
        == 378
    )
    assert all(p["target_session"] > p["as_of_session"] for p in predictions)
    assert result["split"]["holdout_sessions"] == 116


def test_target_must_not_cross_validation_start():
    config = load_config(PROJECT_ROOT / "config/model_config.yaml")
    rows = invented_rows()
    rows[2 * 274]["target_session"] = date(2026, 1, 1)
    with pytest.raises(ValueError, match="train_target_crosses_validation"):
        make_splits(rows, config)


def test_holdout_labels_cannot_change_candidate_selection():
    config = load_config(PROJECT_ROOT / "config/model_config.yaml")
    rows = invented_rows()
    _, original, _, info = run(rows, config)
    first_holdout = date.fromisoformat(info["split"]["holdout_start"])
    changed = [
        {
            **row,
            "target_return": -row["target_return"]
            if row["as_of_session"] >= first_holdout
            else row["target_return"],
        }
        for row in rows
    ]
    _, altered, _, _ = run(changed, config)
    assert (
        original["selected_regression_candidate"]
        == altered["selected_regression_candidate"]
    )
    assert original["development_oof"] == altered["development_oof"]
