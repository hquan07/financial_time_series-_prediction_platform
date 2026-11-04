"""Keep the small offline fixtures aligned with Data Contract v1."""

import csv
import json
from datetime import date
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def _market_rows(filename: str) -> list[dict[str, str]]:
    with (FIXTURES / filename).open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def _valid_market_rows(rows: list[dict[str, str]]) -> bool:
    sessions = [
        (row["ticker"], row["exchange"], date.fromisoformat(row["session_date"]))
        for row in rows
    ]
    if sessions != sorted(set(sessions)):
        return False
    for row in rows:
        open_, high, low, close, adjusted = (
            float(row[key]) for key in ("open", "high", "low", "close", "adj_close")
        )
        if not (low > 0 and open_ > 0 and close > 0 and adjusted > 0):
            return False
        if high < max(open_, close) or low > min(open_, close) or high < low:
            return False
        if int(row["volume"]) < 0:
            return False
    return True


def test_market_fixtures_have_expected_quality() -> None:
    assert _valid_market_rows(_market_rows("aapl_daily.csv"))
    assert not _valid_market_rows(_market_rows("aapl_daily_invalid.csv"))


def test_fred_fixture_preserves_missing_value() -> None:
    response = json.loads((FIXTURES / "vixcls_observations.json").read_text())
    observations = response["observations"]
    assert [date.fromisoformat(item["date"]) for item in observations] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]
    assert [item["value"] for item in observations] == ["12.34", ".", "14.56"]
