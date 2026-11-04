from datetime import date

import pytest

from financial_time_series.processing.calendar import (
    calendar_for,
    missing_sessions,
    require_session,
)
from financial_time_series.processing.quality import QualityError


def test_xnas_alias_holiday_early_close_and_gap():
    require_session("XNAS", date(2024, 1, 2))
    require_session("XNYS", date(2024, 1, 2))
    with pytest.raises(QualityError, match="market_non_session_date"):
        require_session("XNAS", date(2024, 1, 1))
    assert missing_sessions("XNAS", [date(2024, 1, 2), date(2024, 1, 4)]) == [
        "2024-01-03"
    ]
    calendar = calendar_for("XNAS")
    assert calendar.session_close("2024-11-29") == calendar_for("XNYS").session_close(
        "2024-11-29"
    )
    assert (
        calendar.session_close("2024-11-29").hour
        < calendar.session_close("2024-11-27").hour
    )
