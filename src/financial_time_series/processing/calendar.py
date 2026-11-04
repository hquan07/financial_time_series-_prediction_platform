"""Exchange-local session checks."""

from datetime import date

import exchange_calendars as xcals

from financial_time_series.processing.quality import QualityError


def calendar_for(exchange: str):
    if exchange not in {"XNAS", "XNYS"}:
        raise QualityError("unsupported_exchange")
    return xcals.get_calendar(exchange)


def require_session(exchange: str, session_date: date) -> None:
    try:
        valid = calendar_for(exchange).is_session(session_date.isoformat())
    except (TypeError, ValueError) as error:
        raise QualityError("calendar_date_out_of_range") from error
    if not valid:
        raise QualityError("market_non_session_date")


def missing_sessions(exchange: str, dates: list[date]) -> list[str]:
    if len(dates) < 2:
        return []
    calendar = calendar_for(exchange)
    sessions = calendar.sessions_in_range(dates[0].isoformat(), dates[-1].isoformat())
    observed = set(dates)
    return [item.date().isoformat() for item in sessions if item.date() not in observed]
