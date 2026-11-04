import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from financial_time_series.ingestion.service import resolve_entity
from financial_time_series.processing.bronze_reader import BronzeInput
from financial_time_series.processing.canonicalize import canonicalize_market
from financial_time_series.processing.macro_normalizer import normalize_macro
from financial_time_series.processing.market_normalizer import normalize_market
from financial_time_series.processing.quality import QualityError, QualityReport
from financial_time_series.processing.silver_service import (
    _version_input,
    dataset_version,
)
from financial_time_series.storage.parquet import encode_part

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
INGESTED = "2026-11-04T12:00:00+00:00"


def bronze(source: str, key: str, payload: bytes, count: int) -> BronzeInput:
    entity = resolve_entity(source, key)
    run_id = uuid4()
    manifest = {
        "ingested_at": INGESTED,
        "source_available_at": None,
        "availability_policy_version": entity.availability_policy_version,
        "requested_observation_range": {"start": "2024-01-01", "end": "2024-01-05"},
        "payload": {"row_count": count},
    }
    return BronzeInput(run_id, datetime.now(UTC), manifest, "a" * 64, payload)


def test_market_normalization_canonicalization_and_parquet():
    payload = (FIXTURES / "yahoo_aapl_download.csv").read_bytes()
    entity = resolve_entity("yahoo_finance", "AAPL")
    first = bronze("yahoo_finance", "AAPL", payload, 4)
    rows = normalize_market(first, entity)
    assert len(rows) == 4
    assert rows[0]["session_date"].isoformat() == "2024-01-02"
    assert rows[0]["source_available_at"] is None
    second = bronze("yahoo_finance", "AAPL", payload, 4)
    later = normalize_market(second, entity)
    later[0]["close"] = 101.5
    later[0]["_completed_at"] = first.completed_at.replace(year=2027)
    report = QualityReport("market_ohlcv_v1", [])
    selected = canonicalize_market(rows + later, report)
    assert len(selected) == 4
    assert selected[0]["source_run_id"] == second.run_id
    assert len(report.duplicates) == 4
    assert len(report.warnings) == 1
    encoded = encode_part(selected, "market")
    assert encoded == encode_part(selected, "market")


def test_invalid_market_and_macro_availability():
    entity = resolve_entity("yahoo_finance", "AAPL")
    payload = (FIXTURES / "yahoo_aapl_download.csv").read_text()
    invalid = bronze(
        "yahoo_finance",
        "AAPL",
        payload.replace("102.00,99.00", "90.00,99.00").encode(),
        4,
    )
    with pytest.raises(QualityError, match="market_ohlc_bounds"):
        normalize_market(invalid, entity)
    holiday = bronze(
        "yahoo_finance", "AAPL", payload.replace("2024-01-02", "2024-01-01").encode(), 4
    )
    with pytest.raises(QualityError, match="market_non_session_date"):
        normalize_market(holiday, entity)
    macro = bronze(
        "fred", "VIXCLS", (FIXTURES / "fred_vixcls_response.json").read_bytes(), 5
    )
    rows = normalize_macro(macro, resolve_entity("fred", "VIXCLS"))
    assert len(rows) == 5
    assert rows[0]["value"] is None
    assert rows[0]["available_at"] == rows[0]["retrieved_at"]
    assert rows[0]["source_available_at"] is None
    assert encode_part(rows, "macro") == encode_part(rows, "macro")
    document = json.loads(macro.payload)
    document["observations"][1]["value"] = "NaN"
    malformed = bronze("fred", "VIXCLS", json.dumps(document).encode(), 5)
    with pytest.raises(QualityError, match="macro_invalid_value"):
        normalize_macro(malformed, resolve_entity("fred", "VIXCLS"))


def test_version_includes_every_input_and_cutoff():
    cutoff = datetime(2026, 11, 4, 12, tzinfo=UTC)
    first = bronze("fred", "VIXCLS", b"{}", 1)
    second = bronze("fred", "VIXCLS", b"{}", 1)
    one = _version_input("macro_observations_v1", cutoff, [first], "test")
    two = _version_input("macro_observations_v1", cutoff, [first, second], "test")
    assert dataset_version(one, cutoff) == dataset_version(one, cutoff)
    assert dataset_version(one, cutoff) != dataset_version(two, cutoff)
