import json
from datetime import date
from pathlib import Path

import pytest

from financial_time_series.ingestion.fred_client import FredClient, _retry_after
from financial_time_series.ingestion.models import DateRange
from financial_time_series.ingestion.retry import SourceSchemaError

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/fred_vixcls_response.json"


class Response:
    status_code = 200
    headers = {}

    def __init__(self, body):
        self.content = body


class Session:
    def __init__(self, body):
        self.body = body
        self.params = None

    def get(self, _url, params, timeout):
        self.params = params
        assert timeout == 10
        return Response(self.body)


def _body():
    document = json.loads(FIXTURE.read_text())
    document["count"] = len(document["observations"])
    document["offset"] = 0
    document["limit"] = 100000
    return json.dumps(document).encode()


def test_fred_keeps_raw_json_and_vintage_fields():
    session = Session(_body())
    result = FredClient("test-key", session).fetch(
        "VIXCLS", DateRange(date(2024, 1, 1), date(2024, 1, 5))
    )
    assert result.payload == session.body
    assert result.row_count == 5
    assert result.max_observation_date == date(2024, 1, 5)
    assert any(
        item["value"] == "." for item in json.loads(result.payload)["observations"]
    )
    assert session.params["api_key"] == "test-key"


def test_fred_missing_key_and_truncated_response():
    with pytest.raises(ValueError, match="FRED_API_KEY"):
        FredClient("")
    document = json.loads(_body())
    document["count"] += 1
    with pytest.raises(SourceSchemaError, match="incomplete"):
        FredClient("test-key", Session(json.dumps(document).encode())).fetch(
            "VIXCLS", DateRange(date(2024, 1, 1), date(2024, 1, 5))
        )


def test_retry_after_accepts_seconds_and_http_date():
    assert _retry_after("5") == 5
    assert _retry_after("not-a-date") is None
    assert _retry_after("Wed, 21 Oct 2037 07:28:00 GMT") > 0
