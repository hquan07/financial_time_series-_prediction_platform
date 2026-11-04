"""Strict FRED JSON to source-preserving macro rows."""

import json
import math
from datetime import date

from financial_time_series.ingestion.models import Entity
from financial_time_series.processing.bronze_reader import (
    BronzeInput,
    observation_bounds,
    utc_timestamp,
)
from financial_time_series.processing.contracts import validate_macro
from financial_time_series.processing.quality import QualityError

POLICY = "verified_release_or_retrieval_v1"


def normalize_macro(item: BronzeInput, entity: Entity) -> list[dict]:
    try:
        manifest = item.manifest
        if manifest["availability_policy_version"] != POLICY or (
            entity.availability_policy_version != POLICY
        ):
            raise QualityError("macro_unknown_availability_policy")
        retrieved_at = utc_timestamp(manifest["ingested_at"])
        source_available = manifest["source_available_at"]
        source_available_at = (
            utc_timestamp(source_available) if source_available is not None else None
        )
        available_at = source_available_at or retrieved_at
        document = json.loads(item.payload)
        observations = document["observations"]
        if (
            not isinstance(observations, list)
            or int(document["offset"]) != 0
            or int(document["limit"]) < len(observations)
            or int(document["count"]) != len(observations)
        ):
            raise QualityError("macro_invalid_pagination")
        start, end = observation_bounds(manifest)
        rows = []
        previous = None
        for source in observations:
            observation_date = date.fromisoformat(source["date"])
            realtime_start = date.fromisoformat(source["realtime_start"])
            realtime_end = date.fromisoformat(source["realtime_end"])
            if not start <= observation_date <= end:
                raise QualityError("macro_date_outside_request")
            if previous is not None and observation_date <= previous:
                raise QualityError("macro_non_monotonic_or_duplicate_date")
            if realtime_start > realtime_end:
                raise QualityError("macro_invalid_vintage")
            value = None if source["value"] == "." else float(source["value"])
            if value is not None and not math.isfinite(value):
                raise QualityError("macro_invalid_value")
            rows.append(
                {
                    "series_id": entity.key,
                    "observation_date": observation_date,
                    "value": value,
                    "retrieved_at": retrieved_at,
                    "available_at": available_at,
                    "source_available_at": source_available_at,
                    "realtime_start": realtime_start,
                    "realtime_end": realtime_end,
                    "availability_policy_version": POLICY,
                    "source_run_id": item.run_id,
                    "schema_version": "macro_observations_v1",
                }
            )
            previous = observation_date
        if len(rows) != manifest["payload"]["row_count"]:
            raise QualityError("macro_row_count_mismatch")
        validate_macro(rows)
        return rows
    except (
        UnicodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, QualityError):
            raise
        raise QualityError("macro_unreadable_json") from error
