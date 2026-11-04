"""Pandera checks for normalized Silver rows."""

import pandas as pd
import pandera.pandas as pa

from financial_time_series.processing.quality import QualityError

MARKET_SCHEMA = pa.DataFrameSchema(
    {
        "ticker": pa.Column(str, nullable=False),
        "exchange": pa.Column(str, nullable=False),
        "open": pa.Column(float, pa.Check.gt(0), nullable=False),
        "high": pa.Column(float, pa.Check.gt(0), nullable=False),
        "low": pa.Column(float, pa.Check.gt(0), nullable=False),
        "close": pa.Column(float, pa.Check.gt(0), nullable=False),
        "adj_close": pa.Column(float, pa.Check.gt(0), nullable=False),
        "volume": pa.Column(int, pa.Check.ge(0), nullable=False),
    },
    strict=False,
)

MACRO_SCHEMA = pa.DataFrameSchema(
    {
        "series_id": pa.Column(str, nullable=False),
        "value": pa.Column(float, nullable=True),
        "availability_policy_version": pa.Column(str, nullable=False),
    },
    strict=False,
)


def validate_market(rows: list[dict]) -> None:
    try:
        MARKET_SCHEMA.validate(pd.DataFrame(rows), lazy=True)
    except pa.errors.SchemaErrors as error:
        raise QualityError("market_schema_violation") from error


def validate_macro(rows: list[dict]) -> None:
    try:
        MACRO_SCHEMA.validate(pd.DataFrame(rows), lazy=True)
    except pa.errors.SchemaErrors as error:
        raise QualityError("macro_schema_violation") from error
