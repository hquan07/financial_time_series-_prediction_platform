"""Explicit Arrow schemas and stable Parquet part bytes."""

from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

WRITER_OPTIONS = {
    "version": "2.6",
    "data_page_version": "1.0",
    "compression": "zstd",
    "compression_level": 3,
    "use_dictionary": False,
    "write_statistics": True,
    "store_schema": True,
    "row_group_size": 65536,
    "coerce_timestamps": "us",
    "allow_truncated_timestamps": False,
    "use_deprecated_int96_timestamps": False,
}

MARKET_ARROW = pa.schema(
    [
        ("ticker", pa.string()),
        ("exchange", pa.string()),
        ("session_date", pa.date32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
        ("source", pa.string()),
        ("source_run_id", pa.uuid()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_available_at", pa.timestamp("us", tz="UTC")),
        ("schema_version", pa.string()),
    ]
)

MACRO_ARROW = pa.schema(
    [
        ("series_id", pa.string()),
        ("observation_date", pa.date32()),
        ("value", pa.float64()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("available_at", pa.timestamp("us", tz="UTC")),
        ("source_available_at", pa.timestamp("us", tz="UTC")),
        ("realtime_start", pa.date32()),
        ("realtime_end", pa.date32()),
        ("availability_policy_version", pa.string()),
        ("source_run_id", pa.uuid()),
        ("schema_version", pa.string()),
    ]
)


def encode_part(rows: list[dict], dataset: str) -> bytes:
    schema = MARKET_ARROW if dataset == "market" else MACRO_ARROW
    table = pa.Table.from_pylist(rows, schema=schema)
    output = BytesIO()
    pq.write_table(table, output, **WRITER_OPTIONS)
    encoded = output.getvalue()
    restored = pq.read_table(BytesIO(encoded))
    if not restored.schema.equals(schema) or restored.num_rows != len(rows):
        raise OSError("Parquet readback schema or row count mismatch")
    return encoded
