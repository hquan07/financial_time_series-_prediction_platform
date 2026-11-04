# Data Contract v1

This is the Phase 1 contract for later ingestion and dataset builders. It is
documentation, not an implemented schema or an operational pipeline. Version
names are immutable once data is published under them.

## Market sessions and raw prices

One Silver market row is identified by `(ticker, exchange, session_date)`.
`session_date` is the exchange's local trading-session `DATE`, never a UTC
calendar date inferred from a timestamp. Exchange calendar and timezone must
be stored with the dataset, including early closes and holidays. Duplicate or
out-of-order sessions and non-session dates must fail validation.

| Field | Type | Meaning and validation |
|---|---|---|
| `ticker` | nonempty string | Provider symbol at ingestion; stable within dataset version. |
| `exchange` | MIC string | Exchange calendar identity, e.g. `XNAS`. |
| `session_date` | `DATE` | Exchange-local trading session. |
| `open`, `high`, `low`, `close` | positive finite decimal | Raw, unadjusted daily OHLC. `high >= max(open, close)` and `low <= min(open, close)`; also `high >= low`. |
| `volume` | nonnegative integer | Raw reported shares; zero is allowed. |
| `adj_close` | positive finite decimal | Provider adjusted close retained separately from raw close. |
| `ingested_at` | UTC `TIMESTAMPTZ` | Time the response was retrieved. |

For a row with nonzero raw `close`, `adjustment_factor = adj_close / close`.
If a later feature or label needs adjusted open, derive `adjusted_open =
open * adjustment_factor` from that **same session**. Preserve raw OHLCV;
do not silently mix adjusted and unadjusted prices. Record provider revision
and corporate-action behavior because historical prices can change.

## Forecast and target

`forecast_cutoff_at` is a UTC `TIMESTAMPTZ` after the validated close of
session `t`. The primary target `adj_open_to_close_v1` is
`adj_close(t+1) / adjusted_open(t+1) - 1` for the next valid exchange
session. Entry is its open; exit is its close. Features for row `t` may use
only records with recorded `available_at <= forecast_cutoff_at`. The target
is held separately from features and is unavailable at forecast time. If
the next session or required price is missing, the label is null and that
row is excluded from supervised training; do not substitute a calendar day.

Secondary direction and multi-session volatility targets are future contract
versions. `features_v1` reserves the first feature contract version without
claiming any feature has been built.

## Macro observations and availability

One macro observation has `series_id`, `observation_date` (`DATE`), `value`
(nullable finite decimal), `retrieved_at` (UTC `TIMESTAMPTZ`), source
`realtime_start`/`realtime_end` or vintage identifier when available, and
`available_at` (UTC `TIMESTAMPTZ`). A FRED `"."` value is missing, not zero.
The source record and policy version used to assign `available_at` must be
retained. Observation date alone never proves forecast-time availability.
If exact release evidence or a defensible conservative retrieval timestamp
is unavailable, exclude the observation from point-in-time features.

## Bronze, Silver, Gold and lineage

Bronze is immutable response evidence. Each run has `ingestion_run_id`
(UUID), provider, request parameters (with credentials redacted),
`requested_at`, `ingested_at`, response schema version, object URI, SHA-256
of stored bytes, byte count and row count. Store a manifest beside the raw
object and publish it only after both objects are durable. Never overwrite a
run ID or reuse a URI for different bytes.

Proposed object keys, all relative to the configured storage root:

```text
bronze/{provider}/{entity}/{ingestion_run_id}/response.json
bronze/{provider}/{entity}/{ingestion_run_id}/manifest.json
silver/{dataset_name}/{dataset_version}/part-{part_id}.parquet
silver/{dataset_name}/{dataset_version}/manifest.json
gold/{dataset_name}/{dataset_version}/part-{part_id}.parquet
gold/{dataset_name}/{dataset_version}/manifest.json
```

Each Silver/Gold manifest carries a unique immutable `dataset_version`,
sorted `source_run_ids`, parent dataset versions, `feature_contract_version`,
`target_contract_version`, source schema version, code revision, creation
time, row count and file checksums. A version is a unique UTC timestamp plus
content digest, e.g. `20261104T080000Z-a1b2c3d4`; the digest covers the
canonical manifest inputs. Consumers select a published manifest, never a
mutable directory listing. The later storage phase must specify atomic
publication and verify checksums on read.

## Fixtures

`tests/fixtures` contains invented, deterministic examples based on expected
field shapes. They are **not** downloaded market observations or evidence of
a successful provider request. Valid and invalid AAPL CSVs exercise ordered
sessions, OHLC bounds and volume. The VIXCLS JSON includes `"."` to reserve
the missing-value case. Tests must not call live services.
