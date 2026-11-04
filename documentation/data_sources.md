# Source policy and Phase 1 snapshot evidence

## Intended sources

Yahoo Finance via `yfinance` is the proposed daily OHLCV source for research.
The adapter must request `auto_adjust=False` to retain raw OHLC and separate
adjusted close. The `download` API defaults can change, and its documented
`multi_level_index` option means callers must inspect the actual column
shape rather than assume a flat response. Yahoo/yfinance provide no project
SLA; review Yahoo's rights and terms before redistribution or commercial use.
Reference: [yfinance download documentation](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html).

FRED is the proposed source for VIXCLS and DGS10. Its API requires a key and
returns observation date/value and real-time vintage fields. FRED and some
underlying series owners restrict reuse; retain attribution and check each
series' rights. References: [FRED observations API](https://fred.stlouisfed.org/docs/api/fred/series_observations.html),
[FRED API terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html),
[FRED terms](https://fred.stlouisfed.org/legal/terms/).

For VIXCLS and DGS10, `verified_release_or_retrieval_v1` assigns
`available_at` from a verified release timestamp when one exists. Otherwise
it uses the UTC retrieval time as a conservative lower bound and records
that policy, the retrieval time and vintage. This cannot establish
historical point-in-time availability for old observations retrieved today.
Such rows remain EDA-only until point-in-time release/vintage evidence is
available. A macro as-of join requires `available_at <= forecast_cutoff_at`.

## Manual provider snapshot, 2026-11-04

| Item | AAPL daily | VIXCLS |
|---|---|---|
| Planned request | `yf.download("AAPL", start="2024-01-02", end="2024-01-06", interval="1d", auto_adjust=False, progress=False, multi_level_index=False)` | `GET /fred/series/observations?series_id=VIXCLS&observation_start=2024-01-02&observation_end=2024-01-05&file_type=json` |
| Expected response fields | Date index; Open, High, Low, Close, Adj Close, Volume | `observations[]` with `date`, `value`, `realtime_start`, `realtime_end` |
| Missing value | Null/NaN rows must be rejected or documented | `"."` means missing |
| Acceptance | Ordered unique exchange sessions, positive finite OHLC, bounds, nonnegative integer volume | Parse date/value, preserve missing and vintage; assign availability by policy above |
| Live result | Retrieved and checked: 4 rows | Authenticated JSON retrieved and checked: 5 rows (expanded request begins 2024-01-01 to include a missing-value case) |

At **2026-11-04 07:34:44 UTC**, `yfinance` **0.2.66** returned four AAPL
daily rows for 2024-01-02 through 2024-01-05 with columns `Adj Close`,
`Close`, `High`, `Low`, `Open`, `Volume`. Parameters were the planned request
above plus `threads=False`, `timeout=10`. Dates were ordered and unique;
all five price columns were finite and positive; OHLC bounds and nonnegative
volume passed. This small check does not establish completeness across a
long historical window or a provider SLA.

At **2026-11-04 07:35:20 UTC**, a FRED observations API request with the
planned parameters and no `api_key` returned HTTP **400** with
`Variable api_key is not set`. The official [API key documentation](https://fred.stlouisfed.org/docs/api/api_key.html)
requires a registered key. The key was later supplied in the ignored local
`.env` file; no key was logged or committed.

At **2026-11-04 07:52:35 UTC**, `requests` **2.34.2** fetched VIXCLS from
`/fred/series/observations` with `series_id=VIXCLS`, `file_type=json`,
`observation_start=2024-01-01` and `observation_end=2024-01-05`. The range
was expanded one day earlier than planned to include a missing-value case.
HTTP **200** returned five observations dated 2024-01-01 through
2024-01-05. The top-level response included `observations`, count, date
range, real-time range, sort and units metadata. Each observation had
`date`, `value`, `realtime_start` and `realtime_end`; dates were ordered and
unique, four numeric values were positive and finite, and one value was
`"."`. Both real-time fields on these observations were `2026-11-04`, the
request's current vintage; they do **not** prove what was available in 2024.
Under `verified_release_or_retrieval_v1`, these observations receive no
earlier `available_at` than the recorded retrieval time, so they cannot join
2024 feature rows at historical forecast cutoffs. A later phase must obtain
release/vintage evidence before point-in-time claims.

As a limited public check, `GET https://fred.stlouisfed.org/graph/fredgraph.csv`
with `id=VIXCLS`, `cosd=2024-01-02`, `coed=2024-01-05` returned HTTP **200**.
Its header was `observation_date,VIXCLS`; four dates from 2024-01-02 through
2024-01-05 had numeric values (`13.20`, `14.04`, `14.13`, `13.35`). There was
no missing value in this sample and the CSV has no release/vintage timestamp.
The CSV does not establish forecast-time availability; the authenticated
JSON check above establishes the response shape and missing-value encoding,
but still not historical release-time availability. This snapshot is
evidence gathering only; no source client or data pipeline was implemented.

The committed fixtures are synthetic and deterministic, not copies of these
responses. No provider credentials or raw response are committed.
