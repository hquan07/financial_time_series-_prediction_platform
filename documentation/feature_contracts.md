# Gold feature contracts

`features_v1` takes two explicitly published Silver versions. Market rows are
grouped by `(ticker, exchange)` and sorted by exchange session. The stored
`forecast_cutoff_at` is the exchange calendar's session close in UTC, including
early closes. `market_point_in_time_verified` is true only when the selected
market row's retrieval timestamp is no later than that cutoff. Historical
Yahoo backfills commonly have this flag false and remain research snapshots.

Price returns, simple/EMA trend, RSI, MACD and rolling statistics use adjusted
close. Range, gap, ATR, volume statistics and OBV use raw OHLCV. `EMA` uses
`adjust=False` with `min_periods=span`; RSI and ATR use 14 observations for
their arithmetic seed and Wilder smoothing thereafter. Rolling standard
deviation uses `ddof=1`. All undefined warmup and zero-denominator values
remain null.

For VIXCLS and DGS10, select the greatest observation date no later than the
market session whose recorded `available_at` is no later than the forecast
cutoff. Revisions of the same observation date are ordered by `available_at`,
`retrieved_at`, then source-run UUID. A selected source null remains null.
`macro_ready` requires both levels and changes; `price_only_ready` requires
all price, technical, statistical and volume features. `feature_ready`
requires both tiers. Historical FRED retrievals from 2026 do not become
historically available in 2024 merely because their observation dates are old.

`adj_open_to_close_v1` labels the exact next exchange session when that market
row exists. It is `adj_close / (open * adj_close / close) - 1` for that next
session. The factor cancels algebraically, so this is the raw next-session
open-to-close return. The separate label dataset includes price-only ready
rows and retains `macro_ready`, `feature_ready` and
`market_point_in_time_verified` for downstream selection.

Gold parts, report and manifest are immutable. The manifest records the
resolved config, parent dataset IDs and hashes, code revision, writer options,
column types and readiness counts. PostgreSQL `dataset_parents` holds direct
lineage. Published versions must be selected explicitly.
