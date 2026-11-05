"""Deterministic per-asset features and calendar-checked next-session labels."""

import math

import numpy as np
import pandas as pd

from financial_time_series.processing.calendar import calendar_for
from financial_time_series.processing.quality import QualityError

PRICE_FEATURES = (
    "return_1d",
    "return_5d",
    "log_return_1d",
    "sma_5",
    "sma_20",
    "ema_12",
    "ema_26",
    "sma_5_to_20",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_histogram",
    "return_std_5",
    "return_std_20",
    "return_mean_5",
    "return_mean_20",
    "rolling_high_20",
    "rolling_low_20",
    "high_low_range",
    "open_close_return",
    "overnight_gap",
    "atr_14",
    "volume_change",
    "volume_sma_20",
    "relative_volume_20",
    "volume_zscore_20",
    "obv",
)
MACRO_FEATURES = ("vix_level", "vix_change", "dgs10_level", "dgs10_change")


def _wilder(values: pd.Series, period: int) -> pd.Series:
    result = pd.Series(np.nan, index=values.index, dtype=float)
    if len(values) < period:
        return result
    seed = values.iloc[:period]
    if seed.isna().any():
        return result
    result.iloc[period - 1] = seed.mean()
    for position in range(period, len(values)):
        value = values.iloc[position]
        previous = result.iloc[position - 1]
        if pd.notna(value) and pd.notna(previous):
            result.iloc[position] = (previous * (period - 1) + value) / period
    return result


def _features_for_asset(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values("session_date").copy().reset_index(drop=True)
    if group.session_date.duplicated().any():
        raise QualityError("duplicate_market_session")
    close = group.adj_close.astype(float)
    raw_close = group.close.astype(float)
    volume = group.volume.astype(float)
    group["return_1d"] = close.pct_change(fill_method=None)
    group["return_5d"] = close.pct_change(5, fill_method=None)
    group["log_return_1d"] = np.log(close / close.shift())
    for window in (5, 20):
        group[f"sma_{window}"] = close.rolling(window, min_periods=window).mean()
        returns = group.return_1d.rolling(window, min_periods=window)
        group[f"return_std_{window}"] = returns.std(ddof=1)
        group[f"return_mean_{window}"] = returns.mean()
    group["rolling_high_20"] = close.rolling(20, min_periods=20).max()
    group["rolling_low_20"] = close.rolling(20, min_periods=20).min()
    group["sma_5_to_20"] = group.sma_5 / group.sma_20
    for span in (12, 26):
        group[f"ema_{span}"] = close.ewm(
            span=span, adjust=False, min_periods=span
        ).mean()
    group["macd"] = group.ema_12 - group.ema_26
    group["macd_signal"] = group.macd.ewm(span=9, adjust=False, min_periods=9).mean()
    group["macd_histogram"] = group.macd - group.macd_signal
    delta = close.diff()
    gain = _wilder(delta.clip(lower=0).iloc[1:].reset_index(drop=True), 14)
    loss = _wilder((-delta.clip(upper=0)).iloc[1:].reset_index(drop=True), 14)
    rsi = pd.Series(np.nan, index=group.index, dtype=float)
    for position in range(14, len(group)):
        g, loss_value = gain.iloc[position - 1], loss.iloc[position - 1]
        if pd.notna(g) and pd.notna(loss_value):
            rsi.iloc[position] = (
                50.0
                if g == loss_value == 0
                else 100.0
                if loss_value == 0
                else 100.0 - 100.0 / (1.0 + g / loss_value)
            )
    group["rsi_14"] = rsi
    group["high_low_range"] = (group.high - group.low) / raw_close
    group["open_close_return"] = raw_close / group.open - 1
    group["overnight_gap"] = group.open / raw_close.shift() - 1
    true_range = pd.concat(
        [
            (group.high - group.low),
            (group.high - raw_close.shift()).abs(),
            (group.low - raw_close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    group["atr_14"] = _wilder(true_range.reset_index(drop=True), 14)
    group["volume_change"] = volume / volume.shift() - 1
    group["volume_sma_20"] = volume.rolling(20, min_periods=20).mean()
    group["relative_volume_20"] = volume / group.volume_sma_20
    group["volume_zscore_20"] = (volume - group.volume_sma_20) / volume.rolling(
        20, min_periods=20
    ).std(ddof=1)
    group["obv"] = (np.sign(raw_close.diff()).fillna(0) * volume).cumsum()
    group["target_session"] = group.session_date.shift(-1)
    group["target_return"] = (
        group.adj_close.shift(-1)
        / (group.open.shift(-1) * group.adj_close.shift(-1) / raw_close.shift(-1))
        - 1
    )
    for index, row in group.iterrows():
        calendar = calendar_for(row.exchange)
        close_at = calendar.session_close(row.session_date.isoformat()).to_pydatetime()
        group.at[index, "forecast_cutoff_at"] = close_at
        if pd.notna(row.target_session):
            expected = calendar.next_session(row.session_date.isoformat()).date()
            if row.target_session != expected:
                group.at[index, "target_session"] = pd.NaT
                group.at[index, "target_return"] = np.nan
    group["market_point_in_time_verified"] = pd.to_datetime(
        group.ingested_at, utc=True
    ) <= pd.to_datetime(group.forecast_cutoff_at, utc=True)
    return group


def select_macro(macro: pd.DataFrame, series: str, session, cutoff):
    eligible = macro.loc[
        (macro.series_id == series)
        & (macro.observation_date <= session)
        & (macro.available_at <= cutoff)
    ]
    if eligible.empty:
        return None
    return eligible.sort_values(
        ["observation_date", "available_at", "retrieved_at", "source_run_id"]
    ).iloc[-1]


def build_rows(
    market_rows: list[dict], macro_rows: list[dict]
) -> tuple[list[dict], list[dict]]:
    market = pd.DataFrame(market_rows)
    macro = pd.DataFrame(macro_rows)
    if market.empty:
        raise QualityError("empty_market_parent")
    if not macro.empty:
        macro["available_at"] = pd.to_datetime(macro.available_at, utc=True)
        macro["retrieved_at"] = pd.to_datetime(macro.retrieved_at, utc=True)
        macro["source_run_id"] = macro.source_run_id.astype(str)
    features, labels = [], []
    for _, asset in market.groupby(["ticker", "exchange"], sort=True):
        frame = _features_for_asset(asset)
        previous = {"VIXCLS": None, "DGS10": None}
        for _, row in frame.iterrows():
            result = row.to_dict()
            reasons = []
            for name in PRICE_FEATURES:
                value = result[name]
                if not pd.notna(value) or not math.isfinite(float(value)):
                    result[name] = None
                    reasons.append(f"technical_{name}_unavailable")
            for series, prefix in (("VIXCLS", "vix"), ("DGS10", "dgs10")):
                chosen = (
                    select_macro(
                        macro, series, row.session_date, row.forecast_cutoff_at
                    )
                    if not macro.empty
                    else None
                )
                result[f"selected_{prefix}_source_run_id"] = None
                result[f"selected_{prefix}_available_at"] = None
                result[f"selected_{prefix}_observation_date"] = None
                level = (
                    None
                    if chosen is None or pd.isna(chosen.value)
                    else float(chosen.value)
                )
                change = None
                if chosen is not None:
                    result[f"selected_{prefix}_source_run_id"] = chosen.source_run_id
                    result[f"selected_{prefix}_available_at"] = (
                        chosen.available_at.to_pydatetime()
                    )
                    result[f"selected_{prefix}_observation_date"] = (
                        chosen.observation_date
                    )
                    if level is not None and previous[series] is not None:
                        last_date, last_value = previous[series]
                        change = (
                            0.0
                            if last_date == chosen.observation_date
                            else level - last_value
                        )
                    previous[series] = (
                        None if level is None else (chosen.observation_date, level)
                    )
                result[f"{prefix}_level"] = level
                result[f"{prefix}_change"] = change
                if level is None or change is None:
                    reasons.append(f"macro_{prefix}_unavailable")
            result["technical_ready"] = not any(
                reason.startswith("technical_") for reason in reasons
            )
            result["price_only_ready"] = result["technical_ready"]
            result["macro_ready"] = not any(
                reason.startswith("macro_") for reason in reasons
            )
            result["feature_ready"] = (
                result["technical_ready"] and result["macro_ready"]
            )
            result["feature_readiness_reasons"] = sorted(reasons)
            target_session = result.pop("target_session")
            target = result.pop("target_return")
            for key, value in list(result.items()):
                if isinstance(value, (np.integer, np.floating)):
                    result[key] = value.item()
            features.append(result)
            if (
                result["price_only_ready"]
                and pd.notna(target_session)
                and pd.notna(target)
                and math.isfinite(float(target))
            ):
                labels.append(
                    {
                        **result,
                        "as_of_session": row.session_date,
                        "target_session": target_session,
                        "target_return": float(target),
                        "label_ready": True,
                    }
                )
    return features, labels


def build_training_rows(
    feature_rows: list[dict], market_rows: list[dict]
) -> list[dict]:
    market = {
        (row["ticker"], row["exchange"], row["session_date"]): row
        for row in market_rows
    }
    if len(market) != len(market_rows):
        raise QualityError("duplicate_market_session")
    labels = []
    for feature in feature_rows:
        if not feature["price_only_ready"]:
            continue
        session = feature["session_date"]
        calendar = calendar_for(feature["exchange"])
        next_session = calendar.next_session(session.isoformat()).date()
        next_row = market.get((feature["ticker"], feature["exchange"], next_session))
        if next_row is None:
            continue
        if min(next_row["open"], next_row["close"], next_row["adj_close"]) <= 0:
            raise QualityError("invalid_target_price")
        adjusted_open = next_row["open"] * next_row["adj_close"] / next_row["close"]
        target = next_row["adj_close"] / adjusted_open - 1
        if not math.isfinite(target):
            raise QualityError("invalid_target_return")
        labels.append(
            {
                **feature,
                "as_of_session": session,
                "target_session": next_session,
                "target_return": target,
                "label_ready": True,
            }
        )
    return labels
