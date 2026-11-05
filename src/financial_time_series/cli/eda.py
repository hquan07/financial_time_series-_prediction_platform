"""Version-pinned EDA notebooks and synthetic offline execution check."""

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import matplotlib
import nbformat
import pandas as pd
from nbclient import NotebookClient

matplotlib.use("Agg")

from financial_time_series.features.gold_service import GoldService
from financial_time_series.ingestion.service import code_revision
from financial_time_series.settings import PROJECT_ROOT, Settings


def synthetic_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=60)
    return pd.DataFrame(
        {
            "session_date": dates,
            "ticker": ["AAPL"] * len(dates),
            "adj_close": [100 + index * 0.4 for index in range(len(dates))],
            "volume": [1000 + index * 10 for index in range(len(dates))],
            "vix_level": [None] * len(dates),
            "feature_ready": [False] * len(dates),
            "price_only_ready": [index >= 34 for index in range(len(dates))],
        }
    )


def load_frame(dataset_name: str, dataset_version: str) -> pd.DataFrame:
    service = GoldService(Settings())
    layer = "silver" if dataset_name == "market_ohlcv_v1" else "gold"
    with __import__("psycopg").connect(
        **service.settings.postgres_kwargs()
    ) as connection:
        _, rows = service._parent(connection, layer, dataset_name, dataset_version)
    return pd.DataFrame(rows)


def run_report(
    kind: str,
    dataset_name: str,
    dataset_version: str,
    output_dir: Path,
    *,
    synthetic: bool = False,
) -> dict:
    import matplotlib.pyplot as plt

    frame = (
        synthetic_frame() if synthetic else load_frame(dataset_name, dataset_version)
    )
    frame["session_date"] = pd.to_datetime(frame.session_date)
    frame = frame.sort_values(["ticker", "session_date"])
    returns = frame.groupby("ticker").adj_close.pct_change(fill_method=None)
    result = {
        "report": kind,
        "row_count": len(frame),
        "tickers": sorted(frame.ticker.unique().tolist()),
        "first_session": frame.session_date.min().date().isoformat(),
        "last_session": frame.session_date.max().date().isoformat(),
    }
    if kind == "quality":
        result["missing_values"] = frame.isna().sum().to_dict()
        result["duplicate_sessions"] = int(
            frame.duplicated(["ticker", "session_date"]).sum()
        )
    elif kind == "returns":
        result["return_mean"] = float(returns.mean())
        result["return_std"] = float(returns.std())
        peaks = frame.groupby("ticker").adj_close.cummax()
        result["max_drawdown"] = float((frame.adj_close / peaks - 1).min())
    elif kind == "correlation":
        result["close_volume_correlation_by_ticker"] = {
            ticker: float(group.adj_close.corr(group.volume))
            for ticker, group in frame.groupby("ticker")
        }
        result["high_volatility_days"] = int((returns.abs() > 0.05).sum())
    elif kind == "indicators":
        asset = frame.loc[frame.ticker == frame.ticker.iloc[0]]
        result["sma_20_last"] = float(asset.adj_close.rolling(20).mean().iloc[-1])
        result["volume_mean"] = float(frame.volume.mean())
    elif kind == "macro":
        result["macro_available_rows"] = (
            int(frame.vix_level.notna().sum()) if "vix_level" in frame else 0
        )
        result["macro_limitation"] = (
            "Availability follows recorded source timestamps; historical "
            "backfill may have no eligible macro values."
        )
    else:
        raise ValueError("unknown report")
    config_bytes = (PROJECT_ROOT / "config/feature_config.yaml").read_bytes()
    metadata = {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "git_sha": code_revision(),
        "feature_config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "generated_at": datetime.now(UTC).isoformat(),
        "synthetic": synthetic,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(9, 4))
    if kind == "quality":
        frame.isna().sum().sort_values(ascending=False).head(15).plot.bar(ax=axis)
        axis.set_ylabel("Missing rows")
    elif kind == "returns":
        returns.dropna().hist(bins=30, ax=axis)
        axis.set_xlabel("Daily adjusted-close return")
    elif kind == "correlation":
        matrix = (
            frame.assign(returns=returns)
            .pivot_table(index="session_date", columns="ticker", values="returns")
            .corr()
        )
        result["return_correlations"] = matrix.to_dict()
        axis.imshow(matrix.to_numpy(), vmin=-1, vmax=1, cmap="coolwarm")
        axis.set_xticks(range(len(matrix)), matrix.columns, rotation=45)
        axis.set_yticks(range(len(matrix)), matrix.index)
    elif kind == "indicators":
        asset = frame.loc[frame.ticker == frame.ticker.iloc[0]]
        axis.plot(asset.session_date, asset.adj_close, label="Adjusted close")
        axis.plot(
            asset.session_date, asset.adj_close.rolling(20).mean(), label="SMA 20"
        )
        axis.legend()
    else:
        available = (
            frame.vix_level.notna()
            if "vix_level" in frame
            else pd.Series(False, index=frame.index)
        )
        axis.plot(frame.session_date, available.astype(int))
        axis.set_ylabel("VIX eligible at cutoff")
    axis.set_title(kind.replace("_", " ").title())
    figure.tight_layout()
    figure.savefig(output_dir / f"{kind}.png", dpi=120)
    plt.close(figure)
    path = output_dir / f"{kind}.json"
    path.write_text(
        json.dumps({"metadata": metadata, "summary": result}, indent=2, sort_keys=True)
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--dataset-name", default="market_features_v1")
    parser.add_argument("--dataset-version", default="synthetic-v1")
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "artifacts/eda"
    )
    args = parser.parse_args()
    if args.check:
        for path in sorted((PROJECT_ROOT / "notebooks").glob("0*.ipynb")):
            notebook = nbformat.read(path, as_version=4)
            NotebookClient(
                notebook,
                timeout=60,
                kernel_name="python3",
                resources={"metadata": {"path": str(PROJECT_ROOT)}},
            ).execute()
        print("notebook checks passed: 5 synthetic notebooks")
        return
    for kind in ("quality", "returns", "correlation", "indicators", "macro"):
        print(
            json.dumps(
                run_report(
                    kind, args.dataset_name, args.dataset_version, args.output_dir
                )
            )
        )


if __name__ == "__main__":
    main()
