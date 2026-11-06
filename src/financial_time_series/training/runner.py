"""Fixed-candidate walk-forward research on a pinned Gold snapshot."""

import json
import math
from collections import defaultdict
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version as package_version
from pathlib import Path
from time import perf_counter

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xgboost as xgb
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from financial_time_series.features.compute import PRICE_FEATURES
from financial_time_series.ingestion.service import code_revision
from financial_time_series.settings import PROJECT_ROOT, Settings
from financial_time_series.training.dataset import load_training
from financial_time_series.training.splits import make_splits

REGRESSION = (
    "zero_return",
    "historical_mean",
    "linear_regression",
    "xgboost_regressor",
    "lightgbm_regressor",
)
CLASSIFICATION = ("majority_direction", "logistic_regression")
FIXED_PARAMETERS = {
    "linear_regression": {"fit_intercept": True},
    "logistic_regression": {"solver": "lbfgs", "C": 1.0, "max_iter": 1000},
    "xgboost_regressor": {
        "objective": "reg:squarederror",
        "tree_method": "hist",
        "n_estimators": 200,
        "max_depth": 3,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "reg_lambda": 10,
    },
    "lightgbm_regressor": {
        "objective": "regression",
        "n_estimators": 200,
        "max_depth": 3,
        "num_leaves": 7,
        "learning_rate": 0.05,
        "min_child_samples": 40,
        "subsample": 0.8,
        "subsample_freq": 1,
        "colsample_bytree": 0.8,
        "reg_lambda": 10,
        "deterministic": True,
        "force_col_wise": True,
    },
}


def load_config(path: Path) -> dict:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (
        config["model_contract_version"] != "price_only_return_models_v1"
        or config["dataset_name"] != "training_next_session_return_v1"
        or config["dataset_tier"] != "price_only_core_v1"
        or tuple(config["feature_columns"]) != PRICE_FEATURES
        or config["target_contract_version"] != "adj_open_to_close_v1"
        or config["selection_metric"] != "pooled_oof_rmse"
        or config["holdout_fraction"] != 0.2
        or config["fold_count"] != 3
        or config["validation_sessions"] != 63
        or config["min_train_sessions"] != 252
    ):
        raise ValueError("unsupported model contract")
    if config["threads"] != 2 or config["seed"] != 42:
        raise ValueError("unsupported seed or thread count for model contract")
    if any(config.get(name) != params for name, params in FIXED_PARAMETERS.items()):
        raise ValueError("fixed candidate parameters differ from model contract")
    return config


def _inputs(rows: list[dict], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame.from_records(rows)[[*columns, "ticker", "exchange"]]


def _pipeline(name: str, config: dict) -> Pipeline:
    numeric = config["feature_columns"]
    transform = ColumnTransformer(
        [
            ("numeric", StandardScaler(), numeric),
            (
                "asset",
                OneHotEncoder(handle_unknown="error", sparse_output=False),
                ["ticker", "exchange"],
            ),
        ]
    )
    if name == "linear_regression":
        model = LinearRegression(**config[name])
    elif name == "xgboost_regressor":
        model = xgb.XGBRegressor(
            **config[name], random_state=config["seed"], n_jobs=config["threads"]
        )
    elif name == "lightgbm_regressor":
        model = lgb.LGBMRegressor(
            **config[name],
            random_state=config["seed"],
            n_jobs=config["threads"],
            verbosity=-1,
        )
    elif name == "logistic_regression":
        model = LogisticRegression(**config[name], class_weight=None)
    else:
        raise ValueError("unknown candidate")
    return Pipeline([("preprocess", transform), ("model", model)])


def _predict(name: str, train: list[dict], test: list[dict], config: dict):
    if name == "zero_return":
        return np.zeros(len(test)), None, None
    if name == "historical_mean":
        by_ticker = defaultdict(list)
        for row in train:
            by_ticker[row["ticker"]].append(row["target_return"])
        if any(row["ticker"] not in by_ticker for row in test):
            raise ValueError("unseen_ticker_for_historical_mean")
        values = np.array([np.mean(by_ticker[row["ticker"]]) for row in test])
        return values, None, None
    if name == "majority_direction":
        majority = int(np.mean([r["target_return"] > 0 for r in train]) > 0.5)
        values = np.full(len(test), majority)
        return values, None, values
    model = _pipeline(name, config)
    target = np.array([r["target_return"] for r in train])
    if name == "logistic_regression":
        target = (target > 0).astype(int)
    model.fit(_inputs(train, config["feature_columns"]), target)
    prediction = model.predict(_inputs(test, config["feature_columns"]))
    scores = (
        model.predict_proba(_inputs(test, config["feature_columns"]))[:, 1]
        if name == "logistic_regression"
        else None
    )
    return prediction, model, scores


def _metrics(name: str, items: list[dict]) -> dict:
    actual = np.array([item["actual"] for item in items])
    predicted = np.array([item["predicted"] for item in items])
    if name in CLASSIFICATION:
        return {
            "support": len(items),
            "accuracy": float(accuracy_score(actual, predicted)),
            "balanced_accuracy": float(balanced_accuracy_score(actual, predicted)),
            "precision": float(precision_score(actual, predicted, zero_division=0)),
            "recall": float(recall_score(actual, predicted, zero_division=0)),
            "f1": float(f1_score(actual, predicted, zero_division=0)),
            "confusion_matrix": confusion_matrix(
                actual, predicted, labels=[0, 1]
            ).tolist(),
            "pr_auc": float(
                average_precision_score(actual, [item["score"] for item in items])
            )
            if len(set(actual)) == 2
            else None,
        }
    if not np.isfinite(predicted).all():
        raise ValueError("nonfinite_prediction")
    return {
        "support": len(items),
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(math.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)) if len(items) >= 2 else None,
        "directional_accuracy": float(accuracy_score(actual > 0, predicted > 0)),
        "target_mean": float(actual.mean()),
        "target_std": float(actual.std()),
        "prediction_mean": float(predicted.mean()),
        "prediction_std": float(predicted.std()),
        "finite_prediction_rate": 1.0,
    }


def _records(
    name: str, phase: str, fold: int | None, rows: list[dict], values, scores=None
) -> list[dict]:
    return [
        {
            "task": "classification" if name in CLASSIFICATION else "regression",
            "candidate": name,
            "phase": phase,
            "fold": fold,
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "as_of_session": row["as_of_session"],
            "target_session": row["target_session"],
            "actual": int(row["target_return"] > 0)
            if name in CLASSIFICATION
            else float(row["target_return"]),
            "predicted": int(value) if name in CLASSIFICATION else float(value),
            "score": float(scores[index]) if scores is not None else None,
        }
        for index, (row, value) in enumerate(zip(rows, values, strict=True))
    ]


def run(rows: list[dict], config: dict) -> tuple[list[dict], dict, list[dict], dict]:
    folds, holdout = make_splits(rows, config)
    predictions = []
    runtimes = defaultdict(float)
    for fold in folds:
        train_dates = set(fold.train_dates)
        validation_dates = set(fold.validation_dates)
        train = [r for r in rows if r["as_of_session"] in train_dates]
        validation = [r for r in rows if r["as_of_session"] in validation_dates]
        for name in (*REGRESSION, *CLASSIFICATION):
            started = perf_counter()
            values, _, scores = _predict(name, train, validation, config)
            runtimes[name] += perf_counter() - started
            predictions.extend(
                _records(name, "validation", fold.number, validation, values, scores)
            )
    oof = {
        name: _metrics(name, [p for p in predictions if p["candidate"] == name])
        for name in (*REGRESSION, *CLASSIFICATION)
    }
    winner = min(
        REGRESSION,
        key=lambda name: (oof[name]["rmse"], oof[name]["mae"], REGRESSION.index(name)),
    )
    development = [r for r in rows if r["as_of_session"] < holdout[0]]
    final_rows = [r for r in rows if r["as_of_session"] in set(holdout)]
    final_model = None
    for name in dict.fromkeys(
        (winner, "zero_return", "historical_mean", *CLASSIFICATION)
    ):
        started = perf_counter()
        values, model, scores = _predict(name, development, final_rows, config)
        runtimes[name] += perf_counter() - started
        if name == winner:
            final_model = model
        predictions.extend(_records(name, "holdout", None, final_rows, values, scores))
    metrics = {
        "selected_regression_candidate": winner,
        "selection_metric": "pooled_oof_rmse",
        "development_oof": oof,
        "holdout": {
            name: _metrics(
                name,
                [
                    p
                    for p in predictions
                    if p["candidate"] == name and p["phase"] == "holdout"
                ],
            )
            for name in dict.fromkeys(
                (winner, "zero_return", "historical_mean", *CLASSIFICATION)
            )
        },
        "folds": {
            name: {
                str(fold.number): _metrics(
                    name,
                    [
                        p
                        for p in predictions
                        if p["candidate"] == name and p["fold"] == fold.number
                    ],
                )
                for fold in folds
            }
            for name in (*REGRESSION, *CLASSIFICATION)
        },
        "per_ticker": {
            name: {
                ticker: _metrics(name, subset)
                for ticker in sorted({p["ticker"] for p in predictions})
                if len(
                    subset := [
                        p
                        for p in predictions
                        if p["candidate"] == name
                        and p["phase"] == "validation"
                        and p["ticker"] == ticker
                    ]
                )
                >= 30
            }
            for name in (*REGRESSION, *CLASSIFICATION)
        },
        "holdout_per_ticker": {
            name: {
                ticker: _metrics(name, subset)
                for ticker in sorted({p["ticker"] for p in predictions})
                if len(
                    subset := [
                        p
                        for p in predictions
                        if p["candidate"] == name
                        and p["phase"] == "holdout"
                        and p["ticker"] == ticker
                    ]
                )
                >= 30
            }
            for name in dict.fromkeys(
                (winner, "zero_return", "historical_mean", *CLASSIFICATION)
            )
        },
        "runtime_seconds": dict(runtimes),
    }
    boundaries = [
        {
            "fold": fold.number,
            "train_start": fold.train_dates[0].isoformat(),
            "train_end": fold.train_dates[-1].isoformat(),
            "validation_start": fold.validation_dates[0].isoformat(),
            "validation_end": fold.validation_dates[-1].isoformat(),
            "train_sessions": len(fold.train_dates),
            "validation_sessions": len(fold.validation_dates),
            "train_rows": len(fold.train_dates) * len({r["ticker"] for r in rows}),
            "validation_rows": len(fold.validation_dates)
            * len({r["ticker"] for r in rows}),
        }
        for fold in folds
    ]
    split_info = {
        "folds": boundaries,
        "holdout_start": holdout[0].isoformat(),
        "holdout_end": holdout[-1].isoformat(),
        "holdout_sessions": len(holdout),
    }
    return predictions, metrics, boundaries, {"split": split_info, "model": final_model}


def publish(version: str, config_path: Path, settings: Settings) -> Path:
    config = load_config(config_path)
    rows, lineage = load_training(version, config, settings)
    predictions, metrics, folds, result = run(rows, config)
    config_bytes = config_path.read_bytes()
    experiment_id = (
        datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + sha256(config_bytes + version.encode()).hexdigest()[:12]
    )
    target = PROJECT_ROOT / "artifacts" / "experiments" / experiment_id
    target.mkdir(parents=True, exist_ok=False)
    manifest = {
        "experiment_id": experiment_id,
        "created_at": datetime.now(UTC).isoformat(),
        "data": lineage,
        "model_config": config,
        "model_config_sha256": sha256(config_bytes).hexdigest(),
        "git_revision": code_revision(),
        "packages": {
            name: package_version(name)
            for name in ("scikit-learn", "xgboost", "lightgbm", "joblib", "pyarrow")
        },
        "row_count": len(rows),
        "research_only": True,
    }
    (target / "run_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (target / "folds.json").write_text(json.dumps(result["split"], indent=2) + "\n")
    (target / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    pq.write_table(pa.Table.from_pylist(predictions), target / "predictions.parquet")
    (target / "models").mkdir()
    if result["model"] is not None:
        joblib.dump(result["model"], target / "models" / "selected_regression.joblib")
    winner = metrics["selected_regression_candidate"]
    regression_rows = []
    for name in REGRESSION:
        development = metrics["development_oof"][name]
        final = metrics["holdout"].get(name)
        holdout_rmse = f"{final['rmse']:.6f}" if final else "—"
        regression_rows.append(
            f"| {name} | {development['support']} | {development['rmse']:.6f} "
            f"| {development['mae']:.6f} | {holdout_rmse} |"
        )
    classification_rows = []
    for name in CLASSIFICATION:
        development = metrics["development_oof"][name]
        final = metrics["holdout"][name]
        classification_rows.append(
            f"| {name} | {development['support']} | {development['accuracy']:.4f} "
            f"| {development['f1']:.4f} | {final['accuracy']:.4f} |"
        )
    report = (
        f"# Phase 5 comparison — {experiment_id}\n\n"
        f"Pinned Gold: `{version}`. Contract: `adj_open_to_close_v1`; "
        "price-only research snapshot. Historical market availability is not "
        "point-in-time verified; these metrics do not establish tradable "
        "performance. This pinned holdout has been inspected during Phase 5 "
        "implementation validation, so it is descriptive rather than fresh "
        "confirmatory evidence.\n\n"
        f"Selected by development OOF RMSE: `{winner}`. "
        f"Development RMSE: {metrics['development_oof'][winner]['rmse']:.6f}; "
        f"holdout RMSE: {metrics['holdout'][winner]['rmse']:.6f}.\n\n"
        "## Regression\n\n"
        "| Candidate | OOF rows | OOF RMSE | OOF MAE | Holdout RMSE |\n"
        "|---|---:|---:|---:|---:|\n" + "\n".join(regression_rows) + "\n\n"
        "Holdout was evaluated for the selected candidate and reference "
        "baselines only; — means no holdout evaluation.\n\n"
        "## Secondary direction study\n\n"
        "| Candidate | OOF rows | OOF accuracy | OOF F1 | Holdout accuracy |\n"
        "|---|---:|---:|---:|---:|\n" + "\n".join(classification_rows) + "\n\n"
        "See `metrics.json`, `folds.json`, `predictions.parquet` and "
        "`run_manifest.json` for all candidates, support counts and lineage.\n"
    )
    (target / "comparison.md").write_text(report)
    return target
