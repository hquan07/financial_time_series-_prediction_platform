"""Common-session chronological holdout and expanding folds."""

from collections import Counter
from dataclasses import dataclass
from datetime import date
from math import ceil


@dataclass(frozen=True)
class Fold:
    number: int
    train_dates: tuple[date, ...]
    validation_dates: tuple[date, ...]


def make_splits(rows: list[dict], config: dict) -> tuple[list[Fold], tuple[date, ...]]:
    counts = Counter(row["as_of_session"] for row in rows)
    dates = sorted(counts)
    if len(
        {(row["ticker"], row["exchange"], row["as_of_session"]) for row in rows}
    ) != len(rows):
        raise ValueError("duplicate asset/session")
    if len(set(counts.values())) != 1:
        raise ValueError("assets do not share a complete session grid")
    assets = {(row["ticker"], row["exchange"]) for row in rows}
    if any(
        {(r["ticker"], r["exchange"]) for r in rows if r["as_of_session"] == day}
        != assets
        for day in dates
    ):
        raise ValueError("assets do not share common sessions")
    holdout_count = ceil(len(dates) * config["holdout_fraction"])
    development = dates[:-holdout_count]
    holdout = tuple(dates[-holdout_count:])
    blocks = config["fold_count"] * config["validation_sessions"]
    initial = len(development) - blocks
    if initial < config["min_train_sessions"]:
        raise ValueError("insufficient_history")
    folds = []
    for index in range(config["fold_count"]):
        boundary = initial + index * config["validation_sessions"]
        train = tuple(development[:boundary])
        validation = tuple(
            development[boundary : boundary + config["validation_sessions"]]
        )
        train_dates = set(train)
        train_rows = [r for r in rows if r["as_of_session"] in train_dates]
        if max(r["target_session"] for r in train_rows) > validation[0]:
            raise ValueError("train_target_crosses_validation")
        folds.append(Fold(index + 1, train, validation))
    return folds, holdout
