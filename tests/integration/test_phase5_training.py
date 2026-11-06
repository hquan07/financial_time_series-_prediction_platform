"""Verify the pinned real Gold input when explicitly requested."""

import os

import pytest

from financial_time_series.settings import PROJECT_ROOT, Settings
from financial_time_series.training.dataset import load_training
from financial_time_series.training.runner import load_config
from financial_time_series.training.splits import make_splits

pytestmark = pytest.mark.integration


def test_pinned_gold_training_is_splittable():
    version = os.getenv("GOLD_TRAINING_VERSION")
    if not version:
        pytest.skip("set GOLD_TRAINING_VERSION")
    config = load_config(PROJECT_ROOT / "config/model_config.yaml")
    rows, lineage = load_training(version, config, Settings())
    folds, holdout = make_splits(rows, config)
    assert lineage["dataset_version"] == version
    assert len(rows) == 4760
    assert [len(f.train_dates) for f in folds] == [355, 418, 481]
    assert len(holdout) == 136
