"""Read back published Gold parts, lineage and point-in-time gates."""

import os

import psycopg
import pytest

from financial_time_series.features.gold_service import GoldService
from financial_time_series.settings import Settings

pytestmark = pytest.mark.integration


def test_published_gold_versions_and_lineage():
    feature_version = os.getenv("GOLD_FEATURES_VERSION")
    training_version = os.getenv("GOLD_TRAINING_VERSION")
    if not feature_version or not training_version:
        pytest.skip("set GOLD_FEATURES_VERSION and GOLD_TRAINING_VERSION")
    service = GoldService(Settings())
    with psycopg.connect(**service.settings.postgres_kwargs()) as connection:
        features, feature_rows = service._parent(
            connection, "gold", "market_features_v1", feature_version
        )
        training, training_rows = service._parent(
            connection, "gold", "training_next_session_return_v1", training_version
        )
        parents = connection.execute(
            """SELECT parent_dataset_id,relationship FROM dataset_parents
               WHERE child_dataset_id=%s ORDER BY ordinal""",
            (training["dataset_id"],),
        ).fetchall()
    assert parents[0] == (features["dataset_id"], "feature_input")
    assert parents[1][1] == "label_input"
    assert len(feature_rows) >= len(training_rows) > 0
    assert all(
        row["selected_vix_available_at"] is None
        or row["selected_vix_available_at"] <= row["forecast_cutoff_at"]
        for row in feature_rows
    )
    assert all(
        row["selected_dgs10_available_at"] is None
        or row["selected_dgs10_available_at"] <= row["forecast_cutoff_at"]
        for row in feature_rows
    )
    assert all(row["price_only_ready"] and row["label_ready"] for row in training_rows)
