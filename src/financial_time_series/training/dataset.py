"""Load a published Gold training version and verify its direct lineage."""

import json
import math

import psycopg

from financial_time_series.features.compute import PRICE_FEATURES
from financial_time_series.features.gold_service import GoldService
from financial_time_series.processing.calendar import calendar_for
from financial_time_series.processing.quality import QualityError
from financial_time_series.settings import Settings


def load_training(
    version: str, config: dict, settings: Settings
) -> tuple[list[dict], dict]:
    service = GoldService(settings)
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        record, rows = service._parent(
            connection, "gold", "training_next_session_return_v1", version
        )
        manifest = json.loads(
            service._read_object(record["manifest_uri"], record["manifest_sha256"])
        )
        if manifest["readiness_tier"] != config["dataset_tier"]:
            raise QualityError("training_tier_mismatch")
        if (
            manifest["version_input"]["target_contract_version"]
            != config["target_contract_version"]
        ):
            raise QualityError("training_target_contract_mismatch")
        expected = manifest["version_input"]["parents"]
        actual = connection.execute(
            """SELECT p.parent_dataset_id, p.relationship, d.manifest_sha256
               FROM dataset_parents p JOIN dataset_versions d
                 ON d.dataset_id=p.parent_dataset_id
               WHERE p.child_dataset_id=%s ORDER BY p.ordinal""",
            (record["dataset_id"],),
        ).fetchall()
        if [(str(item[0]), item[1], item[2]) for item in actual] != [
            (p["id"], p["relationship"], p["sha256"]) for p in expected
        ]:
            raise QualityError("training_lineage_mismatch")
        if [item[1] for item in actual] != ["feature_input", "label_input"]:
            raise QualityError("training_parent_roles_mismatch")
        feature_parent = connection.execute(
            """SELECT layer,dataset_name,dataset_version FROM dataset_versions
               WHERE dataset_id=%s""",
            (actual[0][0],),
        ).fetchone()
        label_parent = connection.execute(
            """SELECT layer,dataset_name,dataset_version FROM dataset_versions
               WHERE dataset_id=%s""",
            (actual[1][0],),
        ).fetchone()
        if feature_parent[:2] != ("gold", "market_features_v1"):
            raise QualityError("training_feature_parent_mismatch")
        if label_parent[:2] != ("silver", "market_ohlcv_v1"):
            raise QualityError("training_label_parent_mismatch")
        service._parent(connection, *feature_parent)
        service._parent(connection, *label_parent)
    if tuple(config["feature_columns"]) != PRICE_FEATURES:
        raise ValueError("model feature list differs from features_v1")
    if not rows:
        raise QualityError("empty_training_dataset")
    for row in rows:
        if not row["price_only_ready"] or not row["label_ready"]:
            raise QualityError("training_unready_row")
        if row["target_contract_version"] != config["target_contract_version"]:
            raise QualityError("training_row_contract_mismatch")
        if row["feature_dataset_version"] != feature_parent[2]:
            raise QualityError("training_row_feature_version_mismatch")
        if (
            row["target_session"]
            != calendar_for(row["exchange"])
            .next_session(row["as_of_session"].isoformat())
            .date()
        ):
            raise QualityError("training_wrong_target_session")
        if not math.isfinite(float(row["target_return"])):
            raise QualityError("training_nonfinite_target")
        if any(
            row.get(name) is None or not math.isfinite(float(row[name]))
            for name in PRICE_FEATURES
        ):
            raise QualityError("training_nonfinite_feature")
    lineage = {
        "dataset_name": "training_next_session_return_v1",
        "dataset_version": version,
        "dataset_id": str(record["dataset_id"]),
        "manifest_sha256": record["manifest_sha256"],
        "parents": expected,
        "feature_contract_version": manifest["version_input"][
            "feature_contract_version"
        ],
        "target_contract_version": config["target_contract_version"],
        "readiness_tier": manifest["readiness_tier"],
    }
    return rows, lineage
