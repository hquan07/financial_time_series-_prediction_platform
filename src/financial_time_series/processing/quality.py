"""Deterministic quality report primitives."""

import hashlib
import json
from dataclasses import dataclass, field


class QualityError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def json_bytes(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass
class QualityReport:
    dataset_name: str
    source_run_ids: list[str]
    warnings: list[dict] = field(default_factory=list)
    duplicates: list[dict] = field(default_factory=list)
    statistics: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "rule_version": "silver_quality_v1",
            "dataset_name": self.dataset_name,
            "source_run_ids": self.source_run_ids,
            "warnings": sorted(self.warnings, key=lambda item: json_bytes(item)),
            "duplicates": sorted(self.duplicates, key=lambda item: json_bytes(item)),
            "statistics": self.statistics,
        }
