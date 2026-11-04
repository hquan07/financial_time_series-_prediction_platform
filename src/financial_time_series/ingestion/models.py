"""Source-neutral ingestion contracts."""

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start must be on or before end")


@dataclass(frozen=True)
class Entity:
    source: str
    key: str
    interval: str
    manifest_entity: dict[str, str]
    timezone: str | None = None
    availability_policy_version: str | None = None


@dataclass(frozen=True)
class FetchResult:
    payload: bytes
    row_count: int
    max_observation_date: date | None
    payload_format: str
    source_format: str
    schema_version: str
    source_version: str
    column_shape: list[str] = field(default_factory=list)
