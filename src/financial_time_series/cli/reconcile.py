"""Explicitly abandon a stale pending run after checking its entity lock."""

import argparse
import json
from datetime import timedelta
from uuid import UUID

from financial_time_series.ingestion.service import resolve_entity
from financial_time_series.settings import Settings
from financial_time_series.storage.postgres_metadata import MetadataStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--source", choices=("yahoo_finance", "fred"), required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--older-than-minutes", type=int, default=60)
    args = parser.parse_args()
    if args.older_than_minutes < 1:
        parser.error("older-than-minutes must be positive")
    settings = Settings()
    settings.require_storage()
    entity = resolve_entity(args.source, args.entity)
    changed = MetadataStore(settings).abandon_stale(
        args.run_id, entity, timedelta(minutes=args.older_than_minutes)
    )
    print(json.dumps({"ingestion_run_id": str(args.run_id), "abandoned": changed}))


if __name__ == "__main__":
    main()
