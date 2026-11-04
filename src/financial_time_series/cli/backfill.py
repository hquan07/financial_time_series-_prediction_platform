"""Date-bounded backfill for one configured entity."""

import argparse
import json
from datetime import date

from financial_time_series.ingestion.models import DateRange
from financial_time_series.ingestion.service import IngestionService, resolve_entity
from financial_time_series.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("yahoo_finance", "fred"), required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    service = IngestionService(Settings())
    entity = resolve_entity(args.source, args.entity)
    for summary in service.ingest(
        entity, backfill=True, bounds=DateRange(args.start, args.end)
    ):
        print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
