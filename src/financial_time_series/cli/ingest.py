"""One-shot incremental ingestion for configured entities."""

import argparse
import json

from financial_time_series.ingestion.service import IngestionService, resolve_entity
from financial_time_series.settings import Settings, load_assets, load_source_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("yahoo_finance", "fred"))
    parser.add_argument("--entity", help="configured ticker or FRED series ID")
    parser.add_argument(
        "--all", action="store_true", help="ingest all configured entities"
    )
    args = parser.parse_args()
    if args.all == bool(args.entity) or (not args.all and not args.source):
        parser.error("provide --source and --entity, or --all")
    service = IngestionService(Settings())
    if args.all:
        entities = [resolve_entity("yahoo_finance", a["ticker"]) for a in load_assets()]
        entities += [
            resolve_entity("fred", s["id"])
            for s in load_source_config()["macro"]["series"]
        ]
    else:
        entities = [resolve_entity(args.source, args.entity)]
    for entity in entities:
        for summary in service.ingest(entity):
            print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
