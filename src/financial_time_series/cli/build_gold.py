"""Explicit published Silver to Gold build command."""

import argparse
import json
from datetime import datetime

from financial_time_series.features.gold_service import GoldService
from financial_time_series.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("features", "training"), required=True)
    parser.add_argument("--market-version", required=True)
    parser.add_argument("--macro-version", required=True)
    parser.add_argument("--features-version")
    parser.add_argument("--build-cutoff-at", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    result = GoldService(Settings()).build(
        args.dataset,
        args.market_version,
        args.macro_version,
        args.build_cutoff_at,
        args.features_version,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
