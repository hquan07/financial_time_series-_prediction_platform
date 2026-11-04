"""Manual Bronze-to-Silver build entry point."""

import argparse
import json
import sys
from datetime import UTC, datetime
from uuid import UUID

from financial_time_series.processing.silver_service import SilverService
from financial_time_series.settings import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a versioned Silver dataset")
    parser.add_argument("--dataset", choices=("market", "macro"), required=True)
    parser.add_argument("--build-cutoff-at")
    parser.add_argument("--source-run-id", action="append", type=UUID)
    args = parser.parse_args()
    try:
        cutoff = (
            datetime.fromisoformat(args.build_cutoff_at.replace("Z", "+00:00"))
            if args.build_cutoff_at
            else datetime.now(UTC)
        )
        result = SilverService(Settings()).build(
            args.dataset, cutoff, args.source_run_id
        )
    except Exception as error:
        message = str(error)
        if isinstance(error, RuntimeError) and message.startswith(
            "silver build failed: "
        ):
            print(message, file=sys.stderr)
        else:
            print(f"silver build failed: {type(error).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({**result, "build_cutoff_at": cutoff.astimezone(UTC).isoformat()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
