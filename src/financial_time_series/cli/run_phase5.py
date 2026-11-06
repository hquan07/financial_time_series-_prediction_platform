"""Run a fixed Phase 5 comparison on an explicit Gold training version."""

import argparse
from pathlib import Path

from financial_time_series.settings import Settings
from financial_time_series.training.runner import publish


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-training-version", required=True)
    parser.add_argument("--model-config", type=Path, required=True)
    args = parser.parse_args()
    print(publish(args.gold_training_version, args.model_config, Settings()))


if __name__ == "__main__":
    main()
