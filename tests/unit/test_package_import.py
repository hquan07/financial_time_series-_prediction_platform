"""Installed-package smoke test."""

from importlib.metadata import version

import financial_time_series


def test_package_version_matches_distribution() -> None:
    assert financial_time_series.__version__ == version(
        "financial-time-series-platform"
    )
