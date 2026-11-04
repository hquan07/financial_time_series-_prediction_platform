"""Stable market selection and source revision reporting."""

from collections import defaultdict

from financial_time_series.processing.quality import QualityError, QualityReport


def canonicalize_market(rows: list[dict], report: QualityReport) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[(row["ticker"], row["exchange"], row["session_date"])].append(row)
    selected = []
    for key, candidates in sorted(groups.items()):
        winner = max(
            candidates,
            key=lambda row: (row["_completed_at"], str(row["source_run_id"])),
        )
        if len(candidates) > 1:
            signatures = {
                tuple(
                    row[name]
                    for name in ("open", "high", "low", "close", "adj_close", "volume")
                )
                for row in candidates
            }
            record = {
                "ticker": key[0],
                "exchange": key[1],
                "session_date": key[2].isoformat(),
                "source_run_ids": sorted(
                    str(row["source_run_id"]) for row in candidates
                ),
                "selected_run_id": str(winner["source_run_id"]),
                "values_differ": len(signatures) > 1,
            }
            report.duplicates.append(record)
            if record["values_differ"]:
                report.warnings.append({"code": "market_revision", **record})
        selected.append(
            {name: value for name, value in winner.items() if name != "_completed_at"}
        )
    if len(selected) != len(groups):
        raise QualityError("duplicate_canonical_market_key")
    return selected


def report_macro_revisions(rows: list[dict], report: QualityReport) -> None:
    groups = defaultdict(list)
    for row in rows:
        groups[(row["series_id"], row["observation_date"])].append(row)
    for key, candidates in sorted(groups.items()):
        if len(candidates) < 2:
            continue
        record = {
            "series_id": key[0],
            "observation_date": key[1].isoformat(),
            "source_run_ids": sorted(str(row["source_run_id"]) for row in candidates),
            "values_differ": len({row["value"] for row in candidates}) > 1,
        }
        report.duplicates.append(record)
        if record["values_differ"]:
            report.warnings.append({"code": "macro_revision", **record})
