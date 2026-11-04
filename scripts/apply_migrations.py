"""Apply ordered checksum-verified SQL migrations and seed source identities."""

import hashlib
from pathlib import Path

import psycopg

from financial_time_series.settings import Settings, load_assets

MIGRATIONS = Path(__file__).resolve().parents[1] / "infrastructure/postgres/migrations"


def apply_migrations(settings: Settings) -> list[str]:
    settings.require_storage()
    applied = []
    with psycopg.connect(**settings.postgres_kwargs()) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"""
        )
        for path in sorted(MIGRATIONS.glob("[0-9]*.sql")):
            version = path.stem
            script = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(script.encode()).hexdigest()
            row = connection.execute(
                "SELECT checksum FROM schema_migrations WHERE version=%s", (version,)
            ).fetchone()
            if row:
                if row[0] != checksum:
                    raise RuntimeError(f"migration checksum changed: {version}")
                continue
            connection.execute(script)
            connection.execute(
                "INSERT INTO schema_migrations(version, checksum) VALUES (%s,%s)",
                (version, checksum),
            )
            applied.append(version)
        for name, kind, url in (
            ("yahoo_finance", "market", "https://query1.finance.yahoo.com"),
            ("fred", "macro", "https://api.stlouisfed.org"),
        ):
            connection.execute(
                """INSERT INTO data_sources(source_name,kind,base_url)
                   VALUES (%s,%s,%s) ON CONFLICT (source_name) DO NOTHING""",
                (name, kind, url),
            )
        for asset in load_assets():
            connection.execute(
                """INSERT INTO assets(ticker,exchange,timezone)
                   VALUES (%s,%s,%s) ON CONFLICT (ticker,exchange) DO NOTHING""",
                (asset["ticker"], asset["exchange"], asset["timezone"]),
            )
    return applied


if __name__ == "__main__":
    print({"applied_migrations": apply_migrations(Settings())})
