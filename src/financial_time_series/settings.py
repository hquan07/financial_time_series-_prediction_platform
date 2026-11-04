"""Runtime settings. Secret values never appear in model repr or logs."""

from pathlib import Path

import yaml
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", extra="ignore")

    fred_api_key: SecretStr = SecretStr("")
    minio_endpoint: str = "localhost:19000"
    minio_access_key: SecretStr = SecretStr("")
    minio_secret_key: SecretStr = SecretStr("")
    minio_bucket: str = "financial-time-series"
    minio_secure: bool = False
    postgres_host: str = "localhost"
    postgres_port: int = 15432
    postgres_db: str = "financial_time_series"
    postgres_user: str = ""
    postgres_password: SecretStr = SecretStr("")

    def postgres_kwargs(self) -> dict:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password.get_secret_value(),
            "connect_timeout": 5,
        }

    def require_storage(self) -> None:
        if not all(
            (
                self.minio_access_key.get_secret_value(),
                self.minio_secret_key.get_secret_value(),
                self.postgres_user,
                self.postgres_password.get_secret_value(),
            )
        ):
            raise ValueError("MinIO and PostgreSQL credentials must be configured")


def load_source_config() -> dict:
    with (PROJECT_ROOT / "config/data_sources.yaml").open(encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_assets() -> list[dict]:
    with (PROJECT_ROOT / "config/assets.yaml").open(encoding="utf-8") as file:
        return yaml.safe_load(file)["market"]["assets"]
