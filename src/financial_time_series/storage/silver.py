"""Immutable Silver object publication and readback verification."""

from financial_time_series.processing.quality import QualityError, sha256
from financial_time_series.storage.minio_client import MinioStore


class SilverStore:
    def __init__(self, store: MinioStore):
        self.store = store

    def put_verified(self, key: str, payload: bytes, content_type: str) -> dict:
        self.store.put_new(key, payload, content_type)
        self.verify(key, sha256(payload), len(payload))
        return {
            "uri": self.store.uri(key),
            "sha256": sha256(payload),
            "byte_count": len(payload),
        }

    def verify(self, key: str, expected_sha: str, expected_size: int) -> None:
        try:
            payload = self.store.read(key)
        except Exception as error:
            raise QualityError("silver_object_missing") from error
        if len(payload) != expected_size or sha256(payload) != expected_sha:
            raise QualityError("silver_object_checksum_mismatch")

    def key_from_uri(self, uri: str) -> str:
        prefix = f"s3://{self.store.bucket}/"
        if not uri.startswith(prefix):
            raise QualityError("silver_uri_mismatch")
        return uri[len(prefix) :]
