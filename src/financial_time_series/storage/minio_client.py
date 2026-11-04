"""Minimal MinIO object operations with read-back verification."""

from io import BytesIO

from minio import Minio
from minio.error import S3Error

from financial_time_series.settings import Settings


class MinioStore:
    def __init__(self, settings: Settings):
        self.bucket = settings.minio_bucket
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            secure=settings.minio_secure,
        )

    def require_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            raise RuntimeError("configured MinIO bucket does not exist")

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as error:
            if error.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return False
            raise

    def put_new(self, key: str, payload: bytes, content_type: str) -> None:
        if self.exists(key):
            raise FileExistsError("Bronze object key already exists")
        self.client.put_object(
            self.bucket,
            key,
            BytesIO(payload),
            len(payload),
            content_type=content_type,
        )

    def read(self, key: str) -> bytes:
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
