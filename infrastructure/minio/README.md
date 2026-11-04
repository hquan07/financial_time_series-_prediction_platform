# Local Phase 2 MinIO

The `financial-ts-phase2` Compose project binds API port `127.0.0.1:19000`
and console port `127.0.0.1:19001`. Its `minio_data` volume is project scoped.
The idempotent `minio-init` service creates only the configured bucket. It
uses credentials from the ignored `.env`; no default credentials are supplied.
`make infra-down` retains data volumes. Do not use `down -v` for this project.

The public Quay pull was rejected during this implementation. The pinned
`Dockerfile` builds server and `mc` from upstream source tags. Build and tag
the image as described in `infrastructure/README.md` before starting Compose
on a fresh host. Record the resulting image version and digest at validation.
