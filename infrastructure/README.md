# Phase 2 local infrastructure

`docker-compose.yml` starts only `financial-ts-phase2` PostgreSQL and MinIO.
It maps `127.0.0.1:15432`, `:19000`, and `:19001`, with project-scoped
`postgres_data` and `minio_data` volumes. `make infra-up` waits for service
health and runs an idempotent bucket init. `make infra-down` does not delete
volumes. Never use a global Docker prune or `down -v` for this project.

The MinIO community container registry rejected pulls during development.
`minio/Dockerfile` builds MinIO server and client from fixed upstream release
tags using Go. `make infra-up` builds this image before starting services.
It can also be built explicitly:

```bash
docker build -t financial-ts-phase2/minio:2025-10-15 -f infrastructure/minio/Dockerfile infrastructure/minio
```

PostgreSQL uses `postgres:16`. Verify the built image digest and service
health before relying on a new deployment.
