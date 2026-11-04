# Phase 2 integration tests

Run `make infra-up`, `make migrate`, then `make integration-test`. These tests
use local MinIO/PostgreSQL and mocked Yahoo/FRED provider boundaries. Each run
creates a uniquely named temporary database and bucket and removes only those
test resources. The default `make test` excludes integration tests and makes
no live provider calls.
