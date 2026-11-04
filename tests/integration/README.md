# Phase 2 integration tests

Run `make infra-up`, `make migrate`, then `make integration-test`. These tests
use local MinIO/PostgreSQL and mocked Yahoo/FRED provider boundaries. Each run
creates a uniquely named temporary database and bucket and removes only those
test resources. The default `make test` excludes integration tests and makes
no live provider calls.

`make integration-test` uses pytest's one-line traceback so a failed database
connection does not print its connection string and local credentials. Run a
specific failing test with additional diagnostics only after redacting secrets.
