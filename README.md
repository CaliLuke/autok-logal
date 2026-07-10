# autok-logal

Logal is Auto-K's local OpenTelemetry logs-and-traces collector. One Go process accepts OTLP/HTTP and is the only read-write owner of `../otel.debug.sqlite`.

Its purpose is deliberately narrow: give developers and coding agents one dependable, queryable place to inspect recent local logs and traces. It is development tooling, not a production observability platform.

```text
frontend / admin / auth / server
  -> OTLP/HTTP :4318
  -> Logal Collector distribution
  -> SQLite logs + spans
```

The database is disposable. On startup, Logal recreates a missing, stale, or corrupt database instead of migrating it. Old rows are removed after 48 hours; the live database keeps one stable pathname and is never generation-rotated.

## Scope boundary

Logal should stay small and boring:

- Store recent local logs and traces in SQLite.
- Bound disk use and clean up old rows automatically.
- Start, stop, and restart with the local Auto-K stack.
- Support direct read-only SQLite queries.

Logal does not preserve data. Do not add database migrations, backups, archives, restore flows, historical merging, a query API, dashboards, metrics storage, or production deployment machinery. If the schema changes, recreate the disposable database and move on.

## Run

```bash
./scripts/run
curl -fsS http://127.0.0.1:13133/readyz
```

Overrides:

```bash
AUTOK_LOGAL_DB_PATH=/Users/luca/code/autok/otel.debug.sqlite
AUTOK_LOGAL_RETENTION_HOURS=48
AUTOK_LOGAL_OTLP_PORT=4318
AUTOK_LOGAL_HEALTH_PORT=13133
```

## Query

Always open the live database read-only:

```bash
sqlite3 -readonly ../otel.debug.sqlite \
  "SELECT received_at_unix_nano, service_name, severity_number, body_json FROM otel_logs ORDER BY id DESC LIMIT 20"

sqlite3 -readonly ../otel.debug.sqlite \
  "SELECT trace_id, span_id, service_name, name, start_time_unix_nano FROM otel_spans ORDER BY id DESC LIMIT 20"
```

## Validate

```bash
go test ./...
go build ./cmd/logal
./scripts/contract-test
./scripts/run --dry-run
```
