# autok-logal

`autok-logal` is the local Auto-K log collector. It uses Fluent Bit for ingestion, buffering, retry, and backpressure, then writes correlated local logs into one SQLite database through a tiny Fluent Bit output plugin.

## Why This Exists

The old local setup let multiple Bun helper processes open and mutate the same `otel.debug.sqlite` file. That caused WAL sidecar drift, `SQLITE_IOERR_SHORT_READ`, `disk I/O error`, and `Cannot use a closed database` loops.

The intended shape is now:

```text
frontend/admin/auth/server
  -> HTTP or OTLP
  -> Fluent Bit
  -> autok_sqlite output plugin
  -> ../otel.debug.sqlite
```

Fluent Bit is the collector and buffer. The plugin is only the final SQLite sink.

## Ports

| Port | Purpose |
| --- | --- |
| `4318` | Browser-facing OTLP HTTP input with local CORS handling for `/v1/logs` and `/v1/traces` |
| `4319` | Internal Fluent Bit OTLP HTTP input behind the local CORS proxy |
| `3847` | frontend debug log compatibility input |
| `3848` | admin debug log compatibility input |
| `2020` | Fluent Bit health/metrics HTTP server |

## Install Fluent Bit

```bash
brew install fluent-bit
```

The Go output plugin API requires a Fluent Bit build with Go proxy plugin support. Homebrew builds may vary; `./scripts/run --dry-run` checks that `fluent-bit` is available, and a real run will fail early if plugin loading is unsupported.

## Build

```bash
./scripts/build-plugin
```

This writes the plugin to `dist/out_autok_sqlite.so`.

## Run

From this repo:

```bash
./scripts/run
```

From the Auto-K parent repo:

```bash
./dev-traces
```

The normal dev stack starts Logal through `stack.toml` with the same `./scripts/run`
entrypoint. The script keeps the OTLP CORS proxy and Fluent Bit tied together:
if either child exits, the script exits so `stack` can show the service as
failed instead of leaving a half-working collector behind.

For manual background runs outside `stack`:

```bash
./scripts/run --daemon
tail -f ../.tmp/autok-logal.log
```

Useful environment overrides:

```bash
AUTOK_LOGAL_DB_PATH=/Users/luca/code/autok/otel.debug.sqlite
AUTOK_LOGAL_STORAGE_PATH=/Users/luca/code/autok/.tmp/logal-buffer
AUTOK_LOGAL_RETENTION_HOURS=48
AUTOK_LOGAL_FLUENT_BIT_BIN=fluent-bit
AUTOK_LOGAL_OTLP_EXTERNAL_PORT=4318
AUTOK_LOGAL_OTLP_INTERNAL_PORT=4319
AUTOK_LOGAL_OTLP_CORS_ORIGINS=https://localhost:3000,http://localhost:3000
```

## Query

```bash
sqlite3 ../otel.debug.sqlite "SELECT timestamp, severity_text, component, op, body, trace_id FROM otel_logs ORDER BY id DESC LIMIT 20"
sqlite3 ../otel.debug.sqlite "SELECT timestamp, json_extract(resource_json, '$.\"service.name\"') AS service_name, severity_text, body FROM otel_logs ORDER BY id DESC LIMIT 20"
sqlite3 ../otel.debug.sqlite "SELECT occurred_at, phase, message, record_count FROM autok_logal_errors ORDER BY id DESC LIMIT 20"
```

## Schema

The plugin writes the shared `otel_logs` table used by Auto-K debugging:

- indexed correlation columns: `trace_id`, `span_id`, `request_id`, `product_id`
- indexed triage columns: `timestamp`, `severity_text`, `component`, `op`, `body`
- raw context columns: `attributes_json`, `resource_json`

It also creates a `logs` view for older local queries.

The plugin repairs the old local `project_id` column shape by adding and
backfilling `product_id` before ingest. If SQLite writes fail after startup, the
plugin records the failure in `autok_logal_errors` and asks Fluent Bit to retry.
