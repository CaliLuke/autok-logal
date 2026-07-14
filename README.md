# autok-logal

Logal is Auto-K's local OpenTelemetry logs-and-traces collector. It accepts
OTLP/HTTP telemetry from the local applications and persists a recent,
queryable window in `../otel.debug.sqlite`.

This is disposable development infrastructure, not a production observability
platform. The most important rule is:

> Logal is the only process allowed to open `otel.debug.sqlite` read-write.
> Applications export OTLP logs and traces; they never write the database.

## Agent quick start

From the Auto-K monorepo, the normal entrypoint is the stack TUI:

```bash
stack
```

The stack starts Logal with `./autok-logal/scripts/run`, waits for
`http://127.0.0.1:13133/readyz`, and shows Logal's collector, reload, activity,
and shutdown output in its own pane. The runner uses Air, so changing Logal Go
code or configuration rebuilds and restarts the process automatically.

For a standalone session:

```bash
cd autok-logal
./scripts/run

curl -fsS http://127.0.0.1:13133/readyz
curl -fsS http://127.0.0.1:13133/status
```

The database is created at the monorepo root by default:

```text
autok-deploy/
├── otel.debug.sqlite
├── stack.toml
└── autok-logal/
```

## System model

```text
frontend / admin / auth / server
        │
        │ OTLP/HTTP logs and traces
        ▼
  127.0.0.1:4318
        │
        ▼
OpenTelemetry HTTP receiver
        │
        ├── logal_status middleware: readiness + admission limit
        │
        ▼
logal_sqlite exporters: validate + redact + normalize
        │
        ▼
logal_store: ownership + transactions + retention + disk limits
        │
        ▼
../otel.debug.sqlite (WAL mode, one read-write owner)

Health/status: 127.0.0.1:13133/{livez,readyz,status}
```

Logal is a small custom OpenTelemetry Collector distribution. Its main pieces
are:

| Path | Responsibility |
| --- | --- |
| `cmd/logal/main.go` | Registers only the OTLP receiver, Logal exporters, store, and status extension. |
| `config/local.yaml` | Wires the logs and traces pipelines and binds their local endpoints. |
| `internal/exporter` | Validates OTLP values, redacts sensitive fields, extracts query columns, and creates one SQLite record per log/span. |
| `internal/store` | Owns SQLite, verifies schema ownership, serializes writes, deduplicates records, enforces capacity, and performs retention. |
| `internal/status` | Exposes health/status endpoints, limits concurrent requests, counts rejections, and emits operational summaries. |
| `scripts/run` | Applies defaults and launches Air, or runs a direct one-shot binary when watching is disabled. |
| `scripts/contract-test` | Starts a temporary Logal and verifies ingestion, redaction, deduplication, and the metrics boundary. |
| `scripts/reset-db` | Refuses active files/ports and requires explicit confirmation before deleting the disposable database. |

## Scope and invariants

Keep these constraints intact when changing Logal:

- The configured database path is destructive and disposable. Logal may
  recreate empty, corrupt, non-SQLite, legacy, or untagged telemetry files. It
  must never point at a pre-existing file whose contents matter.
- Logal attempts to recognize and refuse foreign SQLite databases, but that
  check is defense in depth rather than permission to use an untrusted path.
- Symlinked, non-regular, or already-open database files are refused.
- Logs and traces are supported. Metrics are intentionally unsupported and
  `/v1/metrics` returns `404`.
- The collector binds to loopback and is intended only for local development.
- Apps communicate over OTLP/HTTP and must not share Logal's SQLite writer.
- Queries must be read-only and short-lived.
- Do not add a query API, dashboards, production deployment machinery, or
  long-term persistence here.

## First-time setup

### Prerequisites

- Go matching `go.mod` (currently Go 1.26.3).
- A C toolchain for `github.com/mattn/go-sqlite3` and CGO. On macOS, install
  Xcode Command Line Tools if compilation cannot find a compiler.
- `lsof`, used to prevent unsafe database reuse or replacement.
- `sqlite3` and `curl`; the contract test requires both.
- `jq` is optional but useful for `/status`.
- Air for automatic development rebuilds.

Install Air once:

```bash
go install github.com/air-verse/air@latest
```

Confirm the resolved setup without starting a process:

```bash
cd autok-logal
./scripts/run --dry-run
```

The output should show `runner=air`, the two ports, the collector config, and
the database path. If the `stack` command itself is not installed, build it
from the monorepo root:

```bash
./autok-stack/scripts/install
```

### Stack-managed operation

The parent [`stack.toml`](../stack.toml) declares Logal with:

- command `./autok-logal/scripts/run`;
- ports `4318` and `13133`;
- readiness `/readyz` and liveness `/livez`;
- a 30-second initial readiness timeout;
- automatic restart if the service process exits unexpectedly.

Air remains the long-running child of the stack service. A successful rebuild
sends Logal `SIGINT`, gives it up to two seconds to shut down gracefully, and
then starts the new binary. A compilation failure leaves the last good process
running. Restart the Logal service once after changing `.air.toml` or
`scripts/run`; Air cannot replace its own launcher configuration in place. In
the stack TUI, select Logal and press `r` to restart it. Press `q` or `Ctrl+C` to
gracefully stop the complete stack.

### Direct operation without Air

Use a single process for debugging the runner, contract tests, or environments
without Air:

```bash
AUTOK_LOGAL_WATCH=0 ./scripts/run
```

In direct mode, `scripts/run` rebuilds only when its tracked Go/module inputs
are newer or changed. `AUTOK_LOGAL_BINARY_PATH` controls that binary location.

Never run the stack-managed and standalone instances against the same database
at the same time. The second process should refuse to start rather than risk
corruption.

## Configuration

[`config/local.yaml`](config/local.yaml) is the collector topology. Environment
variables provide machine-specific paths and ports:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AUTOK_LOGAL_DB_PATH` | `<monorepo>/otel.debug.sqlite` | Destructive/disposable SQLite target; never point it at valuable data. |
| `AUTOK_LOGAL_RETENTION_HOURS` | `48` | Age cutoff for logs and spans; must be positive. |
| `AUTOK_LOGAL_OTLP_PORT` | `4318` | Loopback OTLP/HTTP receiver port. |
| `AUTOK_LOGAL_HEALTH_PORT` | `13133` | Loopback health and status port. |
| `AUTOK_LOGAL_CONFIG_PATH` | `autok-logal/config/local.yaml` | Collector configuration file. |
| `AUTOK_LOGAL_WATCH` | `1` | Set to `0` to bypass Air. |
| `AUTOK_LOGAL_BINARY_PATH` | `autok-logal/bin/logal` | Direct-mode binary path; ignored by Air mode. |

Relative path overrides are resolved from the directory where `scripts/run` is
invoked and converted to absolute paths before the runner changes directory.

Example isolated instance:

```bash
AUTOK_LOGAL_DB_PATH=/tmp/logal/otel.debug.sqlite \
AUTOK_LOGAL_OTLP_PORT=34318 \
AUTOK_LOGAL_HEALTH_PORT=33133 \
./scripts/run
```

Stop this standalone Air session with `Ctrl+C`. The example deliberately avoids
both the normal stack ports and the contract test's default ports.

The default receiver accepts:

- `POST http://127.0.0.1:4318/v1/logs`
- `POST http://127.0.0.1:4318/v1/traces`

The HTTP request body limit is 4 MiB. The exporter additionally rejects log
batches above 10,000 records, trace batches above 5,000 spans, more than 64 MiB
of cumulative normalized payload, body/attribute string or byte values above 1
MiB, and body/attribute values nested beyond 16 levels. The 1 MiB value limit
does not apply to every OTLP text field, such as span names or severity text.
These are request failures; partial batches are not committed.

Direct browser CORS allows `localhost` and `127.0.0.1` on port `3000`, but only
`localhost` on port `5173`. The Vite applications normally use their
`/signoz-otlp` proxy, while the Go server and auth service send directly to the
collector.

## Startup, ownership, and shutdown

On startup, the store performs this sequence before reporting ready:

1. Validate the database path and retention configuration.
2. Refuse symlinks, non-regular files, another Logal lock holder, or any process
   with the database/WAL/SHM open.
3. Inspect the SQLite application ID, schema version, required columns, stored
   schema signature, and `PRAGMA quick_check` result.
4. Recreate an absent, stale, corrupt, non-SQLite, or recognized legacy Logal
   database. This can include an untagged database containing `otel_logs` or
   `otel_spans`. Recognizable unrelated SQLite databases are refused, but the
   configured path must always be treated as destructive.
5. Open SQLite in WAL mode with full synchronous writes and one connection.
6. Execute and roll back a write probe.
7. Start maintenance, health/status reporting, the OTLP receiver, and finally
   mark the pipelines ready.

On graceful shutdown, ingestion becomes not-ready first. Logal stops its
reporter and HTTP status server, drains the collector lifecycle, stops
maintenance, checkpoints the WAL with `TRUNCATE`, closes SQLite, releases the
file lock, and removes the `.lock` file. Air reloads and stack quits use this
path, so their shutdown lines should remain visible in the TUI.

Read-only SQLite processes are safe while Logal is already running, but close
them promptly. A lingering `sqlite3` shell can prevent the next Air reload or
stack restart because startup deliberately refuses any open database
descriptor.

## Data handling

### Persistence and deduplication

Each OTLP record is normalized and committed transactionally:

- `otel_logs` has one row per log record. Its SHA-256 fingerprint is unique, so
  an identical redacted record is ignored on replay.
- `otel_spans` has one row per span and is unique on `(trace_id, span_id)`. An
  identical replay is ignored; different content for an existing identity
  rejects the transaction as invalid.
- `payload_json` contains the redacted single-record OTLP JSON envelope.
- `body_json` contains the redacted log body in a type-preserving tagged JSON
  representation, for example `{"string":"message"}`.
- `trace_id`, `span_id`, and `parent_span_id` are binary blobs. Use `hex(...)`
  in SQLite output.

Frequently queried attributes are copied into dedicated columns:

| OTLP field/attribute | SQLite column |
| --- | --- |
| resource `service.name` | `service_name` |
| log/span `request.id` | `request_id` |
| log/span `autok.product.id` | `product_id` |
| log `app.component` | `component` |
| log `event.name` or event name | `op` |

The log table indexes received time, trace/span identity, service/time,
request/time, product/time, and component/time. The span table indexes received
time, trace/span identity, and service/start time. Span `request_id` and
`product_id` are extracted for filtering but are not separately indexed.

Sensitive keys are recursively replaced with `[REDACTED]` before fingerprinting
or persistence. Matching is case-insensitive, ignores `.`, `_`, and `-`, and
recognizes fragments such as `authorization`, `cookie`, `password`, `passwd`,
`token`, `secret`, and `apikey`. This is a safety net, not authorization to put
secrets in telemetry; producers should still avoid emitting them.

### Retention and capacity protection

Maintenance runs every 30 seconds. It deletes expired logs and spans in bounded
batches, performs incremental vacuuming, and checkpoints the WAL.

Logal begins deleting the oldest telemetry under storage pressure. Current
guardrails are:

- 2 GiB main-database high-water mark;
- 3 GiB active database/WAL/SHM hard limit, with 64 MiB reserved for a request;
- 5 GiB free-disk floor, plus the request reserve;
- 256 MiB WAL readiness limit.

If capacity cannot safely admit another transaction, ingestion returns an
unavailable response rather than risking the machine or database. `/readyz`
also becomes unhealthy when those readiness limits are crossed.

## Health and operational logging

| Endpoint | Meaning |
| --- | --- |
| `/livez` | The status HTTP server is alive. It does not prove ingestion is safe. |
| `/readyz` | Pipelines and store are ready, disk/WAL limits are safe, and the request concurrency limit is not saturated. |
| `/status` | JSON details for pipeline readiness, in-flight/limit/middleware-rejected requests, store counters, sizes, oldest records, free disk, and the latest operational error. |

Inspect an unhealthy instance with:

```bash
curl -sS http://127.0.0.1:13133/status | jq
```

`committed_*`, `deleted_*`, and `rejected_requests` are counters for the current
Logal process. They reset after an Air or stack restart; they are not live table
row counts. `rejected_requests` counts only requests refused by middleware
because the pipeline is not ready or the concurrency limit is saturated. It
does not include receiver parsing/body-limit failures, exporter validation,
span identity conflicts, or store admission errors.

At info level Logal emits:

- an `operational reporting enabled` line at startup;
- an aggregated `Logal activity` summary, at most once every 10 seconds after
  counters or readiness change;
- an idle heartbeat once a minute.

The activity line includes committed/deleted logs and spans,
middleware-rejected requests, in-flight work, readiness, database/WAL bytes,
and free disk. Middleware rejections, not-ready state, and store errors are
warnings. Individual incoming records are never echoed to the console. The
collector's `Internal metrics telemetry disabled` startup message is expected
because Logal intentionally disables its own metrics pipeline.

## Querying telemetry

Always use SQLite read-only mode. From `autok-logal`:

```bash
# Recent logs
sqlite3 -readonly -column -header ../otel.debug.sqlite \
  "SELECT received_at_unix_nano, severity_text, service_name, component, op,
          json_extract(body_json, '$.string') AS message
   FROM otel_logs ORDER BY id DESC LIMIT 20"

# Errors from the last five minutes
sqlite3 -readonly -column -header ../otel.debug.sqlite \
  "SELECT received_at_unix_nano, service_name, severity_text, body_json
   FROM otel_logs
   WHERE severity_number >= 17
     AND received_at_unix_nano >= unixepoch('now', '-5 minutes') * 1000000000
   ORDER BY id DESC LIMIT 50"

# Recent spans with readable IDs
sqlite3 -readonly -column -header ../otel.debug.sqlite \
  "SELECT service_name, name, hex(trace_id) AS trace_id, hex(span_id) AS span_id,
          start_time_unix_nano, end_time_unix_nano
   FROM otel_spans ORDER BY id DESC LIMIT 20"

# Correlate logs with a trace
sqlite3 -readonly -column -header ../otel.debug.sqlite \
  "SELECT service_name, severity_text, component, op, body_json
   FROM otel_logs
   WHERE hex(trace_id)=upper('00112233445566778899aabbccddeeff')
   ORDER BY id"
```

Use `payload_json` when the indexed columns do not contain the field you need.
Use `.schema otel_logs` and `.schema otel_spans` instead of assuming an older
column layout from another repository's documentation.

## Adding or checking a telemetry producer

A local producer should:

1. Export OTLP/HTTP logs to `/v1/logs` and traces to `/v1/traces` on port 4318.
2. Set resource `service.name` to a stable application name.
3. Populate `request.id`, `autok.product.id`, `app.component`, and `event.name`
   when those correlation fields exist.
4. Treat `503` as temporary not-ready/saturation and retry with bounded
   backoff. Treat malformed or oversized payload responses as producer bugs.
5. Avoid metrics and never open the SQLite database read-write.

Verify the producer in three places:

```bash
curl -fsS http://127.0.0.1:13133/readyz
curl -sS http://127.0.0.1:13133/status | jq '.store, .rejected_requests'
sqlite3 -readonly ../otel.debug.sqlite \
  "SELECT service_name, COUNT(*) FROM otel_logs GROUP BY service_name"
```

Allow up to 10 seconds for the aggregated activity line; committed rows should
be queryable immediately after the OTLP request succeeds.

## Validation workflow

Before considering changes complete:

```bash
go test ./...
go build -o /tmp/autok-logal ./cmd/logal
./scripts/run --dry-run
./scripts/contract-test
```

The contract test uses a temporary database and the fixed non-stack ports
`24318`/`23133`. Stop any process using those ports first, or override them:

```bash
AUTOK_LOGAL_TEST_OTLP_PORT=25318 \
AUTOK_LOGAL_TEST_HEALTH_PORT=25133 \
./scripts/contract-test
```

The contract verifies:

- readiness;
- log and trace ingestion;
- recursive sensitive-field redaction;
- extracted correlation columns;
- log/span deduplication behavior;
- the intentional `/v1/metrics` `404`;
- graceful `SIGINT` shutdown.

Air excludes `*_test.go` from reload triggers, so changing only tests does not
restart the live collector. Run the test suite explicitly.

Do not commit generated binaries from `bin/`, `.tmp/`, `dist/`, or the repository
root.

## Troubleshooting

### `another Logal process owns ...` or `database already open`

There is another writer or a lingering reader. Do not delete the lock while a
process is alive.

```bash
lsof ../otel.debug.sqlite ../otel.debug.sqlite-wal ../otel.debug.sqlite-shm \
  ../otel.debug.sqlite.lock
lsof -nP -iTCP:4318 -iTCP:13133 -sTCP:LISTEN
```

Stop the duplicate Logal or close the read-only SQLite shell, then restart the
service.

### Port 4318 or 13133 is already in use

Use the `lsof` command above. Usually a standalone Logal was left running while
the stack tried to start another one. Stop the extra process; do not point two
collectors at the same database.

### `/livez` works but `/readyz` returns 503

`/livez` only confirms the status server. Inspect `/status` for
`pipeline_ready`, `in_flight`, `limit`, `.store.ready`, `.store.wal_bytes`,
`.store.free_bytes`, and `.store.last_error`. Disk/WAL pressure or request
saturation intentionally makes the service not-ready.

### Apps run but no rows appear

1. Check `/readyz` and `/status`.
2. Confirm the producer uses `/v1/logs` or `/v1/traces`, not `/v1/metrics`.
3. Confirm its endpoint resolves to `127.0.0.1:4318` or the Vite proxy.
4. Look for `requests_rejected` or collector validation errors in the Logal
   pane. A zero rejection counter does not rule out parsing, validation, or
   store errors because that counter covers only middleware refusal.
5. Query by `service_name`; missing resource names are stored as
   `unknown_service`.

### Air reports a build failure

The last good Logal should continue running. Fix the compiler error and Air
will retry after the next relevant change. Restart the service manually after
editing `.air.toml` or `scripts/run` itself: select Logal in the stack and press
`r`, or stop a standalone Air session with `Ctrl+C` and rerun `./scripts/run`.

### Metrics return 404

This is expected. Logal stores only logs and traces. Do not add an empty metrics
pipeline as a workaround.

### The disposable database needs a manual reset

Logal normally resets its own stale/corrupt owned schema. If manual cleanup is
necessary, stop Logal first. Use the guarded reset helper; without `--confirm`
it only resolves the configured target and verifies that neither its files nor
its configured ports are active:

```bash
./scripts/reset-db
./scripts/reset-db --confirm
```

The helper honors `AUTOK_LOGAL_DB_PATH`, `AUTOK_LOGAL_OTLP_PORT`, and
`AUTOK_LOGAL_HEALTH_PORT`; pass the same overrides used to start Logal. It
aborts if any database/lock file is open or either port is listening, and the
destructive action requires `--confirm`. Then restart Logal. Never run this
against a path whose ownership or contents matter.
