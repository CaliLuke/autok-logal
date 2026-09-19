# autok-logal

Logal collects and inspects OpenTelemetry logs, traces, and metrics during local app development.
Point an app's standard OTLP exporters at Logal to inspect its instrumentation without a local SigNoz deployment.
Logal accepts OTLP/gRPC and OTLP/HTTP and stores recent telemetry in SQLite.
The Auto-K runner uses `../otel.debug.sqlite` by default.

Version 0.3.1 restores metrics support removed in 0.3.0. Schema version 7 contains
the dedicated `otel_metric_points` table. The next start resets older disposable
databases, including all telemetry in schema versions 5 and 6.

Logal focuses on OpenTelemetry instrumentation: resources, scopes, attributes, span context, and metric semantics.
It uses disposable storage with bounded queries. Dashboards, alerting, proprietary ingestion formats, and production storage are outside its scope.
The most important rule is:

> Logal is the only process allowed to open `otel.debug.sqlite` read-write.
> Applications export OTLP logs, traces, and metrics. They never write the database.

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
        │ OTLP/gRPC :4317 or OTLP/HTTP :4318
        ▼
Official OpenTelemetry OTLP receiver
        │
        ├── logal_status middleware: shared HTTP/gRPC admission limit
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
| `config/local.yaml` | Wires the logs, traces, and metrics pipelines and binds their local endpoints. |
| `internal/exporter` | Validates OTLP values, redacts sensitive fields, extracts query columns, and creates one SQLite record per log, span, or metric point. |
| `internal/query` | Runs bounded read-only queries and formats telemetry values for the CLI. |
| `internal/store` | Owns SQLite, verifies schema ownership, serializes writes, deduplicates records, enforces capacity, and performs retention. |
| `internal/status` | Exposes health/status endpoints, limits concurrent requests, counts rejections, and emits operational summaries. |
| `scripts/run` | Applies defaults and launches Air, or runs a direct one-shot binary when watching is disabled. |
| `cmd/logal/*_test.go` | Runs the real collector against temporary databases to check protocol handling, persistence, and recovery. |
| `scripts/verify` | Runs static checks, race-enabled tests, a build, and the runner configuration check. |
| `scripts/contract-test` | Runs only the collector contract tests with detailed output. |
| `scripts/reset-db` | Refuses active files/ports and requires explicit confirmation before deleting the disposable database. |

## Scope and invariants

Keep these constraints intact when changing Logal:

- The configured database path is destructive and disposable. Logal may
  recreate empty, corrupt, non-SQLite, legacy, or untagged telemetry files. It
  must never point at a pre-existing file whose contents matter.
- Logal attempts to recognize and refuse foreign SQLite databases, but that
  check is defense in depth rather than permission to use an untrusted path.
- Logal refuses symlinked, hard-linked, non-regular, or already-open database files.
- Logs, traces, and metrics share one database, retention policy, and pressure
  policy.
- The collector binds to loopback and is intended only for local development.
- Apps communicate over standard OTLP/gRPC or OTLP/HTTP and must not share Logal's SQLite writer.
- Queries must be read-only and short-lived.
- Do not add a query API, dashboards, production deployment machinery, or
  long-term persistence here.

## Install

```bash
brew install CaliLuke/logal/logal
```

The Homebrew package installs one `logal` executable. Each project starts its
own process with a project-owned collector configuration, ports, and SQLite
database.

## First-time setup

### Prerequisites

- Go matching `go.mod` (currently Go 1.26.3).
- A C toolchain for `github.com/mattn/go-sqlite3` and CGO. On macOS, install
  Xcode Command Line Tools if compilation cannot find a compiler.
- `lsof`, used to prevent unsafe database reuse or replacement.
- `sqlite3` and `curl` are optional tools for manual database and endpoint inspection.
  The automated contract tests use Go clients and SQLite directly.
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

The output should show `runner=air`, all three listener endpoints, the
collector config, and the database path. If `stack` itself is not installed,
build it from the monorepo root:

```bash
./autok-stack/scripts/install
```

### Stack-managed operation

The parent [`stack.toml`](../stack.toml) declares Logal with:

- command `./autok-logal/scripts/run`;
- ports `4317`, `4318`, and `13133`;
- readiness `/readyz` and liveness `/livez`;
- a 30-second initial readiness timeout;
- automatic restart if the service process exits unexpectedly.

Air remains the long-running child of the stack service. A successful rebuild
sends Logal `SIGINT`, gives it up to ten seconds to shut down gracefully, and
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
variables provide machine-specific paths, full bind endpoints, and limits:

| Variable | Default | Purpose |
| --- | --- | --- |
| `LOGAL_DB_PATH` | `<monorepo>/otel.debug.sqlite` | Destructive/disposable SQLite target; never point it at valuable data. |
| `LOGAL_RETENTION_HOURS` | `48` | Receipt-time cutoff for every signal; must be positive. |
| `LOGAL_OTLP_GRPC_ENDPOINT` | `127.0.0.1:4317` | Full OTLP/gRPC bind endpoint. |
| `LOGAL_OTLP_HTTP_ENDPOINT` | `127.0.0.1:4318` | Full OTLP/HTTP bind endpoint. |
| `LOGAL_HEALTH_ENDPOINT` | `127.0.0.1:13133` | Full health/status bind endpoint. |
| `LOGAL_MAX_IN_FLIGHT_REQUESTS` | `8` | Admission limit shared by HTTP and gRPC exports. |
| `AUTOK_LOGAL_CONFIG_PATH` | `autok-logal/config/local.yaml` | Auto-K runner configuration file. |
| `AUTOK_LOGAL_WATCH` | `1` | Set to `0` to bypass Air. |
| `AUTOK_LOGAL_BINARY_PATH` | `autok-logal/bin/logal` | Direct-mode binary path; ignored by Air mode. |

Port-only conveniences `LOGAL_OTLP_GRPC_PORT`, `LOGAL_OTLP_HTTP_PORT`, and
`LOGAL_HEALTH_PORT` construct loopback defaults when the corresponding full
endpoint is not set. Full endpoints take precedence and allow alternate
loopback addresses, IPv6, containers, and isolated project profiles.

Relative path overrides are resolved from the directory where `scripts/run` is
invoked and converted to absolute paths before the runner changes directory.

Example isolated instance:

```bash
LOGAL_DB_PATH=/tmp/logal/otel.debug.sqlite \
LOGAL_OTLP_GRPC_ENDPOINT=127.0.0.1:34317 \
LOGAL_OTLP_HTTP_ENDPOINT=127.0.0.1:34318 \
LOGAL_HEALTH_ENDPOINT=127.0.0.1:33133 \
./scripts/run
```

Stop this standalone Air session with `Ctrl+C`. The example deliberately avoids
both the normal stack ports and the contract test's default ports.

The default receiver accepts OTLP/gRPC on `127.0.0.1:4317` and OTLP/HTTP on:

- `POST http://127.0.0.1:4318/v1/logs`
- `POST http://127.0.0.1:4318/v1/traces`
- `POST http://127.0.0.1:4318/v1/metrics`

Metrics support gauges, sums, histograms, exponential histograms, and summaries.

Both protocols feed the same official Collector receiver, exporters, store,
transactions, retention, and deduplication path. The HTTP and gRPC receive
limits are 4 MiB. Exporters additionally enforce signal count, normalized
payload, scalar-size, and nesting limits; request failures never partially
commit a batch.

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
   database. An untagged database qualifies only when all its tables have recognized Logal names.
   Logal refuses untagged databases with unrelated tables. The configured path remains destructive.
5. Open SQLite in WAL mode with full synchronous writes and one connection.
6. Execute and roll back a write probe.
7. Start maintenance, health/status reporting, the OTLP receiver, and finally
   mark the pipelines ready.

On graceful shutdown, ingestion becomes not-ready first. Logal stops its
reporter and HTTP status server, drains the collector lifecycle, stops
maintenance, checkpoints the WAL with `TRUNCATE`, closes SQLite, releases the
file lock. The `.lock` file remains in place so each process uses the same lock
inode. A checkpoint failure still closes SQLite and releases ownership.

Read-only SQLite processes are safe while Logal is already running, but close
them promptly. A lingering `sqlite3` shell can prevent the next Air reload or
stack restart because startup deliberately refuses any open database
descriptor.

## Data handling

### Persistence and deduplication

Each OTLP record is normalized and committed transactionally:

- `otel_logs` has one row per log record. Its SHA-256 fingerprint is unique, so
  an identical redacted record is ignored on replay. Attribute order does not
  affect the fingerprint.
- `otel_spans` has one row per span and is unique on `(trace_id, span_id)`. An
  identical replay is ignored; different content for an existing identity
  rejects the transaction as invalid.
- `otel_metric_points` has one row per metric point. Its unique fingerprint
  suppresses exact retries while preserving distinct values at the same timestamp.
- `payload_json` contains the redacted single-record OTLP JSON envelope.
- `body_json` contains the redacted log body in a type-preserving tagged JSON
  representation, for example `{"string":"message"}`.
- `trace_id`, `span_id`, and `parent_span_id` are binary blobs. Use `hex(...)`
  in SQLite output.

Logal copies string attributes into dedicated query columns. Maps, arrays, and
other types remain in the redacted payload and cannot leak nested secrets into
these columns:

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

Logal replaces recognized sensitive fields with `[REDACTED]` before persistence.
The matcher recognizes `authorization`, `cookie`, `set_cookie`, `password`,
`passwd`, `secret`, `api_key`, and named credential tokens such as `access_token`.
It also recognizes `apiKey`, `accessToken`, `clientSecret`, `proxy-authorization`, and prefixed names such as `db_password`.
It also checks nested maps, log bodies, span events, span links, metric metadata,
and exemplar attributes.
Numeric token counts such as `gen_ai.usage.input_tokens` remain intact.
Free-text messages are not scanned for secrets. Producers must exclude secrets
from those messages.

### Retention and capacity protection

Maintenance runs every 30 seconds. It deletes expired records in batches of 5,000 per signal.
It repeats across all three signals until the backlog is empty or the deadline expires.
Then it performs incremental vacuuming and checkpoints the WAL. Each maintenance
cycle has a five-second deadline. A blocked checkpoint stops pressure eviction
until a later cycle. Logal reclaims existing free pages before it deletes fresh records.
SQLite reports blocked checkpoints through [result rows](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint).

Logal begins deleting the oldest telemetry under storage pressure. Current
guardrails are:

- 2 GiB main-database high-water mark;
- 3 GiB admission threshold for the database/WAL/SHM, with a 64 MiB request reserve;
- 5 GiB free-disk floor, plus the request reserve;
- 256 MiB WAL readiness limit.

If capacity cannot safely admit another transaction, ingestion returns an
unavailable response rather than risking the machine or database. `/readyz`
also becomes unhealthy when those readiness limits are crossed.
SQLite storage failures stop admission and appear in `last_error`.
Successful maintenance restores readiness. Invalid records and canceled requests do not mark the store unhealthy.

## Health and operational logging

| Endpoint | Meaning |
| --- | --- |
| `/livez` | The status HTTP server is alive. It does not prove ingestion is safe. |
| `/readyz` | Pipelines and store are ready, disk/WAL limits are safe, and the request concurrency limit is not saturated. |
| `/status` | JSON details for pipeline readiness, in-flight/limit/middleware-rejected requests, store counters, sizes, oldest records, free disk, and the latest operational error. |

The status extension uses its `store` configuration field to select the store extension.
The default is `logal_store`. Unexpected status-server failure stops the collector.

The `/status` endpoint waits at most one second for database details.
If the writer remains busy, the response contains operational counters and `snapshot_error`.
The oldest-record fields are incomplete in that response.
Canceled exports also stop their wait for the writer.

Inspect an unhealthy instance with:

```bash
curl -sS http://127.0.0.1:13133/status | jq
```

`committed_*`, `deleted_*`, and `rejected_requests` are counters for the current
Logal process. They reset after an Air or stack restart; they are not live table
row counts. `rejected_requests` counts only requests refused by middleware
because the pipeline/store is not ready or the concurrency limit is saturated. It
does not include receiver parsing/body-limit failures, exporter validation,
span identity conflicts, or store admission errors.

At info level Logal emits:

- an `operational reporting enabled` line at startup;
- an aggregated `Logal activity` summary, at most once every 10 seconds after
  counters or readiness change;
- an idle heartbeat once a minute.

The activity line includes committed/deleted logs, spans, and metric points, rejected requests,
active requests, readiness, database/WAL bytes, and free disk.
Middleware rejections, unready state, and store errors produce warnings.
Individual records never appear in the console output.

## Querying OpenTelemetry data

Build the CLI from `autok-logal`:

```bash
go build -o bin/logal ./cmd/logal
export LOGAL_DB_PATH="$(cd .. && pwd)/otel.debug.sqlite"

# Check which services export each signal.
bin/logal services --json

# Find failed server spans and inspect their trace.
bin/logal spans --service checkout --kind server --status error --min-duration 100ms --json
bin/logal trace 00112233445566778899aabbccddeeff --json

# Match resource attributes separately from log attributes.
bin/logal logs --service checkout --resource deployment.environment.name=development \
  --attribute http.request.method=GET --level error --json

# Discover instruments, then inspect their individual streams.
bin/logal metrics list --service checkout --json
bin/logal metrics series --name http.server.request.duration --json
bin/logal metrics points --name http.server.request.duration --type histogram --json

# Calculate rates within each monotonic sum stream.
bin/logal metrics rate --name app.requests --service checkout --time event --since 10m --json
```

`metrics` without a subcommand behaves as `metrics points`.
Metric flags work before or after the subcommand.
Use `logal COMMAND --help` or `logal metrics rate --help` for the complete argument list.
Existing collector startup flags remain available. `logal serve --help` shows those flags.

### OpenTelemetry filters and output

| Argument | Meaning |
| --- | --- |
| `--service NAME` | Exact resource `service.name`. |
| `--resource KEY=VALUE` | Match a resource attribute. Repeat to require multiple attributes. |
| `--scope NAME`, `--scope-version VERSION` | Match the instrumentation scope. |
| `--attribute KEY=VALUE` | Match a log, span, or metric point attribute. Repeat to require multiple attributes. |
| `--since DURATION_OR_TIMESTAMP` | Inclusive start of the query window. Default: 15 minutes ago. |
| `--until TIMESTAMP` | Exclusive end of the query window, in RFC3339 format. |
| `--time received\|event` | Select the timestamps used for filtering. Default: receipt time. |
| `--payload` | Include the stored, redacted OTLP JSON envelope for each record. |
| `--columns NAME,NAME` | Select output columns and their order, before the byte limit applies. Works with JSON. |
| `--list-columns` | List available columns without reading telemetry. Built-in views need no database. |
| `--details` | Show all fields and complete cells in human output. JSON already includes all fields. |
| `--include-empty` | Restore empty fields within the selected view, in text and JSON output. |

Attribute filters preserve scalar types. `http.response.status_code=200` matches an OTLP integer.
`success=true` matches a boolean. Bare values such as `http.request.method=GET` match strings.
Use shell quotes around `'build.version="200"'` to match a numeric string.
Use `200.0` to match a double. Arrays, maps, and null are not scalar filter values.
Commas remain part of a value. Repeated flags require every filter to match.

Receipt time answers when Logal received a record. Event time uses the log timestamp, span start, or metric point timestamp.
Event-time filters do not substitute receipt time when an OTLP timestamp is missing.
Rate calculations always use metric timestamps, regardless of the filter time basis.

`spans` supports `--name`, `--kind`, `--status`, `--min-duration`, `--trace-id`, and `--span-id`.
`logs` supports `--level`, `--trace-id`, and `--span-id`.
`trace TRACE_ID` returns correlated spans and logs in event-time order, with receipt time as a fallback.
Span results include status, duration, events, links, resources, scopes, and attributes.
Log results include severity, body, event name, span context, resources, scopes, and attributes.
Custom application attributes use the same filters as semantic-convention attributes.

### Metric semantics

`metrics list` groups metric descriptors and reports series counts.
`metrics series` includes point attributes and reports each stream's point count and timestamp range.
`metrics points` exposes units, descriptions, temporality, monotonicity, flags, and attributes.
It also exposes explicit buckets, exponential buckets, summary quantiles, and exemplars.
Exemplar trace IDs can be passed to `logal trace`.
Use `--name`, `--type`, and `--temporality` to select metrics.

Discovery preserves resource and scope attributes, schema URLs, metric names, units, types, and aggregation properties.
Individual streams also preserve point attributes. Integer and double samples can belong to the same stream.
Description changes do not create new streams.
Resource attributes distinguish service instances when the producer supplies `service.instance.id`.
`services` summarizes signal counts by `service.name`; metric queries retain the complete resource identity.
Metric views show a stable `resource_id` derived from resource attributes and the resource schema URL.
This identifier distinguishes resources that share a service name. It is a Logal fingerprint, not an OTLP attribute.
Metric lists also show `last_received_at` to help identify resources that still send data.
Use `--resource-id ID` to select that resource in metric lists, series, points, or rates.
Full resource and stream identity still determine groups and rates.

```bash
bin/logal metrics list --name nodejs.eventloop.time
bin/logal metrics points --name nodejs.eventloop.time --resource-id RESOURCE_ID
```

`metrics rate` requires `--name` and calculates a rate for each valid monotonic sum interval:

- Delta sums divide the reported value by the reported interval duration.
- Cumulative sums use consecutive points from the same stream and start timestamp.
- The first cumulative point in the selected window has no baseline and returns no rate.
- Changed start timestamps report a reset. Later points can establish a new rate.
- Zero-duration points contribute no rate but can establish a cumulative baseline.
- Missing values, absent-value flags, invalid timestamps, decreases, overlaps, and ambiguous timestamps return no rate with a `rate_reason`.
- Delta gaps appear as `gap_seconds`. Rates cover reported intervals and do not fill those gaps.

Valid rates include `interval_start`, `interval_end`, `delta_value`, and `rate_per_second` in JSON and detailed output.
The rate unit is the original metric unit per second.
Gauges and non-monotonic sums return `not_monotonic_sum` instead of an invented counter rate.
An unspecified aggregation temporality also returns no rate.
Calculations use the entire selected window before output pagination.

Histogram output preserves the original OTLP buckets and temporality.
The CLI does not add overlapping cumulative histograms, average summary quantiles, or invent window percentiles.
It does not merge series across different resources or point attributes.
These rules follow the [OpenTelemetry metrics data model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/).

### Query bounds and advanced inspection

`--db PATH` overrides `LOGAL_DB_PATH`. Queries require an existing database with the current Logal schema.
They never create, reset, or repair a database. They work while the collector runs and after it stops.
The CLI opens SQLite read-only and closes its connection before it prints results.
All commands use urfave/cli v3, apart from the Collector's native startup command handling.

Queries return at most 100 rows by default and have a two-second deadline.
Use `--limit`, `--timeout`, and `--max-bytes` to adjust the limits.
Maximum values are 10,000 output rows, 30 seconds, and 16 MiB. The default output budget is 1 MiB.
Discovery and rate queries can scan more input rows than the output limit. The deadline also bounds that work.

By default, rows omit nulls, empty strings, and empty arrays or objects.
Numeric zero and boolean false remain visible. Nested OTLP attributes and payloads remain intact.
The `columns` list includes fields present in at least one row on the current page.
Text output leaves a cell blank when that row omits its field.
Use `--include-empty` to restore empty fields and every selected column, including for SQL queries.
Use `--details --include-empty` for the complete human view.

```bash
bin/logal metrics points --name http.server.request.duration --json --include-empty
```

With `--json`, stdout contains one object with `columns`, `rows`, `count`, and `truncated`.
Truncated results also contain `truncation_reason` and `next_offset`.
Pass that offset through `--offset` to read another page, up to an offset of 1,000,000.
Pages use separate snapshots. Ingestion, retention, and relative time windows can shift their boundaries.
Use fixed `--since` and `--until` timestamps for repeatable time windows. Offsets are not durable cursors.
An empty result is a successful query.

Built-in commands format timestamps as UTC RFC3339 strings and IDs as lowercase hexadecimal strings.
Resource and point attributes retain their OTLP AnyValue structure, including integer strings.
The result envelope is a CLI inspection format. `--payload` contains the stored OTLP envelope.
Human output defaults to an aligned summary with readable span kinds, statuses, and severity labels.
Trace summaries show elapsed milliseconds from the earliest trace event and each span's duration.
Trace rows use `kind` to identify spans or logs; `span_kind` retains the OTLP span kind.
Span rows also include `status_message` when the producer supplies it.
The timeline preserves event order and parent IDs. It does not infer causality from timestamps.
Summary cells longer than 64 characters have a visible truncation marker.
`--details` restores all fields and complete values. JSON values are never shortened.
Control characters remain escaped in every human view.

Use `--list-columns` to discover the names accepted by `--columns` for each view.
Built-in views need no database, trace ID, or metric name for this option.
Add `--payload` to include the optional payload column. Metric discovery summaries do not support payloads.
JSON column discovery returns an object with a `columns` array.
For SQL, supply a database and query. Logal prepares the query without evaluating its rows.

```bash
bin/logal trace --list-columns
bin/logal metrics rate --list-columns --json
bin/logal metrics points --list-columns --payload
bin/logal sql --query 'SELECT service_name, count(*) AS logs FROM otel_logs GROUP BY service_name' --list-columns
```

```bash
bin/logal metrics list --columns metric_name,metric_type,unit,series_count --json
bin/logal trace 00112233445566778899aabbccddeeff --details
```
Errors return exit status 1. With `--json`, stderr contains an `error` object with `code` and `message`.
A query failure does not print partial results.

The `sql` command remains an escape hatch for inspecting stored OTLP fields:

```bash
bin/logal sql --query 'SELECT service_name, count(*) AS n FROM otel_logs GROUP BY service_name' --json
bin/logal sql --file query.sql --json
```

It accepts one SELECT or WITH query, limited to 64 KiB. `--file -` reads SQL from stdin.
A SQLite authorizer rejects writes and extension loading.
SQL preserves scalar values and encodes blobs as hexadecimal strings.
Use an explicit `ORDER BY` for repeatable SQL pagination.
Direct SQLite inspection must also use read-only mode:

```bash
sqlite3 -readonly "$LOGAL_DB_PATH" '.schema otel_metric_points'
```

Non-finite metric values remain in the payload. Their numeric query columns contain SQL `NULL`.
Aggregate counts use text to preserve unsigned 64-bit values.

## Adding or checking a telemetry producer

Configure the app's OpenTelemetry SDK with a standard OTLP exporter. For SDKs that support these environment variables:

```bash
export OTEL_SERVICE_NAME=checkout
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=development,service.instance.id=checkout-local
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

These variables configure an exporter; they do not install instrumentation or enable disabled signal exporters.
The SDK still needs its log, trace, and metric providers and exporters.
Existing signal-specific endpoint variables take precedence over the general endpoint.
OTLP/HTTP uses `/v1/logs`, `/v1/traces`, and `/v1/metrics`. OTLP/gRPC uses port 4317.
Exporter configuration follows the [OpenTelemetry OTLP exporter specification](https://opentelemetry.io/docs/specs/otel/protocol/exporter/).

A local producer must:

1. Export each signal through its standard OTLP endpoint.
2. Set resource `service.name` and identify distinct service instances when needed.
3. Use OpenTelemetry semantic conventions and retain useful custom attributes.
4. Retry temporary unavailable responses with bounded backoff.
5. Send malformed payload and invalid-value failures to the app's diagnostic output.
6. Keep all SQLite write access inside Logal.

Verify the producer in three places:

```bash
curl -fsS http://127.0.0.1:13133/readyz
curl -sS http://127.0.0.1:13133/status | jq '.store, .rejected_requests'
bin/logal services --db ../otel.debug.sqlite --json
```

Allow up to 10 seconds for the aggregated activity line; committed rows should
be queryable immediately after the OTLP request succeeds.

## Validation workflow

Run the complete verification suite:

```bash
./scripts/verify
```

This command runs `go vet`, uncached tests with the race detector, a build, and the standalone runner configuration check.
The collector subprocess also uses the race detector.
The runner check uses direct mode, so verification does not require Air.
GitHub Actions runs the same command on Linux and macOS.

The contract tests are part of `go test ./...`.
They build the real collector and use temporary databases and automatically selected loopback ports.
They retry startup after an automatic port collision and print collector logs on failure.
The tests require Go, a C toolchain, and `lsof`.
They do not require `curl` or the `sqlite3` command.

Run only the contract tests with detailed output:

```bash
./scripts/contract-test
```

To skip collector subprocess tests:

```bash
go test -short ./...
```

To inspect a specific listener, override its test port:

```bash
LOGAL_TEST_OTLP_GRPC_PORT=25317 \
LOGAL_TEST_OTLP_HTTP_PORT=25318 \
LOGAL_TEST_HEALTH_PORT=25133 \
./scripts/contract-test
```

Explicit ports must be available. Separate test runs can overlap with automatic ports.
The contract suite checks:

- readiness, simultaneous collector isolation, and graceful shutdown;
- HTTP and gRPC ingestion for all three signals;
- all five metric types, payload fidelity, and exemplar correlation;
- redaction, correlation fields, and retry deduplication;
- malformed JSON/protobuf, unsupported content types, and request size limits;
- invalid batches and span conflicts, with no partial persistence;
- abrupt process termination, WAL recovery, and retries after restart;
- fresh writes and database integrity after recovery;
- CLI queries while running and stopped, trace correlation, filters, and structured errors.

The store tests also force `SQLITE_FULL` through a temporary database page limit.
This checks transaction rollback and readiness recovery without filling the host disk.

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
lsof -nP -iTCP:4317 -iTCP:4318 -iTCP:13133 -sTCP:LISTEN
```

Stop the duplicate Logal or close the read-only SQLite shell, then restart the
service.

### Port 4317, 4318, or 13133 is already in use

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
2. Confirm the producer uses OTLP/gRPC on `127.0.0.1:4317` or the
   signal-matching OTLP/HTTP endpoint on `127.0.0.1:4318`.
3. Look for `requests_rejected` or collector validation errors in the Logal
   pane. A zero rejection counter does not rule out parsing, validation, or
   store errors because that counter covers only middleware refusal.
4. Query by `service_name`; missing resource names are stored as
   `unknown_service`.

### Air reports a build failure

The last good Logal should continue running. Fix the compiler error and Air
will retry after the next relevant change. Restart the service manually after
editing `.air.toml` or `scripts/run` itself: select Logal in the stack and press
`r`, or stop a standalone Air session with `Ctrl+C` and rerun `./scripts/run`.

### Metrics are not appearing

Use Logal 0.3.1 or later. Version 0.3.0 accidentally removed metrics support.
Make sure that custom configurations include the metrics pipeline and exporter.
The producer sends OTLP/HTTP metrics to `/v1/metrics` or uses OTLP/gRPC.
The `/status` response includes `committed_metric_points` in the store counters.

### The disposable database needs a manual reset

Logal normally resets its own stale/corrupt owned schema. If manual cleanup is
necessary, stop Logal first. Use the guarded reset helper; without `--confirm`
it only resolves the configured target and verifies that neither its files nor
its configured ports are active:

```bash
./scripts/reset-db
./scripts/reset-db --confirm
```

The helper honors `LOGAL_DB_PATH` and all three configurable listener
endpoints; pass the same overrides used to start Logal. It aborts if any
database/lock file is open or any configured TCP port is listening. The
destructive action requires `--confirm` and holds the same ownership lock as startup.
The helper uses `go run ./cmd/logal reset-db` for this operation.
Never use it against a path whose ownership or contents matter.

## License

MIT. See [`LICENSE`](LICENSE).
