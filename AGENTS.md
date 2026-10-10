# autok-logal

Local Auto-K OpenTelemetry collector. Keep persistence and lifecycle ownership here rather than copying SQLite writers into app repos.

## Scope

- Logal is the only process that opens `otel.debug.sqlite` read-write.
- The `stack` TUI runs Logal with `LOGAL_DB_PATH=.tmp/dev-stack/logal-<OTLP HTTP port>/otel.debug.sqlite` (blacknas: `.tmp/blacknas-stack/`), relative to the monorepo root. A standalone `./scripts/run` defaults to `<monorepo>/otel.debug.sqlite`.
- Auto-K apps export signal-specific OTLP logs/traces/metrics and never open the database.
- The database is disposable: reset stale/corrupt schemas instead of adding compatibility migrations.
- Metrics are supported through the separate `otel_metric_points` schema. Preserve all three signal pipelines.

## Validation

Run these before considering logger changes complete:

```bash
go test ./...
go build ./cmd/logal
./scripts/run --dry-run
```

For live verification, also run:

```bash
./scripts/run
curl -fsS http://127.0.0.1:13133/readyz
```

Do not add generated binaries under `dist/` to git.
