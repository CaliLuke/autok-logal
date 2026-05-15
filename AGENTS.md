# autok-logal

Local Auto-K logging collector. Keep implementation here rather than copying logger internals into the app repos.

## Scope

- Fluent Bit owns ingestion, buffering, retry, and backpressure.
- The `autok_sqlite` Fluent Bit output plugin is the only process that writes `otel.debug.sqlite`.
- Auto-K apps should forward logs/traces to Fluent Bit ports, not open the SQLite database directly.

## Validation

Run these before considering logger changes complete:

```bash
./scripts/build-plugin
./scripts/run --dry-run
```

If Fluent Bit is installed locally, also run:

```bash
./scripts/run
curl -s http://127.0.0.1:2020/api/v1/health
```

Do not add generated binaries under `dist/` to git.
