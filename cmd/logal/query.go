package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/CaliLuke/autok-logal/internal/query"
	"github.com/urfave/cli/v3"
)

func newCLI(in io.Reader, out, errOut io.Writer) *cli.Command {
	return &cli.Command{
		Name: "logal", Usage: "Local OTLP collector and OpenTelemetry inspection", Version: version,
		Reader: in, Writer: out, ErrWriter: errOut,
		OnUsageError:   func(_ context.Context, _ *cli.Command, err error, _ bool) error { return err },
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
		Commands: []*cli.Command{
			queryCommand("services", "Discover services exporting OTLP logs, spans, and metrics"),
			queryCommand("spans", "Find OTLP spans by status, kind, duration, and attributes"),
			queryCommand("logs", "Read recent logs, newest first"),
			queryCommand("trace", "Read spans and logs for a trace in event-time order"),
			queryCommand("metrics", "Read recent metric points, newest first"),
			queryCommand("sql", "Run one read-only SELECT or WITH query"),
			{Name: "serve", Usage: "Start the collector; use logal serve --help for collector flags", SkipFlagParsing: true,
				Action: func(ctx context.Context, c *cli.Command) error {
					return runCollector(ctx, c.Args().Slice(), out, errOut)
				}},
			newResetCommand(),
			newClearCommand(),
		},
	}
}

func queryCommand(name, usage string) *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{Name: "db", Usage: "Existing Logal database path", Sources: cli.EnvVars("LOGAL_DB_PATH")},
		&cli.BoolFlag{Name: "json", Usage: "Emit JSON results and errors"},
		&cli.StringFlag{Name: "columns", Usage: "Comma-separated output columns, in display order (see --list-columns)"},
		&cli.BoolFlag{Name: "list-columns", Usage: "List available output columns without reading rows (built-in views need no database)"},
		&cli.BoolFlag{Name: "details", Usage: "Show all fields without shortening cells (JSON already includes all fields)"},
		&cli.BoolFlag{Name: "include-empty", Usage: "Include nulls, empty strings, and empty arrays or objects in output"},
		&cli.IntFlag{Name: "limit", Value: 100, Usage: "Maximum output rows (1–10000)"},
		&cli.IntFlag{Name: "offset", Usage: "Output rows to skip (0–1000000)"},
		&cli.IntFlag{Name: "max-bytes", Value: 1 << 20, Usage: "Maximum output bytes (1024–16777216)"},
		&cli.DurationFlag{Name: "timeout", Value: 2 * time.Second, Usage: "Query deadline (at most 30s)"},
	}
	if name != "sql" && name != "services" {
		usage := "Include the stored, redacted OTLP JSON envelope"
		if name == "metrics" {
			usage += " (points and rates only)"
		}
		flags = append(flags, &cli.BoolFlag{Name: "payload", Usage: usage})
	}
	if name == "logs" || name == "spans" || name == "metrics" || name == "services" {
		flags = append(flags,
			&cli.StringFlag{Name: "service", Usage: "Exact resource service.name"},
			&cli.StringFlag{Name: "scope", Usage: "Exact instrumentation scope name"},
			&cli.StringFlag{Name: "scope-version", Usage: "Exact instrumentation scope version"},
			&cli.StringSliceFlag{Name: "resource", Usage: "Resource attribute KEY=VALUE (repeatable, all must match)"},
			&cli.StringFlag{Name: "since", Value: "15m", Usage: "Inclusive cutoff: positive duration or RFC3339 timestamp"},
			&cli.StringFlag{Name: "until", Usage: "Exclusive cutoff: RFC3339 timestamp"},
			&cli.StringFlag{Name: "time", Value: "received", Usage: "Filter timestamps by received or event time"},
		)
		if name != "services" {
			flags = append(flags, &cli.StringSliceFlag{Name: "attribute", Usage: "Record or data-point attribute KEY=VALUE (repeatable). JSON scalars preserve types"})
		}
	}
	if name == "logs" || name == "spans" {
		flags = append(flags, &cli.StringFlag{Name: "trace-id", Usage: "Exact 32-digit hexadecimal trace ID"}, &cli.StringFlag{Name: "span-id", Usage: "Exact 16-digit hexadecimal span ID"})
	}
	switch name {
	case "logs":
		flags = append(flags, &cli.StringFlag{Name: "level", Usage: "Minimum OTLP severity: trace, debug, info, warn, error, fatal"})
	case "spans":
		flags = append(flags,
			&cli.StringFlag{Name: "name", Usage: "Exact span name"},
			&cli.StringFlag{Name: "kind", Usage: "OTLP kind: unspecified, internal, server, client, producer, consumer"},
			&cli.StringFlag{Name: "status", Usage: "OTLP status: unset, ok, error"},
			&cli.StringFlag{Name: "min-duration", Usage: "Minimum span duration, such as 100ms"},
		)
	case "metrics":
		flags = append(flags,
			&cli.StringFlag{Name: "resource-id", Usage: "Exact resource_id from metric output, to select one resource"},
			&cli.StringFlag{Name: "name", Usage: "Exact OTLP metric name (required for rate)"},
			&cli.StringFlag{Name: "type", Usage: "OTLP type: gauge, sum, histogram, exponential_histogram, summary"},
			&cli.StringFlag{Name: "temporality", Usage: "Aggregation temporality: delta, cumulative, unspecified"},
		)
	case "sql":
		flags = append(flags, &cli.StringFlag{Name: "query", Usage: "SQL SELECT or WITH query"}, &cli.StringFlag{Name: "file", Usage: "SQL file, or - for stdin (at most 64 KiB)"})
	}
	command := &cli.Command{Name: name, Usage: usage, Flags: flags, DisableSliceFlagSeparator: true,
		OnUsageError: queryUsageError, Action: queryAction(name, "points")}
	if name == "trace" {
		command.ArgsUsage = "TRACE_ID"
	}
	if name == "metrics" {
		command.Description = "Inspect OTLP metric points by default. Discovery preserves resource, scope, unit, type, temporality, and point attributes. Rates use original OTLP timestamps."
		for _, view := range []struct{ name, usage string }{
			{"points", "Inspect points, units, attributes, histogram buckets, flags, and exemplars"},
			{"list", "Discover metric descriptors and series counts"},
			{"series", "Discover distinct OTLP streams and their point counts"},
			{"rate", "Calculate per-second rates for monotonic sums, with reset and gap diagnostics"},
		} {
			command.Commands = append(command.Commands, &cli.Command{Name: view.name, Usage: view.usage, DisableSliceFlagSeparator: true, OnUsageError: queryUsageError, Action: queryAction(name, view.name)})
		}
	}
	return command
}

func queryUsageError(_ context.Context, _ *cli.Command, err error, _ bool) error { return err }

func queryAction(name, view string) cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		if c.Bool("list-columns") && name != "sql" {
			columns, err := query.OutputColumns(name, view, c.Bool("payload"))
			if err != nil {
				return err
			}
			return printColumns(c.Root().Writer, columns, c.Bool("json"))
		}
		options := query.Options{ColumnsOnly: c.Bool("list-columns"), IncludeEmpty: c.Bool("include-empty"), Path: c.String("db"), Limit: c.Int("limit"), Offset: c.Int("offset"), MaxBytes: c.Int("max-bytes"), Timeout: c.Duration("timeout")}
		if err := options.Validate(); err != nil {
			return err
		}
		if name != "trace" && c.Args().Len() != 0 {
			return fmt.Errorf("%s does not accept positional arguments", name)
		}
		filters := query.Filters{
			Service: c.String("service"), Scope: c.String("scope"), ScopeVersion: c.String("scope-version"),
			Resource: c.StringSlice("resource"), Attribute: c.StringSlice("attribute"),
			Since: c.String("since"), Until: c.String("until"), TimeBasis: c.String("time"),
			Level: c.String("level"), Name: c.String("name"), Payload: c.Bool("payload"),
			TraceID: c.String("trace-id"), SpanID: c.String("span-id"), Kind: c.String("kind"), Status: c.String("status"), MinDuration: c.String("min-duration"),
			MetricType: c.String("type"), Temporality: c.String("temporality"), ResourceID: c.String("resource-id"),
		}
		var statement string
		var args []any
		var err error
		switch name {
		case "logs":
			statement, args, err = query.Logs(filters, time.Now())
		case "spans":
			statement, args, err = query.Spans(filters, time.Now())
		case "services":
			statement, args, err = query.Services(filters, time.Now())
		case "metrics":
			statement, args, err = query.MetricQuery(filters, time.Now(), view)
		case "trace":
			if c.Args().Len() != 1 {
				return fmt.Errorf("trace requires exactly one trace ID")
			}
			statement, args, err = query.Trace(c.Args().First(), c.Bool("payload"))
		case "sql":
			statement, err = readSQL(c)
		}
		if err != nil {
			return err
		}
		if c.IsSet("columns") {
			options.Columns = strings.Split(c.String("columns"), ",")
			for i, column := range options.Columns {
				options.Columns[i] = strings.TrimSpace(column)
				if options.Columns[i] == "" {
					return fmt.Errorf("--columns requires nonempty comma-separated column names")
				}
			}
		} else if !c.Bool("json") && !c.Bool("details") && !c.Bool("payload") {
			options.Columns = summaryColumns(name, view)
		}
		result, err := query.Read(ctx, options, statement, args, name != "sql")
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("query exceeded --timeout %s; narrow the query or increase --timeout: %w", options.Timeout, err)
			}
			return err
		}
		if c.Bool("json") {
			if options.ColumnsOnly {
				return printColumns(c.Root().Writer, result.Columns, true)
			}
			return printResult(c.Root().Writer, result, true, options.MaxBytes)
		}
		if options.ColumnsOnly {
			return printColumns(c.Root().Writer, result.Columns, false)
		}
		return printHuman(c.Root().Writer, result, options.MaxBytes, !c.Bool("details") && !c.Bool("payload"), name != "sql")
	}
}

func printColumns(out io.Writer, columns []string, jsonOutput bool) error {
	if jsonOutput {
		return json.NewEncoder(out).Encode(struct {
			Columns []string `json:"columns"`
		}{columns})
	}
	_, err := fmt.Fprintln(out, strings.Join(columns, "\n"))
	return err
}

func readSQL(c *cli.Command) (string, error) {
	if c.IsSet("query") == c.IsSet("file") {
		return "", fmt.Errorf("sql requires exactly one of --query or --file")
	}
	if c.IsSet("query") {
		return c.String("query"), nil
	}
	var reader io.Reader = c.Root().Reader
	if c.String("file") != "-" {
		file, err := os.Open(c.String("file"))
		if err != nil {
			return "", err
		}
		defer file.Close()
		reader = file
	}
	data, err := io.ReadAll(io.LimitReader(reader, query.MaxSQLBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > query.MaxSQLBytes {
		return "", fmt.Errorf("SQL file exceeds 64 KiB")
	}
	return string(data), nil
}

func printResult(out io.Writer, result query.Result, jsonOutput bool, maxBytes int) error {
	if !jsonOutput {
		return printHuman(out, result, maxBytes, false, true)
	}
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(result); err != nil {
		return err
	}
	if buffer.Len() > maxBytes {
		return fmt.Errorf("formatted output exceeds --max-bytes; reduce --limit or --columns, or increase --max-bytes")
	}
	_, err := out.Write(buffer.Bytes())
	return err
}
