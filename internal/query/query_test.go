package query

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func TestReadBoundsAndIsolation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "database with ? and #.sqlite")
	writer, err := store.NewFactory().Create(ctx, extension.Settings{ID: component.NewID(store.Type)}, &store.Config{Path: path, RetentionHours: 48})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Shutdown(ctx) })
	options := Options{Path: path, Limit: 2, Timeout: time.Second, MaxBytes: 1024}
	read := func(sql string) (Result, error) { return Read(ctx, options, sql, nil, false) }
	t.Run("column catalog matches SQL even without telemetry", func(t *testing.T) {
		for _, command := range []string{"services", "logs", "spans", "trace", "metrics"} {
			views := []string{"points"}
			if command == "metrics" {
				views = []string{"points", "list", "series", "rate"}
			}
			for _, view := range views {
				for _, payload := range []bool{false, true} {
					if payload && (command == "services" || view == "list" || view == "series") {
						continue
					}
					f := Filters{Since: "1m", Name: "test", Payload: payload}
					var statement string
					var args []any
					var err error
					switch command {
					case "services":
						statement, args, err = Services(f, time.Now())
					case "logs":
						statement, args, err = Logs(f, time.Now())
					case "spans":
						statement, args, err = Spans(f, time.Now())
					case "trace":
						statement, args, err = Trace(strings.Repeat("1", 32), payload)
					case "metrics":
						statement, args, err = MetricQuery(f, time.Now(), view)
					}
					if err != nil {
						t.Fatal(err)
					}
					o := options
					o.IncludeEmpty, o.MaxBytes = true, 1<<20
					result, err := Read(ctx, o, statement, args, true)
					if err != nil {
						t.Fatal(err)
					}
					want, err := OutputColumns(command, view, payload)
					if err != nil || !reflect.DeepEqual(result.Columns, want) {
						t.Fatalf("%s/%s payload=%v: actual=%v catalog=%v err=%v", command, view, payload, result.Columns, want, err)
					}
				}
			}
		}
	})
	t.Run("SQL column discovery does not evaluate rows", func(t *testing.T) {
		o := options
		o.ColumnsOnly = true
		result, err := Read(ctx, o, `SELECT abs(-9223372036854775808) AS would_overflow, NULL AS empty`, nil, false)
		if err != nil || !reflect.DeepEqual(result.Columns, []string{"would_overflow", "empty"}) || result.Count != 0 {
			t.Fatalf("%+v %v", result, err)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		sql := `WITH vals(x) AS (VALUES(1),(2),(3)) SELECT x FROM vals ORDER BY x`
		result, err := read(sql)
		if err != nil {
			t.Fatal(err)
		}
		if result.Count != 2 || !result.Truncated || result.Reason != "row_limit" || result.NextOffset == nil || *result.NextOffset != 2 {
			t.Fatalf("%+v", result)
		}
		next := options
		next.Offset = *result.NextOffset
		result, err = Read(ctx, next, sql, nil, false)
		if err != nil || result.Count != 1 || result.Truncated || result.Rows[0]["x"] != int64(3) {
			t.Fatalf("%+v %v", result, err)
		}
	})
	t.Run("quoted separators and blobs", func(t *testing.T) {
		result, err := read("SELECT '; -- /*' AS text, x'000aff' AS blob; ")
		if err != nil || result.Rows[0]["blob"] != "000aff" || result.Rows[0]["text"] != "; -- /*" {
			t.Fatalf("%+v %v", result, err)
		}
	})
	t.Run("empty result", func(t *testing.T) {
		result, err := read("SELECT 1 AS n WHERE 0")
		if err != nil || result.Count != 0 || result.Rows == nil || result.Truncated {
			t.Fatalf("%+v %v", result, err)
		}
	})
	t.Run("byte budget", func(t *testing.T) {
		result, err := read(`WITH vals(x) AS (VALUES(1),(2),(3)) SELECT replace(hex(zeroblob(250)),'0','x') AS text FROM vals`)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(result)
		if !result.Truncated || result.Reason != "byte_limit" || result.Count != 1 || len(encoded)+1 > options.MaxBytes {
			t.Fatalf("%+v (%d bytes)", result, len(encoded))
		}
		if _, err = read(`SELECT hex(zeroblob(1024)) AS text`); err == nil {
			t.Fatal("oversized first row accepted")
		}
	})
	t.Run("empty fields and meaningful zero values", func(t *testing.T) {
		statement := `SELECT NULL AS absent,'' AS empty,0 AS zero,0 AS monotonic,'[ ]' AS attributes,'{}' AS body,'{"empty":"","false":false,"zero":0}' AS payload`
		result, err := Read(ctx, options, statement, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		row := result.Rows[0]
		for _, name := range []string{"absent", "empty", "attributes", "body"} {
			if _, ok := row[name]; ok {
				t.Errorf("empty field %s present", name)
			}
		}
		if row["zero"] != int64(0) || row["monotonic"] != false || row["payload"] == nil {
			t.Fatal(row)
		}
		if len(result.Columns) != 3 {
			t.Fatal(result.Columns)
		}
		full := options
		full.IncludeEmpty = true
		result, err = Read(ctx, full, statement, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows[0]) != 7 || len(result.Columns) != 7 {
			t.Fatal(result)
		}
		for _, name := range []string{"absent", "empty", "attributes", "body"} {
			if _, ok := result.Rows[0][name]; !ok {
				t.Errorf("include-empty omitted %s", name)
			}
		}
	})
	t.Run("columns describe sparse page in query order", func(t *testing.T) {
		result, err := read(`SELECT NULL AS mixed,1 AS n UNION ALL SELECT 'present',2`)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Columns) != 2 || result.Columns[0] != "mixed" || result.Columns[1] != "n" {
			t.Fatal(result.Columns)
		}
		if _, ok := result.Rows[0]["mixed"]; ok {
			t.Fatal("missing value emitted")
		}
		if result.Rows[1]["mixed"] != "present" {
			t.Fatal(result)
		}
	})
	t.Run("empty columns do not consume output budget", func(t *testing.T) {
		statement := `SELECT NULL AS "` + strings.Repeat("x", 1500) + `"`
		result, err := read(statement)
		if err != nil || result.Count != 1 || len(result.Columns) != 0 || len(result.Rows[0]) != 0 {
			t.Fatalf("%+v %v", result, err)
		}
		full := options
		full.IncludeEmpty = true
		if _, err := Read(ctx, full, statement, nil, false); err == nil {
			t.Fatal("included empty column exceeded budget without error")
		}
	})
	t.Run("column selection before byte budgeting", func(t *testing.T) {
		selected := options
		selected.Columns = []string{"second", "first"}
		result, err := Read(ctx, selected, `SELECT 1 AS first,2 AS second,hex(zeroblob(2000)) AS huge`, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Columns) != 2 || result.Columns[0] != "second" || len(result.Rows[0]) != 2 {
			t.Fatal(result)
		}
		selected.Columns = []string{"missing"}
		if _, err := Read(ctx, selected, `SELECT 1 AS first`, nil, false); err == nil {
			t.Fatal("unknown column accepted")
		}
		selected.Columns = []string{"first", "first"}
		if _, err := Read(ctx, selected, `SELECT 1 AS first`, nil, false); err == nil {
			t.Fatal("duplicate requested column accepted")
		}
	})
	t.Run("writes and stacked statements", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "escaped.sqlite")
		for _, sql := range []string{
			"DELETE FROM otel_logs", "UPDATE otel_logs SET service_name='oops'", "DROP TABLE otel_logs", "PRAGMA query_only=OFF", "VACUUM",
			"SELECT 1; DELETE FROM otel_logs", "SELECT 1); ATTACH DATABASE '" + target + "' AS extra; SELECT (1",
			"ATTACH DATABASE '" + target + "' AS extra", "SELECT load_extension('missing')", "SELECT 1 /* unterminated",
		} {
			if _, err := read(sql); err == nil {
				t.Errorf("accepted %s", sql)
			}
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("ATTACH created file: %v", err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		bounded := options
		bounded.Timeout = 10 * time.Millisecond
		start := time.Now()
		_, err := Read(ctx, bounded, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n`, nil, false)
		if err == nil || time.Since(start) > time.Second {
			t.Fatalf("deadline: %v, duration %s", err, time.Since(start))
		}
		if _, err := read("SELECT 1"); err != nil {
			t.Fatalf("subsequent query: %v", err)
		}
	})
	if err := writer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read("SELECT count(*) FROM otel_logs"); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read changed database bytes")
	}
	// A finished reader must not prevent the collector from acquiring ownership.
	reopened, err := store.NewFactory().Create(ctx, extension.Settings{ID: component.NewID(store.Type)}, &store.Config{Path: path, RetentionHours: 48})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFiltersAndValidation(t *testing.T) {
	for _, input := range []string{"0s", "-1m", "nonsense", "2500-01-01T00:00:00Z"} {
		if _, err := cutoff(input, time.Now()); err == nil {
			t.Errorf("accepted since %q", input)
		}
	}
	for _, input := range []string{"", "bad", strings.Repeat("0", 32), strings.Repeat("g", 32)} {
		if _, _, err := Trace(input, false); err == nil {
			t.Errorf("accepted trace %q", input)
		}
	}
	if _, _, err := Logs(Filters{Since: "1m", Level: "oops"}, time.Now()); err == nil {
		t.Fatal("invalid severity accepted")
	}
	valid := Options{Path: "db", Limit: 100, Timeout: time.Second, MaxBytes: 1024}
	for _, change := range []func(*Options){func(o *Options) { o.Limit = 0 }, func(o *Options) { o.Limit = 10001 }, func(o *Options) { o.Offset = -1 }, func(o *Options) { o.Timeout = 31 * time.Second }, func(o *Options) { o.MaxBytes = 0 }} {
		bad := valid
		change(&bad)
		if bad.Validate() == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}
