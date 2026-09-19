package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/query"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestCollectorQueryCLI(t *testing.T) {
	c := startCollector(t, "")
	logs, traces, metrics := contractLogs(), contractTraces(), contractMetrics()
	traceID := pcommon.TraceID{1, 2, 3, 4}
	record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	record.SetTraceID(traceID)
	record.Attributes().PutStr("http.request.method", "GET")
	record.SetEventName("http.request.completed")
	now := time.Now().UTC().Truncate(time.Millisecond)
	record.SetTimestamp(pcommon.NewTimestampFromTime(now))
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	span.SetParentSpanID(pcommon.SpanID{9})
	span.SetKind(ptrace.SpanKindServer)
	span.Attributes().PutInt("http.response.status_code", 500)
	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("request rejected")
	exportSignals(t, c.grpcConnection(t), logs, traces, metrics)
	invoke := func(args ...string) query.Result {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, collectorBinary, args...)
		command.Env = append(os.Environ(), "LOGAL_DB_PATH="+c.dbPath)
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		if err := command.Run(); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, stderr.String())
		}
		var result query.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("invalid JSON: %s (%v)", stdout.String(), err)
		}
		if stderr.Len() != 0 {
			t.Fatalf("unexpected stderr: %s", stderr.String())
		}
		return result
	}
	result := invoke("logs", "--service", "grpc-contract", "--level", "info", "--json", "--payload")
	if result.Count != 1 || result.Rows[0]["service_name"] != "grpc-contract" || result.Rows[0]["payload"] == nil || result.Rows[0]["received_at"] == nil {
		t.Fatalf("%+v", result)
	}
	result = invoke("logs", "--service", "grpc-contract", "--level", "error", "--json")
	if result.Count != 0 {
		t.Fatalf("severity filter: %+v", result)
	}
	// Flags following the positional trace ID must work for agents.
	result = invoke("trace", "01020304000000000000000000000000", "--json")
	if result.Count != 2 {
		t.Fatalf("trace correlation: %+v", result)
	}
	if result.Rows[0]["duration_ms"] != float64(1000) || result.Rows[0]["offset_ms"] != float64(0) || result.Rows[1]["offset_ms"] != float64(1000) {
		t.Fatalf("trace timing: %+v", result)
	}
	if result.Rows[0]["kind"] != "span" || result.Rows[0]["parent_span_id"] != "0900000000000000" || result.Rows[0]["status_code"] != float64(2) || result.Rows[1]["time"] != now.Format(time.RFC3339Nano) {
		t.Fatalf("trace timeline: %+v", result)
	}
	if result.Rows[0]["span_kind"] != float64(2) || result.Rows[0]["status_message"] != "request rejected" {
		t.Fatalf("trace lost span diagnostics: %+v", result)
	}
	for _, column := range []string{"span_kind", "status_message"} {
		if _, exists := result.Rows[1][column]; exists {
			t.Fatalf("log inherited span field %s: %+v", column, result.Rows[1])
		}
	}
	kinds := map[any]bool{}
	for _, row := range result.Rows {
		if row["trace_id"] != "01020304000000000000000000000000" {
			t.Fatal(row)
		}
		kinds[row["kind"]] = true
	}
	if !kinds["span"] || !kinds["log"] {
		t.Fatal(kinds)
	}
	result = invoke("trace", "01020304000000000000000000000000", "--columns", "kind,span_kind,status_message", "--include-empty", "--json")
	if len(result.Columns) != 3 || result.Rows[0]["status_message"] != "request rejected" || result.Rows[0]["span_kind"] != float64(2) {
		t.Fatalf("trace diagnostic selection: %+v", result)
	}
	for _, column := range []string{"span_kind", "status_message"} {
		if value, exists := result.Rows[1][column]; !exists || value != nil {
			t.Fatalf("log span field should be null with include-empty: %+v", result.Rows[1])
		}
	}
	result = invoke("metrics", "--name", "grpc.contract.gauge", "--json")
	if result.Count != 1 || result.Rows[0]["number_int"] != float64(1) {
		t.Fatalf("%+v", result)
	}
	for _, column := range []string{"aggregate_count", "scope_version", "explicit_bounds"} {
		if _, ok := result.Rows[0][column]; ok {
			t.Fatalf("default metrics output contains empty %s", column)
		}
	}
	result = invoke("metrics", "points", "--name", "grpc.contract.gauge", "--json", "--include-empty")
	for _, column := range []string{"aggregate_count", "scope_version", "explicit_bounds"} {
		if _, ok := result.Rows[0][column]; !ok {
			t.Fatalf("include-empty omits %s", column)
		}
	}
	result = invoke("metrics", "points", "--name", "grpc.contract.gauge", "--columns", "metric_name,number_int", "--max-bytes", "1024", "--json")
	if result.Count != 1 || len(result.Columns) != 2 || len(result.Rows[0]) != 2 {
		t.Fatalf("column selection: %+v", result)
	}
	result = invoke("logs", "--service", "' OR 1=1 --", "--json")
	if result.Count != 0 {
		t.Fatalf("unbound filter: %+v", result)
	}
	result = invoke("logs", "--since", now.Add(time.Hour).Format(time.RFC3339Nano), "--json")
	if result.Count != 0 {
		t.Fatalf("since filter: %+v", result)
	}
	sqlFile := filepath.Join(t.TempDir(), "query.sql")
	if err := os.WriteFile(sqlFile, []byte("SELECT service_name FROM otel_logs ORDER BY id"), 0600); err != nil {
		t.Fatal(err)
	}
	result = invoke("sql", "--file", sqlFile, "--json")
	if result.Count != 1 {
		t.Fatalf("%+v", result)
	}
	result = invoke("services", "--service", "grpc-contract", "--json")
	if result.Count != 1 || result.Rows[0]["logs"] != float64(1) || result.Rows[0]["spans"] != float64(1) || result.Rows[0]["metric_points"] != float64(1) {
		t.Fatalf("service discovery: %+v", result)
	}
	result = invoke("spans", "--status", "error", "--kind", "server", "--min-duration", "500ms", "--attribute", "http.response.status_code=500", "--resource", "service.name=grpc-contract", "--json")
	if result.Count != 1 || result.Rows[0]["duration_ms"] != float64(1000) {
		t.Fatalf("span discovery: %+v", result)
	}
	result = invoke("logs", "--attribute", "http.request.method=GET", "--trace-id", "01020304000000000000000000000000", "--json")
	if result.Count != 1 || result.Rows[0]["event_name"] != "http.request.completed" {
		t.Fatalf("log OTel filters: %+v", result)
	}
	result = invoke("metrics", "--service", "grpc-contract", "list", "--json")
	if result.Count != 1 || result.Rows[0]["series_count"] != float64(1) {
		t.Fatalf("metric discovery with parent flags: %+v", result)
	}
	resourceID := result.Rows[0]["resource_id"].(string)
	result = invoke("metrics", "points", "--resource-id", resourceID, "--json")
	if result.Count != 1 || result.Rows[0]["resource_id"] != resourceID {
		t.Fatalf("metric resource selection: %+v", result)
	}
	result = invoke("metrics", "series", "--service", "grpc-contract", "--name", "grpc.contract.gauge", "--json")
	if result.Count != 1 || result.Rows[0]["metric_type"] != "gauge" {
		t.Fatalf("series discovery: %+v", result)
	}
	result = invoke("metrics", "rate", "--name", "grpc.contract.gauge", "--json")
	if result.Count != 1 || result.Rows[0]["rate_reason"] != "not_monotonic_sum" {
		t.Fatalf("gauge rate diagnostics: %+v", result)
	}
	c.post(t, "metrics", "application/json", bytes.NewReader(fixture(t, "metrics")), 200)
	result = invoke("metrics", "points", "--type", "histogram", "--json")
	if result.Count != 1 || len(result.Rows[0]["bucket_counts"].([]any)) != 3 || result.Rows[0]["temporality"] != "cumulative" {
		t.Fatalf("OTLP histogram projection: %+v", result)
	}
	result = invoke("metrics", "points", "--type", "exponential_histogram", "--json")
	if result.Count != 1 || result.Rows[0]["positive"] == nil || result.Rows[0]["negative"] == nil {
		t.Fatalf("OTLP exponential histogram: %+v", result)
	}
	// Output must also work after a clean collector shutdown.
	c.stop(t)
	result = invoke("sql", "--query", "SELECT count(*) AS n FROM otel_logs", "--json")
	if result.Count != 1 || result.Rows[0]["n"] != float64(1) {
		t.Fatalf("%+v", result)
	}
	for _, args := range [][]string{
		{"logs", "--json", "--db", filepath.Join(t.TempDir(), "missing.sqlite")},
		{"logs", "--json", "--limit", "invalid"},
		{"sql", "--json", "--query", "DELETE FROM otel_logs"},
		{"metrics", "rate", "--json"},
		{"metrics", "rate", "--json", "--name", "x", "--limit", "invalid"},
		{"metrics", "points", "--json", "--resource-id", "invalid"},
		{"spans", "--json", "--attribute", "invalid"},
	} {
		command := exec.Command(collectorBinary, args...)
		command.Env = append(os.Environ(), "LOGAL_DB_PATH="+c.dbPath)
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		if err := command.Run(); err == nil {
			t.Fatalf("expected failure: %v", args)
		}
		var envelope struct {
			Error struct{ Code, Message string }
		}
		if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil || envelope.Error.Code == "" || stdout.Len() != 0 {
			t.Fatalf("error output: stdout=%s stderr=%s err=%v", stdout.String(), stderr.String(), err)
		}
	}
}

func TestCLIHelpAndUsage(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"logs", "--help"}, {"trace", "--help"}, {"metrics", "--help"}, {"metrics", "rate", "--help"}, {"services", "--help"}, {"spans", "--help"}, {"sql", "--help"}, {"reset-db", "--help"}, {"serve", "--help"}} {
		var out, errOut bytes.Buffer
		if err := run(context.Background(), append([]string{"logal"}, args...), strings.NewReader(""), &out, &errOut); err != nil {
			t.Fatal(err)
		}
		if out.Len() == 0 {
			t.Fatalf("no help for %v", args)
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"logal", "reset-db", "--path", "unused"}, strings.NewReader(""), &out, &out); err == nil || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("reset safeguard: %v", err)
	}
}

func TestColumnHelpWithoutDatabase(t *testing.T) {
	for _, args := range [][]string{
		{"services"}, {"logs"}, {"logs", "--payload"}, {"spans"}, {"trace"}, {"trace", "--payload"},
		{"metrics"}, {"metrics", "list"}, {"metrics", "series"}, {"metrics", "points"}, {"metrics", "rate"},
	} {
		var out, errOut bytes.Buffer
		fullArgs := append([]string{"logal"}, args...)
		fullArgs = append(fullArgs, "--list-columns", "--json", "--db", filepath.Join(t.TempDir(), "missing.sqlite"))
		if err := run(context.Background(), fullArgs, strings.NewReader(""), &out, &errOut); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var result struct{ Columns []string }
		if err := json.Unmarshal(out.Bytes(), &result); err != nil || len(result.Columns) == 0 || errOut.Len() != 0 {
			t.Fatalf("%v: %s stderr=%s err=%v", args, out.String(), errOut.String(), err)
		}
	}
	var out, errOut bytes.Buffer
	if err := run(context.Background(), []string{"logal", "trace", "--list-columns"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"span_kind\n", "status_message\n", "offset_ms\n"} {
		if !strings.Contains(out.String(), field) {
			t.Fatalf("missing %q: %s", field, out.String())
		}
	}
}

func TestHumanDetailsPreserveFloatPrecision(t *testing.T) {
	var out bytes.Buffer
	result := query.Result{Columns: []string{"rate_per_second"}, Rows: []map[string]any{{"rate_per_second": 1.0000000000000002}}, Count: 1}
	if err := printHuman(&out, result, 1024, false, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "1.0000000000000002") {
		t.Fatal(out.String())
	}
}

func TestCompactHumanPresentation(t *testing.T) {
	var out bytes.Buffer
	result := query.Result{Columns: []string{"name", "kind", "status_code", "duration_ms"}, Rows: []map[string]any{{"name": strings.Repeat("x", 100), "kind": int64(2), "status_code": int64(2), "duration_ms": float64(10.5)}}, Count: 1}
	if err := printHuman(&out, result, 2048, true, true); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"server", "error", "10.5", "…", "--details"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q: %s", want, out.String())
		}
	}
	out.Reset()
	if err := printHuman(&out, result, 2048, false, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), strings.Repeat("x", 100)) {
		t.Fatal("details shortened value")
	}
}

func TestHumanOutputLeavesOmittedCellsBlank(t *testing.T) {
	var out bytes.Buffer
	result := query.Result{Columns: []string{"n", "optional"}, Rows: []map[string]any{{"n": 0}, {"n": 1, "optional": false}}, Count: 2}
	if err := printResult(&out, result, false, 1024); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "null") || !strings.Contains(out.String(), "0\t\n") || !strings.Contains(out.String(), "1\tfalse") {
		t.Fatal(out.String())
	}
}

func TestHumanOutputEscapesTelemetry(t *testing.T) {
	var out bytes.Buffer
	result := query.Result{Columns: []string{"message"}, Rows: []map[string]any{{"message": "\x1b[31munsafe\nnext"}}, Count: 1}
	if err := printResult(&out, result, false, 1024); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(out.String(), '\x1b') || !strings.Contains(out.String(), `\nnext`) {
		t.Fatalf("unescaped output: %q", out.String())
	}
}
