package exporter

import (
	"bytes"
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func startExporterStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	factory := store.NewFactory()
	cfg := factory.CreateDefaultConfig().(*store.Config)
	cfg.Path = filepath.Join(t.TempDir(), "telemetry.sqlite")
	instance, err := factory.Create(context.Background(), extension.Settings{ID: component.NewID(store.Type)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := instance.(*store.Store)
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s, cfg.Path
}

func TestLogFingerprintCanonicalizesMapsWithoutMutatingInput(t *testing.T) {
	data := plog.NewLogs()
	resource := data.ResourceLogs().AppendEmpty()
	scope := resource.ScopeLogs().AppendEmpty()
	log := scope.LogRecords().AppendEmpty()
	resource.Resource().Attributes().PutStr("z", "last")
	resource.Resource().Attributes().PutStr("a", "first")
	log.Body().SetEmptyMap().PutStr("z", "last")
	log.Body().Map().PutStr("a", "first")
	before, err := (&plog.JSONMarshaler{}).MarshalLogs(data)
	if err != nil {
		t.Fatal(err)
	}
	first, err := marshalSingleLog(resource, scope, log)
	if err != nil {
		t.Fatal(err)
	}
	after, err := (&plog.JSONMarshaler{}).MarshalLogs(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("caller data mutated")
	}
	resource.Resource().Attributes().Clear()
	resource.Resource().Attributes().PutStr("a", "first")
	resource.Resource().Attributes().PutStr("z", "last")
	log.Body().Map().Clear()
	log.Body().Map().PutStr("a", "first")
	log.Body().Map().PutStr("z", "last")
	second, err := marshalSingleLog(resource, scope, log)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("attribute order changed fingerprint:\n%s\n%s", first, second)
	}
}

func TestSpanRetryWithReorderedAttributesAndConflictAtomicity(t *testing.T) {
	s, path := startExporterStore(t)
	e := &tracesExporter{store: s}
	data := ptrace.NewTraces()
	resource := data.ResourceSpans().AppendEmpty()
	scope := resource.ScopeSpans().AppendEmpty()
	span := scope.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{1})
	span.SetName("same")
	span.Attributes().PutStr("z", "last")
	span.Attributes().PutStr("a", "first")
	if err := e.ConsumeTraces(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	span.Attributes().Clear()
	span.Attributes().PutStr("a", "first")
	span.Attributes().PutStr("z", "last")
	if err := e.ConsumeTraces(context.Background(), data); err != nil {
		t.Fatalf("equivalent retry rejected: %v", err)
	}
	batch := ptrace.NewTraces()
	batchScope := batch.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	first := batchScope.Spans().AppendEmpty()
	span.CopyTo(first)
	first.SetSpanID(pcommon.SpanID{2})
	conflicting := batchScope.Spans().AppendEmpty()
	span.CopyTo(conflicting)
	conflicting.SetName("changed")
	if err := e.ConsumeTraces(context.Background(), batch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("conflict error=%v", err)
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM otel_spans`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("conflicting batch partially committed: %d", count)
	}
}

func TestProjectedAttributesCannotLeakNestedSecrets(t *testing.T) {
	s, path := startExporterStore(t)
	data := plog.NewLogs()
	resource := data.ResourceLogs().AppendEmpty()
	resource.Resource().Attributes().PutEmptyMap("service.name").PutStr("password", "resource-secret")
	log := resource.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	log.Attributes().PutEmptyMap("request.id").PutStr("authorization", "projection-secret")
	log.Body().SetEmptyMap().PutStr("password", "body-secret")
	before, err := (&plog.JSONMarshaler{}).MarshalLogs(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&logsExporter{store: s}).ConsumeLogs(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	after, err := (&plog.JSONMarshaler{}).MarshalLogs(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("exporter mutated input")
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stored string
	if err := db.QueryRow(`SELECT service_name || COALESCE(request_id,'') || body_json || payload_json FROM otel_logs`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, "-secret") || !strings.Contains(stored, "[REDACTED]") {
		t.Fatalf("redaction failed: %s", stored)
	}
}

func TestExporterRejectsTimestampOverflowAndCancellation(t *testing.T) {
	data := plog.NewLogs()
	log := data.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	log.SetTimestamp(pcommon.Timestamp(math.MaxUint64))
	e := &logsExporter{}
	if err := e.ConsumeLogs(context.Background(), data); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("overflow error=%v", err)
	}
	log.SetTimestamp(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.ConsumeLogs(ctx, data); status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation error=%v", err)
	}
}

func TestAttributeKeySizeLimit(t *testing.T) {
	attributes := pcommon.NewMap()
	attributes.PutStr(strings.Repeat("x", (1<<20)+1), "value")
	if err := validateMap(attributes); err == nil {
		t.Fatal("oversized key accepted")
	}
}

func TestCanonicalPayloadPreservesArraysAndIntegerPrecision(t *testing.T) {
	payload := []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"18446744073709551615","body":{"arrayValue":{"values":[{"intValue":"9007199254740993"},{"intValue":"1"}]}}}]}]}]}`)
	canonical, err := canonicalPayload(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(canonical)
	if err != nil {
		t.Fatal(err)
	}
	record := decoded.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if uint64(record.Timestamp()) != math.MaxUint64 || record.Body().Slice().At(0).Int() != 9007199254740993 || record.Body().Slice().At(1).Int() != 1 {
		t.Fatalf("canonicalization changed values: %s", canonical)
	}
}

func TestRedactionRecognizesCredentialSpellings(t *testing.T) {
	attributes := pcommon.NewMap()
	sensitive := []string{"apiKey", "accessToken", "clientSecret", "http.request.header.proxy-authorization", "db_password", "auth.client_secret", "token"}
	safe := []string{"gen_ai.usage.input_tokens", "output_token_count", "tokenizer", "token_count"}
	for _, key := range sensitive {
		attributes.PutStr(key, "must-not-persist")
	}
	for _, key := range safe {
		attributes.PutInt(key, 42)
	}
	redactMap(attributes)
	for _, key := range sensitive {
		value, _ := attributes.Get(key)
		if value.Str() != "[REDACTED]" {
			t.Errorf("credential %s persisted", key)
		}
	}
	for _, key := range safe {
		value, _ := attributes.Get(key)
		if value.Type() != pcommon.ValueTypeInt || value.Int() != 42 {
			t.Errorf("token count %s redacted", key)
		}
	}
}
