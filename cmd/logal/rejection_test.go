package main

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Send invalid protobuf through the real transport rather than constructing an
// invalid pdata object that the client itself could refuse to encode.
type malformedCodec struct{}

func (malformedCodec) Name() string                { return "proto" }
func (malformedCodec) Marshal(any) ([]byte, error) { return []byte{0xff}, nil }
func (malformedCodec) Unmarshal([]byte, any) error { return nil }

func TestCollectorRejectsBadRequests(t *testing.T) {
	c := startCollector(t, "")
	connection := c.grpcConnection(t)
	oversized := strings.Repeat(" ", 4<<20) + "{}"
	for _, signal := range []struct{ name, method string }{
		{"logs", "/opentelemetry.proto.collector.logs.v1.LogsService/Export"},
		{"traces", "/opentelemetry.proto.collector.trace.v1.TraceService/Export"},
		{"metrics", "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"},
	} {
		t.Run(signal.name, func(t *testing.T) {
			c.post(t, signal.name, "application/json", strings.NewReader(`{"broken":`), 400)
			c.post(t, signal.name, "application/x-protobuf", bytes.NewReader([]byte{0xff}), 400)
			c.post(t, signal.name, "text/plain", strings.NewReader("{}"), 415)
			body := c.post(t, signal.name, "application/json", strings.NewReader(oversized), 400)
			if !bytes.Contains(body, []byte("too large")) {
				t.Fatalf("body limit was not the rejection cause: %s", body)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := connection.Invoke(ctx, signal.method, new(struct{}), new(struct{}), grpc.ForceCodec(malformedCodec{}))
			if status.Code(err) != codes.Internal {
				t.Fatalf("malformed protobuf: %v", err)
			}
		})
	}
	assertSignalCounts(t, c, 0, 0, 0)

	large := strings.Repeat("x", (4<<20)+1)
	logs, traces, metrics := contractLogs(), contractTraces(), contractMetrics()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(large)
	traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetName(large)
	metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetDescription(large)
	for name, send := range map[string]func(context.Context) error{
		"logs": func(ctx context.Context) error {
			_, err := plogotlp.NewGRPCClient(connection).Export(ctx, plogotlp.NewExportRequestFromLogs(logs))
			return err
		},
		"traces": func(ctx context.Context) error {
			_, err := ptraceotlp.NewGRPCClient(connection).Export(ctx, ptraceotlp.NewExportRequestFromTraces(traces))
			return err
		},
		"metrics": func(ctx context.Context) error {
			_, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(metrics))
			return err
		},
	} {
		t.Run(name+"_grpc_size_limit", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := send(ctx); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("oversized gRPC: %v", err)
			}
		})
	}
	assertSignalCounts(t, c, 0, 0, 0)
	// Every validation batch contains a valid record before the invalid one.
	// Neither protocol may partially persist it or turn a producer error into 503.
	invalidLogs := contractLogs()
	invalidLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().SetTimestamp(pcommon.Timestamp(math.MaxUint64))
	invalidTraces := contractTraces()
	invalidTraces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().AppendEmpty().SetName("missing identity")
	invalidMetrics := contractMetrics()
	invalidMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty().SetName("missing type")
	logJSON, err := (&plog.JSONMarshaler{}).MarshalLogs(invalidLogs)
	if err != nil {
		t.Fatal(err)
	}
	traceJSON, err := (&ptrace.JSONMarshaler{}).MarshalTraces(invalidTraces)
	if err != nil {
		t.Fatal(err)
	}
	metricJSON, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(invalidMetrics)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		payload []byte
		send    func(context.Context) error
	}{
		{"logs", logJSON, func(ctx context.Context) error {
			_, err := plogotlp.NewGRPCClient(connection).Export(ctx, plogotlp.NewExportRequestFromLogs(invalidLogs))
			return err
		}},
		{"traces", traceJSON, func(ctx context.Context) error {
			_, err := ptraceotlp.NewGRPCClient(connection).Export(ctx, ptraceotlp.NewExportRequestFromTraces(invalidTraces))
			return err
		}},
		{"metrics", metricJSON, func(ctx context.Context) error {
			_, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(invalidMetrics))
			return err
		}},
	} {
		t.Run(test.name+"_atomic_validation", func(t *testing.T) {
			c.post(t, test.name, "application/json", bytes.NewReader(test.payload), 400)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := test.send(ctx); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid batch: %v", err)
			}
			assertSignalCounts(t, c, 0, 0, 0)
		})
	}
	exportSignals(t, connection, contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, c, 1, 1, 2)
	// A conflict discovered inside the transaction must roll back earlier inserts.
	conflict := contractTraces()
	spans := conflict.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	spans.At(0).CopyTo(spans.AppendEmpty())
	spans.At(0).SetSpanID(pcommon.SpanID{99})
	spans.At(1).SetName("conflicting content")
	payload, err := (&ptrace.JSONMarshaler{}).MarshalTraces(conflict)
	if err != nil {
		t.Fatal(err)
	}
	c.post(t, "traces", "application/json", bytes.NewReader(payload), 400)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := ptraceotlp.NewGRPCClient(connection).Export(ctx, ptraceotlp.NewExportRequestFromTraces(conflict)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("span conflict: %v", err)
	}
	assertSignalCounts(t, c, 1, 1, 2)
}
