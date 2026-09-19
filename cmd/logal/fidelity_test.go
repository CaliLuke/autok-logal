package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	contents, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func (c *collector) grpcConnection(t *testing.T) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient(c.grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func exportSignals(t *testing.T, connection *grpc.ClientConn, logs plog.Logs, traces ptrace.Traces, metrics pmetric.Metrics) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if response, err := plogotlp.NewGRPCClient(connection).Export(ctx, plogotlp.NewExportRequestFromLogs(logs)); err != nil {
		t.Fatal(err)
	} else if response.PartialSuccess().RejectedLogRecords() != 0 {
		t.Fatal("logs partially rejected")
	}
	if response, err := ptraceotlp.NewGRPCClient(connection).Export(ctx, ptraceotlp.NewExportRequestFromTraces(traces)); err != nil {
		t.Fatal(err)
	} else if response.PartialSuccess().RejectedSpans() != 0 {
		t.Fatal("spans partially rejected")
	}
	if response, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(metrics)); err != nil {
		t.Fatal(err)
	} else if response.PartialSuccess().RejectedDataPoints() != 0 {
		t.Fatal("metrics partially rejected")
	}
}

func TestCollectorFidelity(t *testing.T) {
	c := startCollector(t, "")
	for range 2 {
		c.post(t, "logs", "application/json", bytes.NewReader(fixture(t, "logs")), 200)
	}
	c.post(t, "traces", "application/json", strings.NewReader(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"contract"}}]},"scopeSpans":[{"spans":[{"traceId":"00112233445566778899aabbccddeeff","spanId":"0011223344556677","name":"stable"}]}]}]}`), 200)
	c.post(t, "traces", "application/json", strings.NewReader(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"contract"}}]},"scopeSpans":[{"spans":[{"traceId":"ffeeddccbbaa99887766554433221100","spanId":"7766554433221100","name":"other"},{"traceId":"00112233445566778899aabbccddeeff","spanId":"0011223344556677","name":"stable"}]}]}]}`), 200)
	for _, name := range []string{"metrics", "metrics", "metrics-overlap"} {
		c.post(t, "metrics", "application/json", bytes.NewReader(fixture(t, name)), 200)
	}
	c.post(t, "metrics", "application/json", strings.NewReader(`{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"contract-cross"}}]},"scopeMetrics":[{"metrics":[{"name":"contract.cross.gauge","gauge":{"dataPoints":[{"timeUnixNano":"0","asInt":"1"}]}}]}]}]}`), 200)
	exportSignals(t, c.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	for _, check := range []struct {
		name  string
		want  int
		query string
	}{
		{"gRPC log ingestion", 1, `SELECT COUNT(*) FROM otel_logs WHERE service_name='grpc-contract'`},
		{"gRPC trace ingestion", 1, `SELECT COUNT(*) FROM otel_spans WHERE service_name='grpc-contract'`},
		{"gRPC metric ingestion", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE service_name='grpc-contract' AND metric_name='grpc.contract.gauge'`},
		{"cross-protocol metric deduplication", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE service_name='contract-cross' AND metric_name='contract.cross.gauge'`},
		{"metric deduplication", 6, `SELECT COUNT(*) FROM otel_metric_points WHERE service_name='contract'`},
		{"metric type coverage", 5, `SELECT COUNT(DISTINCT metric_type) FROM otel_metric_points WHERE service_name='contract'`},
		{"same-time metric preservation", 2, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.gauge' AND metric_type='gauge' AND number_kind='int' AND number_int IN (1,2) AND time_unix_nano=0`},
		{"exact metric retry deduplication", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.gauge' AND number_int=1`},
		{"sum projection", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.sum' AND metric_type='sum' AND number_kind='int' AND number_int=7`},
		{"histogram projection", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.histogram' AND aggregate_count='3' AND aggregate_sum=6 AND aggregate_min=1 AND aggregate_max=3`},
		{"exponential histogram projection", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.exponential_histogram' AND aggregate_count='2' AND aggregate_sum=3`},
		{"summary projection", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.summary' AND aggregate_count='2' AND aggregate_sum=3`},
		{"histogram payload fidelity", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.histogram' AND json_array_length(json_extract(payload_json,'$.resourceMetrics[0].scopeMetrics[0].metrics[0].histogram.dataPoints[0].bucketCounts'))=3`},
		{"exponential histogram payload fidelity", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.exponential_histogram' AND json_extract(payload_json,'$.resourceMetrics[0].scopeMetrics[0].metrics[0].exponentialHistogram.dataPoints[0].positive.offset')=-1`},
		{"summary payload fidelity", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.summary' AND json_array_length(json_extract(payload_json,'$.resourceMetrics[0].scopeMetrics[0].metrics[0].summary.dataPoints[0].quantileValues'))=2`},
		{"exemplar correlation", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.gauge' AND json_extract(payload_json,'$.resourceMetrics[0].scopeMetrics[0].metrics[0].gauge.dataPoints[0].exemplars[0].traceId')='00112233445566778899aabbccddeeff'`},
		{"metric redaction markers", 6, `SELECT COUNT(*) FROM otel_metric_points WHERE payload_json LIKE '%[REDACTED]%'`},
		{"metric credential redaction", 0, `SELECT COUNT(*) FROM otel_metric_points WHERE payload_json LIKE '%metric-resource-secret%' OR payload_json LIKE '%metric-scope-secret%' OR payload_json LIKE '%metric-point-secret%' OR payload_json LIKE '%metric-exemplar-secret%'`},
		{"token count preservation", 1, `SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='contract.gauge' AND payload_json LIKE '%gen_ai.usage.input_tokens%' AND payload_json LIKE '%123%'`},
		{"log deduplication", 1, `SELECT COUNT(*) FROM otel_logs WHERE service_name='contract'`},
		{"log payload redaction", 0, `SELECT COUNT(*) FROM otel_logs WHERE payload_json LIKE '%must-not-persist%'`},
		{"log body redaction", 0, `SELECT COUNT(*) FROM otel_logs WHERE body_json LIKE '%must-not-persist%'`},
		{"redaction marker persistence", 1, `SELECT COUNT(*) FROM otel_logs WHERE payload_json LIKE '%[REDACTED]%'`},
		{"correlation field extraction", 1, `SELECT COUNT(*) FROM otel_logs WHERE op='contract.frontend.ready' AND request_id='request-123' AND product_id='product-456' AND component='frontend'`},
		{"span deduplication", 2, `SELECT COUNT(*) FROM otel_spans WHERE service_name='contract'`},
	} {
		t.Run(check.name, func(t *testing.T) {
			if got := c.count(t, check.query); got != check.want {
				t.Fatalf("got %d, want %d", got, check.want)
			}
		})
	}
	response, err := c.client.Get("http://" + c.healthAddress + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var snapshot struct {
		Ready bool `json:"ready"`
		Store struct {
			Metrics int `json:"committed_metric_points"`
		} `json:"store"`
	}
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !snapshot.Ready || snapshot.Store.Metrics != 8 {
		t.Fatalf("status=%+v HTTP %d", snapshot, response.StatusCode)
	}
	c.stop(t)
}
