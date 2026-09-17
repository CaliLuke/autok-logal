package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:4317", "OTLP/gRPC endpoint")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connection, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer connection.Close()

	if _, err := plogotlp.NewGRPCClient(connection).Export(ctx, plogotlp.NewExportRequestFromLogs(contractLogs())); err != nil {
		fatal(fmt.Errorf("export logs: %w", err))
	}
	if _, err := ptraceotlp.NewGRPCClient(connection).Export(ctx, ptraceotlp.NewExportRequestFromTraces(contractTraces())); err != nil {
		fatal(fmt.Errorf("export traces: %w", err))
	}
	metricsClient := pmetricotlp.NewGRPCClient(connection)
	if _, err := metricsClient.Export(ctx, pmetricotlp.NewExportRequest()); status.Code(err) != codes.Unimplemented {
		fatal(fmt.Errorf("expected unsupported metrics, got: %v", err))
	}
}

func contractLogs() plog.Logs {
	logs := plog.NewLogs()
	resource := logs.ResourceLogs().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "grpc-contract")
	record := resource.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetSeverityNumber(plog.SeverityNumberInfo)
	record.Body().SetStr("grpc log")
	return logs
}

func contractTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	resource := traces.ResourceSpans().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "grpc-contract")
	span := resource.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4})
	span.SetSpanID(pcommon.SpanID{5, 6, 7, 8})
	span.SetName("grpc span")
	return traces
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
