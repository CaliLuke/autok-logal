package main

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

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

func contractMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "grpc-contract")
	scope := resource.ScopeMetrics().AppendEmpty()
	metric := scope.Metrics().AppendEmpty()
	metric.SetName("grpc.contract.gauge")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)

	crossResource := metrics.ResourceMetrics().AppendEmpty()
	crossResource.Resource().Attributes().PutStr("service.name", "contract-cross")
	crossMetric := crossResource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	crossMetric.SetName("contract.cross.gauge")
	crossMetric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	return metrics
}
