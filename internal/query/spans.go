package query

import (
	"fmt"
	"time"
)

func Spans(f Filters, now time.Time) (string, []any, error) {
	where, args, err := otelWhere(f, now, "start_time_unix_nano")
	if err != nil {
		return "", nil, err
	}
	for _, filter := range []struct {
		column, value string
		size          int
	}{{"trace_id", f.TraceID, 16}, {"span_id", f.SpanID, 8}} {
		if filter.value != "" {
			id, err := decodeID(filter.value, filter.column, filter.size)
			if err != nil {
				return "", nil, err
			}
			where += " AND " + filter.column + "=?"
			args = append(args, id)
		}
	}
	if f.Name != "" {
		where += " AND name=?"
		args = append(args, f.Name)
	}
	if f.Kind != "" {
		value, ok := map[string]int{"unspecified": 0, "internal": 1, "server": 2, "client": 3, "producer": 4, "consumer": 5}[f.Kind]
		if !ok {
			return "", nil, fmt.Errorf("kind must be unspecified, internal, server, client, producer, or consumer")
		}
		where += " AND kind=?"
		args = append(args, value)
	}
	if f.Status != "" {
		value, ok := map[string]int{"unset": 0, "ok": 1, "error": 2}[f.Status]
		if !ok {
			return "", nil, fmt.Errorf("status must be unset, ok, or error")
		}
		where += " AND status_code=?"
		args = append(args, value)
	}
	if f.MinDuration != "" {
		duration, err := time.ParseDuration(f.MinDuration)
		if err != nil || duration < 0 {
			return "", nil, fmt.Errorf("min-duration must be a nonnegative duration")
		}
		where += " AND start_time_unix_nano>0 AND end_time_unix_nano>=start_time_unix_nano AND end_time_unix_nano-start_time_unix_nano>=?"
		args = append(args, int64(duration))
	}
	columns := `id,received_at_unix_nano AS received_at,service_name,trace_id,span_id,parent_span_id,name,kind,status_code,status_message,
 start_time_unix_nano AS start_time,end_time_unix_nano AS end_time,
 CASE WHEN start_time_unix_nano>0 AND end_time_unix_nano>=start_time_unix_nano THEN (end_time_unix_nano-start_time_unix_nano)/1000000.0 END AS duration_ms,
 resource_attributes,resource_schema_url,scope_name,scope_version,scope_attributes,scope_schema_url,attributes,events,links`
	if f.Payload {
		columns += ",payload_json AS payload"
	}
	source := "SELECT *," + otelContext(spanResource, spanScope) + "," + arrayField(spanRecord+".attributes") + " AS attributes," +
		arrayField(spanRecord+".events") + " AS events," + arrayField(spanRecord+".links") + " AS links,COALESCE(" + extract(spanRecord+".kind") + ",0) AS kind,COALESCE(" + extract(spanRecord+".status.code") + ",0) AS status_code," + textField(spanRecord+".status.message") + " AS status_message FROM otel_spans"
	return "WITH source AS (" + source + ") SELECT " + columns + " FROM source WHERE " + where + " ORDER BY id DESC", args, nil
}

// Services aggregates each signal before the union. Keeping OTLP context in
// the inner SELECT lets SQLite prune JSON extraction unless a filter needs it.
func Services(f Filters, now time.Time) (string, []any, error) {
	sources := []struct{ table, resource, scope, event, signal, receivedIndex string }{
		{"otel_logs", logResource, logScope, "time_unix_nano", "logs", "idx_logs_received"},
		{"otel_spans", spanResource, spanScope, "start_time_unix_nano", "spans", "idx_spans_received"},
		{"otel_metric_points", metricResource, metricScope, "time_unix_nano", "metric_points", "idx_metric_points_received"},
	}
	sql := "WITH source AS ("
	var bindings []any
	for i, s := range sources {
		where, args, err := otelWhere(f, now, s.event)
		if err != nil {
			return "", nil, err
		}
		bindings = append(bindings, args...)
		if i > 0 {
			sql += " UNION ALL "
		}
		sql += "SELECT service_name"
		for _, signal := range []string{"logs", "spans", "metric_points"} {
			if signal == s.signal {
				sql += ",COUNT(*) AS " + signal
			} else {
				sql += ",0 AS " + signal
			}
		}
		// With only a lower bound, GROUP BY can tempt SQLite into scanning the
		// complete service/time index. The receipt range is the bounded work here.
		table := s.table
		if f.TimeBasis == "" || f.TimeBasis == "received" {
			table += " INDEXED BY " + s.receivedIndex
		}
		sql += ",MAX(received_at_unix_nano) AS last_received_at FROM (SELECT *," + otelContext(s.resource, s.scope) + " FROM " + table + ") WHERE " + where + " GROUP BY service_name"
	}
	sql += ") SELECT service_name,SUM(logs) AS logs,SUM(spans) AS spans,SUM(metric_points) AS metric_points,MAX(last_received_at) AS last_received_at FROM source GROUP BY service_name ORDER BY service_name"
	return sql, bindings, nil
}
