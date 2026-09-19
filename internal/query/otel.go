package query

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// These paths refer to the single-record OTLP envelopes stored by the exporters.
const (
	logResource    = "$.resourceLogs[0]"
	logScope       = logResource + ".scopeLogs[0]"
	logRecord      = logScope + ".logRecords[0]"
	spanResource   = "$.resourceSpans[0]"
	spanScope      = spanResource + ".scopeSpans[0]"
	spanRecord     = spanScope + ".spans[0]"
	metricResource = "$.resourceMetrics[0]"
	metricScope    = metricResource + ".scopeMetrics[0]"
	metricRecord   = metricScope + ".metrics[0]"
)

func extract(path string) string    { return "json_extract(payload_json,'" + path + "')" }
func textField(path string) string  { return "COALESCE(" + extract(path) + ",'')" }
func arrayField(path string) string { return "COALESCE(" + extract(path) + ",'[]')" }

func otelContext(resource, scope string) string {
	return arrayField(resource+".resource.attributes") + ` AS resource_attributes, ` +
		textField(resource+".schemaUrl") + ` AS resource_schema_url, ` +
		textField(scope+".scope.name") + ` AS scope_name, ` +
		textField(scope+".scope.version") + ` AS scope_version, ` +
		arrayField(scope+".scope.attributes") + ` AS scope_attributes, ` +
		textField(scope+".schemaUrl") + ` AS scope_schema_url`
}

func decodeID(value, name string, size int) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != size || strings.Trim(value, "0") == "" {
		return nil, fmt.Errorf("%s must be %d hexadecimal digits and nonzero", name, size*2)
	}
	return decoded, nil
}

func otelWhere(f Filters, now time.Time, eventColumn string) (string, []any, error) {
	field := "received_at_unix_nano"
	switch f.TimeBasis {
	case "", "received":
	case "event":
		field = eventColumn
	default:
		return "", nil, fmt.Errorf("time must be received or event")
	}
	since, err := cutoff(f.Since, now)
	if err != nil {
		return "", nil, err
	}
	where := field + ">=?"
	args := []any{since}
	if f.Until != "" {
		end, err := time.Parse(time.RFC3339Nano, f.Until)
		if err != nil || end.Before(time.Unix(0, 0)) || end.After(time.Unix(0, 1<<63-1)) {
			return "", nil, fmt.Errorf("until must be an RFC3339 timestamp in the supported range")
		}
		if end.UnixNano() <= since {
			return "", nil, fmt.Errorf("until must be after since")
		}
		where += " AND " + field + "<?"
		args = append(args, end.UnixNano())
	}
	for _, filter := range []struct{ column, value string }{{"service_name", f.Service}, {"scope_name", f.Scope}, {"scope_version", f.ScopeVersion}} {
		if filter.value != "" {
			where += " AND " + filter.column + "=?"
			args = append(args, filter.value)
		}
	}
	if len(f.Resource)+len(f.Attribute) > 32 {
		return "", nil, fmt.Errorf("at most 32 resource and attribute filters are allowed")
	}
	for _, group := range []struct {
		column string
		values []string
	}{{"resource_attributes", f.Resource}, {"attributes", f.Attribute}} {
		for _, value := range group.values {
			clause, bindings, err := attributeFilter(group.column, value)
			if err != nil {
				return "", nil, err
			}
			where += " AND " + clause
			args = append(args, bindings...)
		}
	}
	return where, args, nil
}

// Bare values are strings. JSON scalars retain their OTLP type; quote a numeric
// string explicitly (http.response.status_code=200 versus build.version="200").
func attributeFilter(column, filter string) (string, []any, error) {
	key, value, ok := strings.Cut(filter, "=")
	if !ok || key == "" {
		return "", nil, fmt.Errorf("attribute filter must use KEY=VALUE")
	}
	if len(filter) > 4096 {
		return "", nil, fmt.Errorf("attribute filter exceeds 4096 bytes")
	}
	field := "stringValue"
	var bound any = value
	if json.Valid([]byte(value)) {
		decoder := json.NewDecoder(strings.NewReader(value))
		decoder.UseNumber()
		var scalar any
		if err := decoder.Decode(&scalar); err != nil {
			return "", nil, err
		}
		switch v := scalar.(type) {
		case string:
			bound = v
		case bool:
			field = "boolValue"
			bound = v
		case json.Number:
			if strings.ContainsAny(string(v), ".eE") {
				field = "doubleValue"
				n, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return "", nil, fmt.Errorf("invalid double attribute value")
				}
				bound = n
			} else {
				field = "intValue"
				n, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return "", nil, fmt.Errorf("integer attribute value is outside int64 range")
				}
				bound = n
			}
		default:
			return "", nil, fmt.Errorf("attribute filters support strings, numbers, and booleans")
		}
	}
	expr := "json_extract(a.value,'$.value." + field + "')"
	if field == "intValue" {
		expr = "CAST(" + expr + " AS INTEGER)"
	}
	return "EXISTS (SELECT 1 FROM json_each(" + column + ") a WHERE json_extract(a.value,'$.key')=? AND " + expr + "=?)", []any{key, bound}, nil
}
