package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/CaliLuke/autok-logal/internal/query"
)

func summaryColumns(command, view string) []string {
	switch command {
	case "logs":
		return strings.Split("time,service_name,severity_number,body,trace_id,span_id", ",")
	case "spans":
		return strings.Split("start_time,service_name,name,kind,status_code,duration_ms,trace_id,span_id", ",")
	case "trace":
		return strings.Split("offset_ms,duration_ms,service_name,kind,span_kind,name,status_code,status_message,span_id,parent_span_id,body", ",")
	case "metrics":
		common := "service_name,resource_id,scope_name,metric_name,metric_type,unit,temporality"
		switch view {
		case "list":
			return strings.Split(common+",monotonic,series_count,point_count,last_received_at", ",")
		case "series":
			return strings.Split(common+",attributes,point_count,first_time,last_time", ",")
		case "rate":
			return strings.Split("time,service_name,resource_id,metric_name,unit,attributes,rate_per_second,rate_reason,gap_seconds", ",")
		default:
			return strings.Split("time,"+common+",attributes,number_int,number_double,aggregate_count,aggregate_sum,aggregate_min,aggregate_max,flags", ",")
		}
	default:
		return nil
	}
}

// Human summaries use readable labels. JSON retains OTLP codes and typed values.
func humanValue(column string, value any, otel bool) string {
	if otel {
		if n, ok := value.(int64); ok {
			switch column {
			case "kind", "span_kind":
				if n >= 0 && n <= 5 {
					return []string{"unspecified", "internal", "server", "client", "producer", "consumer"}[n]
				}
			case "status_code":
				if n >= 0 && n <= 2 {
					return []string{"unset", "ok", "error"}[n]
				}
			case "severity_number":
				if n >= 1 && n <= 24 {
					return []string{"trace", "debug", "info", "warn", "error", "fatal"}[(n-1)/4]
				}
			}
		}
		if raw, ok := value.(json.RawMessage); ok {
			if column == "body" {
				var body map[string]json.RawMessage
				if json.Unmarshal(raw, &body) == nil {
					if message, ok := body["string"]; ok {
						var text string
						if json.Unmarshal(message, &text) == nil {
							return text
						}
					}
				}
			}
			if column == "attributes" || column == "resource_attributes" || column == "scope_attributes" {
				var attrs []struct {
					Key   string                     `json:"key"`
					Value map[string]json.RawMessage `json:"value"`
				}
				if json.Unmarshal(raw, &attrs) == nil {
					var pairs []string
					for _, attr := range attrs {
						for kind, v := range attr.Value {
							text := string(v)
							if kind == "intValue" {
								var number string
								if json.Unmarshal(v, &number) == nil {
									text = number
								}
							}
							pairs = append(pairs, attr.Key+"="+text)
						}
					}
					return strings.Join(pairs, ", ")
				}
			}
		}
	}
	switch value := value.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64)
	default:
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
}

func safeCell(text string) string {
	var out strings.Builder
	for _, r := range text {
		switch r {
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
				fmt.Fprintf(&out, `\u%04x`, r)
			} else {
				out.WriteRune(r)
			}
		}
	}
	return out.String()
}

func printHuman(out io.Writer, result query.Result, maxBytes int, compact, otel bool) error {
	var buffer bytes.Buffer
	widths := make([]int, len(result.Columns))
	cells := make([][]string, len(result.Rows))
	shortened := false
	for i, column := range result.Columns {
		widths[i] = utf8.RuneCountInString(safeCell(column))
	}
	for i, row := range result.Rows {
		cells[i] = make([]string, len(result.Columns))
		for j, column := range result.Columns {
			value, exists := row[column]
			if !exists {
				continue
			}
			cell := safeCell(humanValue(column, value, otel))
			if compact && utf8.RuneCountInString(cell) > 64 {
				cell = string([]rune(cell)[:63]) + "…"
				shortened = true
			}
			cells[i][j] = cell
			if width := utf8.RuneCountInString(cell); width > widths[j] {
				widths[j] = width
			}
		}
	}
	writeRow := func(values []string) {
		for i, value := range values {
			buffer.WriteString(value)
			if i+1 < len(values) {
				if compact {
					buffer.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(value)+2))
				} else {
					buffer.WriteByte('\t')
				}
			}
		}
		buffer.WriteByte('\n')
	}
	if len(result.Columns) > 0 {
		headers := make([]string, len(result.Columns))
		for i, column := range result.Columns {
			headers[i] = safeCell(column)
		}
		writeRow(headers)
	}
	for _, row := range cells {
		writeRow(row)
		if buffer.Len() > maxBytes {
			return fmt.Errorf("formatted output exceeds --max-bytes; reduce --limit or --columns, or increase --max-bytes")
		}
	}
	fmt.Fprintf(&buffer, "%d rows", result.Count)
	if result.Truncated {
		fmt.Fprintf(&buffer, "; truncated (%s), next --offset %d", result.Reason, *result.NextOffset)
	}
	fmt.Fprintln(&buffer)
	if shortened {
		fmt.Fprintln(&buffer, "Long cells shortened; use --details or --json for complete values.")
	}
	if compact && otel {
		fmt.Fprintln(&buffer, "Summary view; use --details for all fields, or --columns to choose them.")
	}
	if buffer.Len() > maxBytes {
		return fmt.Errorf("formatted output exceeds --max-bytes; reduce --limit or --columns, or increase --max-bytes")
	}
	_, err := out.Write(buffer.Bytes())
	return err
}
