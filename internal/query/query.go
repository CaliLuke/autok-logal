// Package query implements bounded, read-only access to disposable telemetry.
package query

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"github.com/mattn/go-sqlite3"
)

const MaxSQLBytes = 64 << 10

type Options struct {
	ColumnsOnly  bool
	IncludeEmpty bool
	Columns      []string
	Path         string
	Limit        int
	Offset       int
	MaxBytes     int
	Timeout      time.Duration
}

type Result struct {
	Columns    []string         `json:"columns"`
	Rows       []map[string]any `json:"rows"`
	Count      int              `json:"count"`
	Truncated  bool             `json:"truncated"`
	Reason     string           `json:"truncation_reason,omitempty"`
	NextOffset *int             `json:"next_offset,omitempty"`
}

func (o Options) Validate() error {
	if o.Path == "" {
		return fmt.Errorf("database path is required (--db or LOGAL_DB_PATH)")
	}
	if o.Limit < 1 || o.Limit > 10000 {
		return fmt.Errorf("limit must be between 1 and 10000")
	}
	if o.Offset < 0 || o.Offset > 1000000 {
		return fmt.Errorf("offset must be between 0 and 1000000")
	}
	if o.MaxBytes < 1024 || o.MaxBytes > 16<<20 {
		return fmt.Errorf("max-bytes must be between 1024 and 16777216")
	}
	if o.Timeout <= 0 || o.Timeout > 30*time.Second {
		return fmt.Errorf("timeout must be positive and at most 30s")
	}
	return nil
}

// Read returns fully materialized results, releasing all readers before the
// caller formats or writes them. The SELECT wrapper applies pagination to SQL
// queries as well as built-in commands; SQLite's authorizer is the write barrier.
func Read(ctx context.Context, options Options, statement string, args []any, formatTelemetry bool) (Result, error) {
	result := Result{Rows: make([]map[string]any, 0)}
	if err := options.Validate(); err != nil {
		return result, err
	}
	statement, err := singleStatement(statement)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	db, err := store.OpenReadOnly(ctx, options.Path)
	if err != nil {
		return result, err
	}
	defer db.Close()
	connection, err := db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer connection.Close()
	if err := connection.Raw(func(raw any) error {
		sqlite := raw.(*sqlite3.SQLiteConn)
		if err := sqlite.RegisterFunc("logal_resource_id", resourceID, true); err != nil {
			return err
		}
		sqlite.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, 16<<20)
		sqlite.SetLimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, MaxSQLBytes+256)
		sqlite.SetLimit(sqlite3.SQLITE_LIMIT_COLUMN, 128)
		sqlite.RegisterAuthorizer(func(operation int, _, name, _ string) int {
			switch operation {
			case sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, 33: // SQLITE_RECURSIVE is not exported by this driver.
				return sqlite3.SQLITE_OK
			case sqlite3.SQLITE_FUNCTION:
				if !strings.EqualFold(name, "load_extension") {
					return sqlite3.SQLITE_OK
				}
			}
			return sqlite3.SQLITE_DENY
		})
		return nil
	}); err != nil {
		return result, err
	}
	limit, offset := options.Limit+1, options.Offset
	if options.ColumnsOnly {
		limit, offset = 0, 0
	}
	args = append(append([]any(nil), args...), limit, offset)
	rows, err := connection.QueryContext(ctx, "SELECT * FROM (\n"+statement+"\n) LIMIT ? OFFSET ?", args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return result, err
	}
	seen := make(map[string]bool)
	for _, column := range columns {
		if seen[column] {
			return result, fmt.Errorf("duplicate column %q; use distinct SQL aliases", column)
		}
		seen[column] = true
	}
	if options.ColumnsOnly {
		result.Columns = columns
		encoded, _ := json.Marshal(result)
		if len(encoded)+1 > options.MaxBytes {
			return result, fmt.Errorf("column names exceed output budget")
		}
		return result, nil
	}
	selected := columns
	if len(options.Columns) > 0 {
		selected = options.Columns
		requested := make(map[string]bool)
		for _, column := range selected {
			if !seen[column] {
				return result, fmt.Errorf("unknown column %q; available columns: %s", column, strings.Join(columns, ", "))
			}
			if requested[column] {
				return result, fmt.Errorf("duplicate requested column %q", column)
			}
			requested[column] = true
		}
	}
	wanted := make(map[string]bool)
	for _, column := range selected {
		wanted[column] = true
	}
	result.Columns = []string{}
	if options.IncludeEmpty {
		result.Columns = selected
	}
	visible := make(map[string]bool)
	header, _ := json.Marshal(result)
	used := len(header) + 128 // Reserve metadata, commas, and the final newline.
	if used > options.MaxBytes {
		return result, fmt.Errorf("column names exceed output budget")
	}
	for rows.Next() {
		if len(result.Rows) == options.Limit {
			result.Truncated = true
			result.Reason = "row_limit"
			break
		}
		values := make([]any, len(columns))
		destinations := make([]any, len(values))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			return result, err
		}
		row := make(map[string]any, len(values))
		for i, value := range values {
			if !wanted[columns[i]] {
				continue
			}
			if blob, ok := value.([]byte); ok {
				value = hex.EncodeToString(blob)
			}
			if formatTelemetry {
				value = formatValue(columns[i], value)
			}
			if options.IncludeEmpty || !emptyField(value) {
				row[columns[i]] = value
			}
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return result, err
		}
		columnBytes := 0
		if !options.IncludeEmpty {
			for column := range row {
				if !visible[column] {
					encodedColumn, _ := json.Marshal(column)
					columnBytes += len(encodedColumn) + 1
				}
			}
		}
		if used+len(encoded)+1+columnBytes > options.MaxBytes {
			if len(result.Rows) == 0 {
				return result, fmt.Errorf("first row exceeds output budget; increase --max-bytes or choose fewer --columns")
			}
			result.Truncated = true
			result.Reason = "byte_limit"
			break
		}
		used += len(encoded) + 1 + columnBytes
		for column := range row {
			visible[column] = true
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !options.IncludeEmpty {
		for _, column := range selected {
			if visible[column] {
				result.Columns = append(result.Columns, column)
			}
		}
	}
	result.Count = len(result.Rows)
	if result.Truncated {
		next := options.Offset + result.Count
		result.NextOffset = &next
	}
	return result, nil
}

// Only omit empty top-level fields. Nested OTLP values are data and must stay
// intact, including explicitly empty attributes. Numeric zero and false matter.
func emptyField(value any) bool {
	switch value := value.(type) {
	case nil:
		return true
	case string:
		return value == ""
	case json.RawMessage:
		raw := bytes.TrimSpace(value)
		if bytes.Equal(raw, []byte("null")) || bytes.Equal(raw, []byte(`""`)) {
			return true
		}
		if len(raw) >= 2 && ((raw[0] == '[' && raw[len(raw)-1] == ']') || (raw[0] == '{' && raw[len(raw)-1] == '}')) {
			return len(bytes.TrimSpace(raw[1:len(raw)-1])) == 0
		}
	}
	return false
}

func formatValue(column string, value any) any {
	switch column {
	case "monotonic":
		if n, ok := value.(int64); ok {
			return n != 0
		}
	case "received_at", "time", "start_time", "end_time", "first_time", "last_time", "last_received_at", "interval_start", "interval_end", "observed_time":
		if number, ok := value.(int64); ok {
			if number == 0 {
				return nil
			}
			return time.Unix(0, number).UTC().Format(time.RFC3339Nano)
		}
	case "body", "payload", "resource_attributes", "scope_attributes", "attributes", "events", "links", "bucket_counts", "explicit_bounds", "positive", "negative", "quantile_values", "exemplars":
		if text, ok := value.(string); ok && json.Valid([]byte(text)) {
			return json.RawMessage(text)
		}
	}
	return value
}

// Reject statement separators outside SQL strings/comments. This prevents an
// input from escaping the bounded SELECT wrapper and executing a second query.
func singleStatement(statement string) (string, error) {
	if len(statement) > MaxSQLBytes {
		return "", fmt.Errorf("SQL exceeds 64 KiB")
	}
	statement = strings.TrimSpace(statement)
	statement = strings.TrimSuffix(statement, ";")
	if len(statement) == 0 || len(statement) > MaxSQLBytes || strings.ContainsRune(statement, 0) {
		return "", fmt.Errorf("SQL must contain one SELECT or WITH query of at most 64 KiB")
	}
	var quote byte
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(statement) && statement[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if ch == '-' && i+1 < len(statement) && statement[i+1] == '-' {
			for i < len(statement) && statement[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(statement) && statement[i+1] == '*' {
			end := strings.Index(statement[i+2:], "*/")
			if end < 0 {
				return "", fmt.Errorf("unterminated SQL comment")
			}
			i += end + 3
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '[':
			quote = ']'
		case ';':
			return "", fmt.Errorf("only one SELECT or WITH query is allowed")
		}
	}
	if quote != 0 {
		return "", fmt.Errorf("unterminated SQL string or identifier")
	}
	return statement, nil
}
