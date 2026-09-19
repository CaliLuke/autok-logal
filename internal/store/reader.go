package store

import (
	"context"
	"database/sql"
	"fmt"
)

// OpenReadOnly opens an existing current Logal database without taking writer
// ownership, creating a file, repairing a schema, or starting maintenance.
func OpenReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required (--db or LOGAL_DB_PATH)")
	}
	db, err := sql.Open("sqlite3", sqliteURI(path, "mode=ro&_query_only=1&_busy_timeout=50"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var appID, version int
	if err = db.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&appID); err == nil {
		err = db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	}
	if err == nil && (appID != applicationID || version != schemaVersion) {
		err = fmt.Errorf("database is not a current Logal database (application_id=%d, schema=%d)", appID, version)
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
