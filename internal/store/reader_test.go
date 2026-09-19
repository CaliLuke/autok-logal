package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReaderNeverCreatesOrRepairs(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "foreign", "stale"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.sqlite")
			switch kind {
			case "corrupt":
				if err := os.WriteFile(path, []byte("invalid sqlite"), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign", "stale":
				db, err := sql.Open("sqlite3", path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec("CREATE TABLE important(value TEXT); INSERT INTO important VALUES('preserve')"); err != nil {
					t.Fatal(err)
				}
				if kind == "stale" {
					if _, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d; PRAGMA user_version=%d", applicationID, schemaVersion-1)); err != nil {
						t.Fatal(err)
					}
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadFile(path)
			db, err := OpenReadOnly(context.Background(), path)
			if err == nil {
				db.Close()
				t.Fatal("unexpected success")
			}
			if kind == "missing" {
				if _, err = os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("created database: %v", err)
				}
				return
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("reader changed file")
			}
		})
	}
}

func TestReaderEnforcesReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.sqlite")
	writer, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = createSchema(writer); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err = reader.Exec("CREATE TABLE forbidden(value TEXT)"); err == nil {
		t.Fatal("read-only handle allowed write")
	}
}
