package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
)

func TestCollectorClearCLI(t *testing.T) {
	c := startCollector(t, "")
	export := func() { exportSignals(t, c.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics()) }
	export()
	invoke := func(wantSuccess bool, extra ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		args := []string{"clear", "--db", c.dbPath, "--endpoint", c.healthAddress, "--json"}
		args = append(args, extra...)
		command := exec.CommandContext(ctx, collectorBinary, args...)
		var out, errOut bytes.Buffer
		command.Stdout, command.Stderr = &out, &errOut
		err := command.Run()
		if (err == nil) != wantSuccess {
			t.Fatalf("args=%v err=%v stdout=%s stderr=%s", args, err, out.String(), errOut.String())
		}
		if !wantSuccess {
			var result struct {
				Error struct{ Code, Message string }
			}
			if err := json.Unmarshal(errOut.Bytes(), &result); err != nil || result.Error.Code == "" || out.Len() != 0 {
				t.Fatalf("invalid clear failure: stdout=%s stderr=%s", out.String(), errOut.String())
			}
		}
		return out.Bytes()
	}
	counts := func(want int) {
		t.Helper()
		for _, table := range []string{"otel_logs", "otel_spans", "otel_metric_points"} {
			expected := want
			if table == "otel_metric_points" {
				expected *= 2 // Fixture includes a second resource.
			}
			if count := c.count(t, "SELECT count(*) FROM "+table); count != expected {
				t.Fatalf("%s count=%d want=%d", table, count, expected)
			}
		}
	}
	invoke(false) // No confirmation must leave every signal intact.
	invoke(false, "--confirm", "--db", filepath.Join(t.TempDir(), "wrong.sqlite"))
	counts(1)
	var result store.ClearResult
	if err := json.Unmarshal(invoke(true, "--confirm"), &result); err != nil || result.DeletedLogs != 1 || result.DeletedSpans != 1 || result.DeletedMetrics != 2 || result.ClearedAt.IsZero() {
		t.Fatalf("clear result=%+v err=%v", result, err)
	}
	actual, err := filepath.EvalSymlinks(c.dbPath)
	if err != nil || result.Database != actual {
		t.Fatalf("wrong target in result: %+v err=%v", result, err)
	}
	counts(0)
	select {
	case <-c.done:
		t.Fatal("clear stopped collector")
	default:
	}
	if err := c.awaitReady(); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(invoke(true, "--confirm"), &result); err != nil || result.DeletedLogs+result.DeletedSpans+result.DeletedMetrics != 0 {
		t.Fatalf("empty clear result=%+v err=%v", result, err)
	}
	export()
	counts(1)
	c.stop(t)
	invoke(false, "--confirm") // No offline fallback that opens SQLite read-write.
	counts(1)
}

func TestClearEndpointValidation(t *testing.T) {
	for input, want := range map[string]string{
		"127.0.0.1:13133":        "http://127.0.0.1:13133/clear",
		"http://localhost:1234/": "http://localhost:1234/clear",
		"[::1]:1234":             "http://[::1]:1234/clear",
	} {
		got, err := clearEndpoint(input)
		if err != nil || got != want {
			t.Fatalf("%s: %s %v", input, got, err)
		}
	}
	for _, input := range []string{"", "https://127.0.0.1:13133", "http://example.com", "http://192.0.2.1", "http://user@localhost", "http://localhost/other", "http://localhost?q=x", "http://localhost/#x", "http://localhost:bad"} {
		if _, err := clearEndpoint(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestClearDoesNotSendUnconfirmedRequestsOrFollowRedirects(t *testing.T) {
	var calls, redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.Header.Get("X-Logal-Confirm") != "clear" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("invalid clear request: %+v", r)
		}
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	var out bytes.Buffer
	args := []string{"logal", "clear", "--db", filepath.Join(t.TempDir(), "db.sqlite"), "--endpoint", server.URL}
	if err := run(context.Background(), args, strings.NewReader(""), &out, &out); err == nil || calls.Load() != 0 {
		t.Fatalf("unconfirmed request reached server: %v", err)
	}
	if err := run(context.Background(), append(args, "--confirm"), strings.NewReader(""), &out, &out); err == nil || calls.Load() != 1 || redirected.Load() != 0 {
		t.Fatalf("unexpected retry/redirect: err=%v calls=%d redirected=%d", err, calls.Load(), redirected.Load())
	}
}

func TestClearRequiresDatabase(t *testing.T) {
	t.Setenv("LOGAL_DB_PATH", "")
	var out bytes.Buffer
	if err := run(context.Background(), []string{"logal", "clear", "--confirm"}, strings.NewReader(""), &out, &out); err == nil || !strings.Contains(err.Error(), "database path") {
		t.Fatalf("missing database accepted: %v", err)
	}
}
