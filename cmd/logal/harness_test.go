package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var collectorBinary string
var projectRoot string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	flag.Parse()
	if testing.Short() {
		return m.Run()
	}
	var err error
	projectRoot, err = filepath.Abs("../..")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	directory, err := os.MkdirTemp("", "logal-contract-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(directory)
	collectorBinary = filepath.Join(directory, "logal")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"build", "-o", collectorBinary}
	if os.Getenv("LOGAL_TEST_RACE") == "1" {
		args = append(args, "-race")
	}
	args = append(args, "./cmd/logal")
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = projectRoot
	if output, err := command.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build contract collector: %v\n%s", err, output)
		return 1
	}
	return m.Run()
}

// Each child has its own files and ports. No process-wide environment changes
// are needed, so separate invocations can run concurrently.
type collector struct {
	cmd                                     *exec.Cmd
	done                                    chan struct{}
	waitErr                                 error
	ready, crashed, stopped                 bool
	dbPath, logPath                         string
	grpcAddress, httpAddress, healthAddress string
	client                                  *http.Client
}

func startCollector(t *testing.T, dbPath string) *collector {
	t.Helper()
	if testing.Short() {
		t.Skip("collector subprocess tests disabled by -short")
	}
	// Read the configuration in the test process too, so Go's test cache tracks
	// it. Production Go code is already a dependency of this main-package test.
	if _, err := os.ReadFile(filepath.Join(projectRoot, "config/local.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Fatal("lsof is required for collector ownership checks")
	}
	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), "otel.debug.sqlite")
	}
	overrides := []string{os.Getenv("LOGAL_TEST_OTLP_GRPC_PORT"), os.Getenv("LOGAL_TEST_OTLP_HTTP_PORT"), os.Getenv("LOGAL_TEST_HEALTH_PORT")}
	if overrides[1] == "" {
		overrides[1] = os.Getenv("AUTOK_LOGAL_TEST_OTLP_PORT")
	}
	for attempt := 0; attempt < 3; attempt++ {
		listeners := make([]net.Listener, 0, 3)
		addresses := make([]string, 0, 3)
		for _, port := range overrides {
			if port == "" {
				port = "0"
			}
			listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", port))
			if err != nil {
				for _, held := range listeners {
					_ = held.Close()
				}
				t.Fatalf("reserve test listener: %v", err)
			}
			listeners = append(listeners, listener)
			addresses = append(addresses, listener.Addr().String())
		}
		c := &collector{done: make(chan struct{}), dbPath: dbPath,
			grpcAddress: addresses[0], httpAddress: addresses[1], healthAddress: addresses[2],
			logPath: filepath.Join(t.TempDir(), "collector.log"),
			client:  &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{}},
		}
		log, err := os.Create(c.logPath)
		if err != nil {
			for _, held := range listeners {
				_ = held.Close()
			}
			t.Fatal(err)
		}
		c.cmd = exec.Command(collectorBinary, "--config", filepath.Join(projectRoot, "config/local.yaml"))
		overridesEnv := map[string]string{
			"LOGAL_DB_PATH": dbPath, "LOGAL_RETENTION_HOURS": "48", "LOGAL_MAX_IN_FLIGHT_REQUESTS": "8",
			"LOGAL_OTLP_GRPC_ENDPOINT": c.grpcAddress, "LOGAL_OTLP_HTTP_ENDPOINT": c.httpAddress, "LOGAL_HEALTH_ENDPOINT": c.healthAddress,
		}
		for _, entry := range os.Environ() {
			key, _, _ := strings.Cut(entry, "=")
			if _, replaced := overridesEnv[key]; !replaced {
				c.cmd.Env = append(c.cmd.Env, entry)
			}
		}
		for key, value := range overridesEnv {
			c.cmd.Env = append(c.cmd.Env, key+"="+value)
		}
		c.cmd.Stdout, c.cmd.Stderr = log, log
		// Collector does not accept inherited listeners. Keep all reservations until
		// launch, then retry only a confirmed bind collision in the remaining gap.
		for _, held := range listeners {
			_ = held.Close()
		}
		if err := c.cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatal(err)
		}
		go func() { c.waitErr = c.cmd.Wait(); _ = log.Close(); close(c.done) }()
		t.Cleanup(func() {
			c.stop(t)
			c.client.CloseIdleConnections()
			if t.Failed() {
				t.Logf("collector output:\n%s", c.logs())
			}
		})
		if err := c.awaitReady(); err == nil {
			c.ready = true
			return c
		} else {
			c.stop(t)
			if strings.Contains(c.logs(), "address already in use") && overrides[0]+overrides[1]+overrides[2] == "" && attempt < 2 {
				t.Log("retrying automatic ports after a bind collision")
				continue
			}
			t.Fatalf("collector startup: %v\n%s", err, c.logs())
		}
	}
	t.Fatal("could not allocate collector ports")
	return nil
}

func (c *collector) logs() string { contents, _ := os.ReadFile(c.logPath); return string(contents) }

func (c *collector) awaitReady() error {
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return fmt.Errorf("exited before readiness: %v", c.waitErr)
		case <-deadline.C:
			return fmt.Errorf("readiness deadline exceeded")
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.healthAddress+"/readyz", nil)
			if err != nil {
				cancel()
				return err
			}
			response, err := c.client.Do(request)
			if err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					cancel()
					return nil
				}
			}
			cancel()
		}
	}
}

func (c *collector) stop(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
		if c.ready && !c.crashed && !c.stopped {
			t.Errorf("collector exited unexpectedly: %v", c.waitErr)
		}
		return
	default:
	}
	c.stopped = true
	if err := c.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Errorf("interrupt collector: %v", err)
	}
	select {
	case <-c.done:
		if c.waitErr != nil {
			t.Errorf("graceful shutdown: %v", c.waitErr)
		}
	case <-time.After(10 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
		t.Error("collector required a forced kill after shutdown timeout")
	}
}

func (c *collector) crash(t *testing.T) {
	t.Helper()
	c.crashed = true
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.done:
		if c.waitErr == nil {
			t.Fatal("forced crash unexpectedly exited successfully")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("killed collector did not exit")
	}
}

func (c *collector) post(t *testing.T, signal, contentType string, body io.Reader, expected int) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.httpAddress+"/v1/"+signal, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	response, err := c.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expected {
		t.Fatalf("%s: HTTP %d, want %d: %s", signal, response.StatusCode, expected, data)
	}
	return data
}

func (c *collector) readDB(t *testing.T) *sql.DB {
	t.Helper()
	uri := (&url.URL{Scheme: "file", Path: c.dbPath, RawQuery: "mode=ro&_query_only=1&_busy_timeout=1000"}).String()
	db, err := sql.Open("sqlite3", uri)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func (c *collector) count(t *testing.T, query string) int {
	t.Helper()
	db := c.readDB(t)
	defer db.Close() // Startup refuses even readers, so none may survive a restart.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
