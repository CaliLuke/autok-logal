package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"github.com/urfave/cli/v3"
)

func newClearCommand() *cli.Command {
	return &cli.Command{
		Name: "clear", Usage: "Delete all stored logs, spans, and metrics from a running collector",
		Description: "Requires --confirm and the database path. The collector deletes stored telemetry atomically. In-flight or later exports can write new data. No collector restart is needed.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "db", Usage: "Database owned by the collector (required)", Sources: cli.EnvVars("LOGAL_DB_PATH")},
			&cli.StringFlag{Name: "endpoint", Value: "127.0.0.1:13133", Usage: "Local collector health endpoint (host:port or http://host:port)", Sources: cli.EnvVars("LOGAL_HEALTH_ENDPOINT")},
			&cli.BoolFlag{Name: "confirm", Usage: "Confirm deletion of all stored telemetry"},
			&cli.BoolFlag{Name: "json", Usage: "Emit JSON results and errors"},
			&cli.DurationFlag{Name: "timeout", Value: 10 * time.Second, Usage: "Client request deadline (at most 30s; server deletion limit remains 5s)"},
		},
		OnUsageError: queryUsageError,
		Action: func(ctx context.Context, c *cli.Command) error {
			if c.Args().Len() != 0 {
				return fmt.Errorf("clear does not accept positional arguments")
			}
			if !c.Bool("confirm") {
				return fmt.Errorf("clear requires --confirm to delete all stored logs, spans, and metrics")
			}
			if c.String("db") == "" {
				return fmt.Errorf("database path is required (--db or LOGAL_DB_PATH)")
			}
			if c.Duration("timeout") <= 0 || c.Duration("timeout") > 30*time.Second {
				return fmt.Errorf("timeout must be positive and at most 30s")
			}
			path, err := filepath.Abs(c.String("db"))
			if err != nil {
				return err
			}
			endpoint, err := clearEndpoint(c.String("endpoint"))
			if err != nil {
				return err
			}
			body, err := json.Marshal(struct {
				Database string `json:"database"`
			}{path})
			if err != nil {
				return err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
			if err != nil {
				return err
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Logal-Confirm", "clear")
			transport := &http.Transport{} // No environment proxy for local administration.
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: c.Duration("timeout"), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			response, err := client.Do(request)
			if err != nil {
				return fmt.Errorf("clear request failed: %w; check the collector and query telemetry before retrying", err)
			}
			defer response.Body.Close()
			data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
			if err != nil {
				return fmt.Errorf("read clear result: %w; query telemetry before retrying", err)
			}
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("clear failed (HTTP %d): %s", response.StatusCode, strings.TrimSpace(string(data)))
			}
			var result store.ClearResult
			if err := json.Unmarshal(data, &result); err != nil || result.Database == "" || result.ClearedAt.IsZero() {
				return fmt.Errorf("invalid collector clear response; query telemetry before retrying")
			}
			if c.Bool("json") {
				return json.NewEncoder(c.Root().Writer).Encode(result)
			}
			_, err = fmt.Fprintf(c.Root().Writer, "Cleared %d logs, %d spans, and %d metric points from %s at %s.\n", result.DeletedLogs, result.DeletedSpans, result.DeletedMetrics, result.Database, result.ClearedAt.Format(time.RFC3339Nano))
			return err
		},
	}
}

func clearEndpoint(value string) (string, error) {
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	endpoint, err := url.Parse(value)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return "", fmt.Errorf("endpoint must be a local HTTP health endpoint, such as 127.0.0.1:13133")
	}
	host := endpoint.Hostname()
	if host != "localhost" && !net.ParseIP(host).IsLoopback() {
		return "", fmt.Errorf("clear endpoint must use a loopback IP address or localhost")
	}
	endpoint.Path = "/clear"
	return endpoint.String(), nil
}
