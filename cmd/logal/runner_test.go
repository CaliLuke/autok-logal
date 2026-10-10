package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRunnerIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("runner subprocess tests disabled by -short")
	}
	if _, err := exec.LookPath("air"); err != nil {
		t.Skip("Air is required for runner isolation tests")
	}
	for _, name := range []string{"LOGAL_TEST_OTLP_GRPC_PORT", "LOGAL_TEST_OTLP_HTTP_PORT", "LOGAL_TEST_HEALTH_PORT", "AUTOK_LOGAL_TEST_OTLP_PORT"} {
		if os.Getenv(name) != "" {
			t.Skip("simultaneous collectors require automatic test ports")
		}
	}
	runner := filepath.Join(projectRoot, "scripts/run")
	// Track launcher changes in Go's test cache, not only compiled Go inputs.
	for _, path := range []string{runner, filepath.Join(projectRoot, ".air.toml")} {
		if _, err := os.ReadFile(path); err != nil {
			t.Fatal(err)
		}
	}
	first := startCollectorCommand(t, "", runner)
	second := startCollectorCommand(t, "", runner)
	exportSignals(t, first.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, first, 1, 1, 2)
	assertSignalCounts(t, second, 0, 0, 0)

	first.stop(t)
	// Air removes its temporary directory on exit. The surviving runner must
	// retain its executable so a failed rebuild can still use the last good one.
	secondBinary := filepath.Join(projectRoot, ".tmp", fmt.Sprintf("air-%d", second.cmd.Process.Pid), "logal")
	if _, err := os.Stat(secondBinary); err != nil {
		t.Fatalf("surviving runner lost its executable: %v", err)
	}
	exportSignals(t, second.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, second, 1, 1, 2)
}
