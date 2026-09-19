package main

import (
	"os"
	"testing"
)

func TestCollectorIsolation(t *testing.T) {
	for _, name := range []string{"LOGAL_TEST_OTLP_GRPC_PORT", "LOGAL_TEST_OTLP_HTTP_PORT", "LOGAL_TEST_HEALTH_PORT", "AUTOK_LOGAL_TEST_OTLP_PORT"} {
		if os.Getenv(name) != "" {
			t.Skip("simultaneous collectors require automatic test ports")
		}
	}
	first, second := startCollector(t, ""), startCollector(t, "")
	if first.dbPath == second.dbPath {
		t.Fatal("collectors share a database")
	}
	ports := make(map[string]bool)
	for _, address := range []string{first.grpcAddress, first.httpAddress, first.healthAddress, second.grpcAddress, second.httpAddress, second.healthAddress} {
		if ports[address] {
			t.Fatalf("collectors share listener %s", address)
		}
		ports[address] = true
	}
	exportSignals(t, first.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, first, 1, 1, 2)
	assertSignalCounts(t, second, 0, 0, 0)
	first.stop(t)
	exportSignals(t, second.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, second, 1, 1, 2)
}
