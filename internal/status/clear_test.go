package status

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClearRejectsUnsafeRequests(t *testing.T) {
	s := &Status{store: newStatusStore(t)}
	s.pipelineReady.Store(true)
	for _, test := range []struct {
		name, method, peer, origin, confirm, contentType, body string
		code                                                   int
	}{
		{"get", "GET", "127.0.0.1:123", "", "clear", "application/json", `{"database":"/tmp/test"}`, 405},
		{"remote", "POST", "192.0.2.1:123", "", "clear", "application/json", `{"database":"/tmp/test"}`, 403},
		{"browser", "POST", "127.0.0.1:123", "http://localhost:3000", "clear", "application/json", `{"database":"/tmp/test"}`, 403},
		{"unconfirmed", "POST", "127.0.0.1:123", "", "", "application/json", `{"database":"/tmp/test"}`, 403},
		{"form", "POST", "127.0.0.1:123", "", "clear", "text/plain", `{"database":"/tmp/test"}`, 415},
		{"empty", "POST", "127.0.0.1:123", "", "clear", "application/json", `{}`, 400},
		{"unknown field", "POST", "127.0.0.1:123", "", "clear", "application/json", `{"database":"/tmp/test","force":true}`, 400},
		{"extra object", "POST", "127.0.0.1:123", "", "clear", "application/json", `{"database":"/tmp/test"}{}`, 400},
		{"oversized", "POST", "127.0.0.1:123", "", "clear", "application/json", `{"database":"` + strings.Repeat("x", 5000) + `"}`, 400},
		{"wrong database", "POST", "[::1]:123", "", "clear", "application/json", `{"database":"/tmp/nonexistent-logal-clear-test"}`, 409},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://localhost/clear", strings.NewReader(test.body))
			request.RemoteAddr = test.peer
			request.Header.Set("Origin", test.origin)
			request.Header.Set("X-Logal-Confirm", test.confirm)
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			s.handleClear(response, request)
			if response.Code != test.code {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestClearRequiresRunningPipeline(t *testing.T) {
	s := &Status{store: newStatusStore(t)}
	body, _ := json.Marshal(map[string]string{"database": "/tmp/test"})
	r := httptest.NewRequest(http.MethodPost, "/clear", strings.NewReader(string(body)))
	r.RemoteAddr = "127.0.0.1:123"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Logal-Confirm", "clear")
	w := httptest.NewRecorder()
	s.handleClear(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
