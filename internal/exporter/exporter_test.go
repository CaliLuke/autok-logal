package exporter

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestSpanFingerprintDoesNotDependOnBatchPosition(t *testing.T) {
	standalone := ptrace.NewTraces()
	standaloneResource := standalone.ResourceSpans().AppendEmpty()
	standaloneResource.Resource().Attributes().PutStr("service.name", "test")
	standaloneScope := standaloneResource.ScopeSpans().AppendEmpty()
	span := standaloneScope.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{2})
	span.SetName("stable")

	batched := ptrace.NewTraces()
	batchedResource := batched.ResourceSpans().AppendEmpty()
	standaloneResource.Resource().CopyTo(batchedResource.Resource())
	batchedScope := batchedResource.ScopeSpans().AppendEmpty()
	other := batchedScope.Spans().AppendEmpty()
	other.SetTraceID(pcommon.TraceID{3})
	other.SetSpanID(pcommon.SpanID{4})
	span.CopyTo(batchedScope.Spans().AppendEmpty())

	first, err := marshalSingleSpan(standaloneResource, standaloneScope, span)
	if err != nil {
		t.Fatal(err)
	}
	second, err := marshalSingleSpan(batchedResource, batchedScope, batchedScope.Spans().At(1))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(first) != sha256.Sum256(second) {
		t.Fatalf("record fingerprint changed with batch: %s != %s", first, second)
	}
}

func TestTaggedValuePreservesNestedBytes(t *testing.T) {
	value := pcommon.NewValueMap()
	value.Map().PutEmptySlice("items").AppendEmpty().SetEmptyBytes().FromRaw([]byte{1, 2, 3})
	encoded, err := json.Marshal(taggedValue(value))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"bytes":"AQID"`)) {
		t.Fatalf("missing base64 bytes tag: %s", encoded)
	}
}

func TestValidateValueRejectsLargeScalarAndDeepNesting(t *testing.T) {
	if err := validateValue(pcommon.NewValueStr(strings.Repeat("x", (1<<20)+1)), 0); err == nil {
		t.Fatal("expected oversized string rejection")
	}
	value := pcommon.NewValueMap()
	current := value.Map()
	for i := 0; i < 18; i++ {
		current = current.PutEmptyMap("nested")
	}
	if err := validateValue(value, 0); err == nil {
		t.Fatal("expected nesting rejection")
	}
}

func TestRedactMapRemovesNestedSecrets(t *testing.T) {
	attributes := pcommon.NewMap()
	attributes.PutStr("authorization", "Bearer secret")
	nested := attributes.PutEmptyMap("request")
	nested.PutStr("api_key", "secret-key")
	nested.PutStr("method", "GET")

	redactMap(attributes)
	if value, _ := attributes.Get("authorization"); value.Str() != "[REDACTED]" {
		t.Fatalf("authorization=%q", value.Str())
	}
	if value, _ := nested.Get("api_key"); value.Str() != "[REDACTED]" {
		t.Fatalf("api_key=%q", value.Str())
	}
	if value, _ := nested.Get("method"); value.Str() != "GET" {
		t.Fatalf("method=%q", value.Str())
	}
}

func TestTaggedBodyRedactsSensitiveMapKeys(t *testing.T) {
	body := pcommon.NewValueMap()
	body.Map().PutStr("password", "must-not-persist")
	body.Map().PutStr("message", "safe")
	redactValue(body)
	encoded, err := json.Marshal(taggedValue(body))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("must-not-persist")) || !bytes.Contains(encoded, []byte("[REDACTED]")) {
		t.Fatalf("body was not redacted: %s", encoded)
	}
}
