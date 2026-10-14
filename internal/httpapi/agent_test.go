package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-api-demo/internal/llm"
)

func TestAgentRunReturnsExecutionTrace(t *testing.T) {
	generator := fakeGenerator{toolResult: llm.ToolResult{
		ID:         "resp_agent",
		Model:      "test-model",
		Text:       "结果是 100。",
		ModelCalls: 3,
		ToolCalls: []llm.ToolCallTrace{
			{Step: 1, CallID: "call_1", Name: "calculate", Arguments: []byte(`{"operation":"multiply","a":12,"b":7}`), Output: []byte(`{"result":84}`)},
			{Step: 2, CallID: "call_2", Name: "calculate", Arguments: []byte(`{"operation":"add","a":84,"b":16}`), Output: []byte(`{"result":100}`)},
		},
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/run", strings.NewReader(`{"goal":"先算 12×7，再加 16"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, expected := range []string{`"text":"结果是 100。"`, `"model_calls":3`, `"step":2`, `"result":100`} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Fatalf("body does not contain %q: %s", expected, recorder.Body.String())
		}
	}
}

func TestAgentRunRejectsEmptyGoal(t *testing.T) {
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/run", strings.NewReader(`{"goal":" "}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "goal must not be empty") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
