package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-api-demo/internal/llm"
)

func TestAssistantStreamRoutesChatAndStreamsDeltas(t *testing.T) {
	handler := newTestHandler(fakeGenerator{
		deltas: []string{"你", "好"},
		result: llm.Result{ID: "resp_chat", Model: "test-model", Text: "你好", Usage: llm.Usage{TotalTokens: 5}},
	}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/assistant/stream", strings.NewReader(`{"message":"你好"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	assertSSEContains(t, recorder, `event: route`, `"path":"chat"`, `data: {"text":"你"}`, `data: {"text":"好"}`, `"route":"chat"`)
}

func TestAssistantStreamRoutesRAGWithSources(t *testing.T) {
	knowledge := newTestKnowledgeService(t, fakeKnowledgeEmbedder{})
	if _, err := knowledge.AddDocument(context.Background(), "go-guide", "Go 使用 goroutine 和 channel 实现并发编程。"); err != nil {
		t.Fatal(err)
	}
	var receivedSources []llm.GroundingSource
	generator := fakeGenerator{groundedStreamFn: func(_ context.Context, _ string, sources []llm.GroundingSource, onDelta func(string) error) (llm.Result, error) {
		receivedSources = append([]llm.GroundingSource(nil), sources...)
		if err := onDelta("使用 goroutine。[1]"); err != nil {
			return llm.Result{}, err
		}
		return llm.Result{ID: "resp_rag", Model: "test-model", Text: "使用 goroutine。[1]"}, nil
	}}
	handler := newTestHandlerWithKnowledge(generator, knowledge, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/assistant/stream", strings.NewReader(`{"message":"从知识库检索 Go 并发资料"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if len(receivedSources) != 1 || receivedSources[0].DocumentID != "go-guide" {
		t.Fatalf("sources=%#v", receivedSources)
	}
	assertSSEContains(t, recorder, `"path":"rag"`, `event: sources`, `"document_id":"go-guide"`, `使用 goroutine`, `"route":"rag"`)
}

func TestAssistantStreamRoutesAgentWithToolTrace(t *testing.T) {
	handler := newTestHandler(fakeGenerator{toolResult: llm.ToolResult{
		ID: "resp_agent", Model: "test-model", Text: "结果是 100。", ModelCalls: 3,
		ToolCalls: []llm.ToolCallTrace{{Step: 1, Name: "calculate", Arguments: json.RawMessage(`{"operation":"multiply","a":12,"b":7}`), Output: json.RawMessage(`{"result":84}`)}},
	}}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/assistant/stream", strings.NewReader(`{"message":"计算 12 乘以 7"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	assertSSEContains(t, recorder, `"path":"agent"`, `event: status`, `event: tools`, `"name":"calculate"`, `结果是 100`, `"route":"agent"`)
}

func TestAssistantStreamRejectsEmptyMessage(t *testing.T) {
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/assistant/stream", strings.NewReader(`{"message":" "}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func assertSSEContains(t *testing.T, recorder *httptest.ResponseRecorder, expected ...string) {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type=%q", contentType)
	}
	for _, value := range expected {
		if !strings.Contains(recorder.Body.String(), value) {
			t.Fatalf("body does not contain %q: %s", value, recorder.Body.String())
		}
	}
}
