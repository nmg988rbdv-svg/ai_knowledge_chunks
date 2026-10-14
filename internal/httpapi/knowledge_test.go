package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-api-demo/internal/llm"
)

type fakeKnowledgeEmbedder struct{}

func (fakeKnowledgeEmbedder) Embed(_ context.Context, texts []string) ([][]float64, error) {
	vectors := make([][]float64, len(texts))
	for index, text := range texts {
		if strings.Contains(strings.ToLower(text), "go") || strings.Contains(text, "并发") {
			vectors[index] = []float64{1, 0}
		} else {
			vectors[index] = []float64{0, 1}
		}
	}
	return vectors, nil
}

func TestKnowledgeIndexAndSearch(t *testing.T) {
	knowledge := newTestKnowledgeService(t, fakeKnowledgeEmbedder{})
	handler := newTestHandlerWithKnowledge(
		fakeGenerator{},
		knowledge,
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if _, err := knowledge.AddDocument(context.Background(), "go-guide", "Go 使用 goroutine 和 channel 实现并发编程。"); err != nil {
		t.Fatal(err)
	}

	searchRequest := httptest.NewRequest(http.MethodPost, "/v1/knowledge/search", strings.NewReader(`{
		"query":"Go 如何处理并发？",
		"top_k":1
	}`))
	searchRecorder := httptest.NewRecorder()
	handler.ServeHTTP(searchRecorder, searchRequest)
	if searchRecorder.Code != http.StatusOK {
		t.Fatalf("search status=%d body=%s", searchRecorder.Code, searchRecorder.Body.String())
	}
	for _, expected := range []string{`"document_id":"go-guide"`, `"score":1`, `"chunk_index":0`} {
		if !strings.Contains(searchRecorder.Body.String(), expected) {
			t.Fatalf("search body does not contain %q: %s", expected, searchRecorder.Body.String())
		}
	}
}

func TestKnowledgeSearchRejectsInvalidTopK(t *testing.T) {
	knowledge := newTestKnowledgeService(t, fakeKnowledgeEmbedder{})
	handler := newTestHandlerWithKnowledge(
		fakeGenerator{}, knowledge, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/knowledge/search", strings.NewReader(`{"query":"Go","top_k":11}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestKnowledgeAskRetrievesSourcesAndGeneratesAnswer(t *testing.T) {
	knowledge := newTestKnowledgeService(t, fakeKnowledgeEmbedder{})
	var receivedQuestion string
	var receivedSources []llm.GroundingSource
	generator := fakeGenerator{groundedFn: func(_ context.Context, question string, sources []llm.GroundingSource) (llm.Result, error) {
		receivedQuestion = question
		receivedSources = append([]llm.GroundingSource(nil), sources...)
		return llm.Result{
			ID:    "resp_rag",
			Model: "test-model",
			Text:  "Go 使用 goroutine 和 channel 实现并发。[1]",
			Usage: llm.Usage{InputTokens: 30, OutputTokens: 12, TotalTokens: 42},
		}, nil
	}}
	handler := newTestHandlerWithKnowledge(
		generator, knowledge, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if _, err := knowledge.AddDocument(context.Background(), "go-guide", "Go 使用 goroutine 和 channel 实现并发编程。"); err != nil {
		t.Fatal(err)
	}

	askRequest := httptest.NewRequest(http.MethodPost, "/v1/knowledge/ask", strings.NewReader(`{
		"query":"Go 如何处理并发？",
		"top_k":1
	}`))
	askRecorder := httptest.NewRecorder()
	handler.ServeHTTP(askRecorder, askRequest)

	if askRecorder.Code != http.StatusOK {
		t.Fatalf("ask status=%d body=%s", askRecorder.Code, askRecorder.Body.String())
	}
	if receivedQuestion != "Go 如何处理并发？" {
		t.Fatalf("question=%q", receivedQuestion)
	}
	if len(receivedSources) != 1 || receivedSources[0].Reference != 1 || receivedSources[0].DocumentID != "go-guide" {
		t.Fatalf("sources=%#v", receivedSources)
	}
	for _, expected := range []string{
		`"text":"Go 使用 goroutine 和 channel 实现并发。[1]"`,
		`"reference":1`,
		`"document_id":"go-guide"`,
	} {
		if !strings.Contains(askRecorder.Body.String(), expected) {
			t.Fatalf("ask body does not contain %q: %s", expected, askRecorder.Body.String())
		}
	}
}

func TestKnowledgeAskDoesNotCallModelWithoutSources(t *testing.T) {
	knowledge := newTestKnowledgeService(t, fakeKnowledgeEmbedder{})
	modelCalled := false
	generator := fakeGenerator{groundedFn: func(context.Context, string, []llm.GroundingSource) (llm.Result, error) {
		modelCalled = true
		return llm.Result{}, nil
	}}
	handler := newTestHandlerWithKnowledge(
		generator, knowledge, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/knowledge/ask", strings.NewReader(`{"query":"未知问题"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "knowledge_not_found") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if modelCalled {
		t.Fatal("model must not be called when the knowledge base is empty")
	}
}
