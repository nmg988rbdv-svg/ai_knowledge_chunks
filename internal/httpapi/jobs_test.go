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

	"llm-api-demo/internal/ingest"
)

type jobsEmbedder struct{}

func (jobsEmbedder) Embed(_ context.Context, texts []string) ([][]float64, error) {
	vectors := make([][]float64, len(texts))
	for i := range texts {
		vectors[i] = []float64{1, float64(i + 1)}
	}
	return vectors, nil
}

func newJobsTestHandler(t *testing.T) http.Handler {
	t.Helper()
	knowledge := newTestKnowledgeService(t, jobsEmbedder{})
	statuses := newTestJobStatusStore()
	processor, err := ingest.NewProcessor(statuses, knowledge.AddDocument)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := ingest.NewQueue(&testJobPublisher{handle: processor.Handle}, statuses)
	if err != nil {
		t.Fatal(err)
	}
	return NewPlatformHandler(
		fakeGenerator{}, newTestConversationStore(), knowledge, queue, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestKnowledgeJobLifecycle(t *testing.T) {
	handler := newJobsTestHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/v1/knowledge/jobs", strings.NewReader(`{"document_id":"guide","text":"Go uses goroutines."}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var queued ingest.JobStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &queued); err != nil {
		t.Fatal(err)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/knowledge/jobs/"+queued.ID, nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var completed ingest.JobStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &completed); err != nil {
		t.Fatal(err)
	}
	if completed.State != ingest.StateCompleted || completed.Chunks < 1 {
		t.Fatalf("completed=%#v", completed)
	}
}

func TestKnowledgeJobNotFound(t *testing.T) {
	handler := newJobsTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/knowledge/jobs/missing", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "job_not_found") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestReadinessIncludesPlatformDependencies(t *testing.T) {
	handler := newJobsTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, expected := range []string{`"ready":true`, `"conversation_store":"ok"`, `"vector_store":"ok"`, `"job_queue":"ok"`} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Fatalf("body missing %s: %s", expected, recorder.Body.String())
		}
	}
}
