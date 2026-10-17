package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-api-demo/internal/usage"
)

type fakeUsageReader struct {
	snapshot usage.Snapshot
	err      error
	limit    int
}

func (r *fakeUsageReader) Snapshot(_ context.Context, limit int) (usage.Snapshot, error) {
	r.limit = limit
	return r.snapshot, r.err
}

func (r *fakeUsageReader) Ping(context.Context) error { return r.err }

func TestGetUsageReturnsSummaryAndRecords(t *testing.T) {
	reader := &fakeUsageReader{snapshot: usage.Snapshot{
		Summary: usage.Summary{TotalCalls: 2, InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
		Records: []usage.Record{{
			ID: "event-1", Model: "test-model", Operation: "responses", TotalTokens: 15, Status: usage.StatusCompleted,
		}},
	}}
	handler := NewPlatformHandler(
		fakeGenerator{}, newTestConversationStore(), nil, nil, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)), WithUsageReader(reader),
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/usage?limit=12", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
	if reader.limit != 12 || !strings.Contains(recorder.Body.String(), `"total_tokens":15`) || !strings.Contains(recorder.Body.String(), `"model":"test-model"`) {
		t.Fatalf("limit=%d; body=%s", reader.limit, recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", recorder.Header().Get("Cache-Control"))
	}
}

func TestGetUsageRejectsInvalidLimit(t *testing.T) {
	reader := &fakeUsageReader{}
	handler := NewPlatformHandler(
		fakeGenerator{}, newTestConversationStore(), nil, nil, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)), WithUsageReader(reader),
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/usage?limit=101", nil))

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "invalid_request") {
		t.Fatalf("status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGetUsageHandlesStoreFailure(t *testing.T) {
	reader := &fakeUsageReader{err: errors.New("database unavailable")}
	handler := NewPlatformHandler(
		fakeGenerator{}, newTestConversationStore(), nil, nil, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)), WithUsageReader(reader),
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/usage", nil))

	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "usage_unavailable") {
		t.Fatalf("status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
}
