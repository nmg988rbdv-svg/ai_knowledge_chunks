package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkflowRouteEndpoint(t *testing.T) {
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow/route", strings.NewReader(`{"input":"请从知识库查找退款规则"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, expected := range []string{`"intent":"rag"`, `"path":"rag"`, `"visited":["normalize","classify","rag"]`} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Fatalf("body does not contain %q: %s", expected, recorder.Body.String())
		}
	}
}
