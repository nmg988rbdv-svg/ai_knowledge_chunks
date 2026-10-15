package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"llm-api-demo/internal/usage"
)

func TestRequestMetadataMiddleware(t *testing.T) {
	handler := requestMetadataMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata := usage.MetadataFromContext(r.Context())
		if metadata.RequestID != "request-123" || metadata.TraceID != "trace-456" {
			t.Fatalf("unexpected metadata: %#v", metadata)
		}
		if metadata.Route != "/v1/chat" {
			t.Fatalf("route = %q", metadata.Route)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodPost, "/v1/chat", nil)
	request.Header.Set("X-Request-ID", "request-123")
	request.Header.Set("X-Trace-ID", "trace-456")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
	if recorder.Header().Get("X-Request-ID") != "request-123" {
		t.Fatalf("X-Request-ID = %q", recorder.Header().Get("X-Request-ID"))
	}
}

func TestRequestMetadataMiddlewareRejectsInvalidClientID(t *testing.T) {
	handler := requestMetadataMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		metadata := usage.MetadataFromContext(r.Context())
		if metadata.RequestID == "invalid request id" || metadata.RequestID == "" {
			t.Fatalf("request ID was not regenerated: %q", metadata.RequestID)
		}
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Request-ID", "invalid request id")
	handler.ServeHTTP(httptest.NewRecorder(), request)
}
