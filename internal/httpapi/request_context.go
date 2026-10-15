package httpapi

import (
	"net/http"
	"strings"

	"llm-api-demo/internal/usage"
)

// requestMetadataMiddleware 为一次 HTTP 请求中的检索、模型和 Agent 多轮调用提供统一追踪 ID。
func requestMetadataMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := validCorrelationID(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = usage.NewID("req_")
		}
		traceID := validCorrelationID(r.Header.Get("X-Trace-ID"))
		if traceID == "" {
			traceID = requestID
		}
		w.Header().Set("X-Request-ID", requestID)
		ctx := usage.WithMetadata(r.Context(), usage.Metadata{
			RequestID: requestID,
			TraceID:   traceID,
			Route:     r.URL.Path,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validCorrelationID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 96 {
		return ""
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-_:.", character) {
			continue
		}
		return ""
	}
	return value
}
