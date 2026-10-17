package httpapi

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

const (
	defaultUsageLimit = 30
	maxUsageLimit     = 100
)

// getUsage 返回累计 Token 用量与最近调用明细，供同源管理界面展示。
func (h *Handler) getUsage(w http.ResponseWriter, r *http.Request) {
	limit := defaultUsageLimit
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > maxUsageLimit {
			writeJSON(w, http.StatusBadRequest, newError("invalid_request", "limit must be an integer between 1 and 100"))
			return
		}
		limit = parsed
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	snapshot, err := h.usageReader.Snapshot(ctx, limit)
	if err != nil {
		h.logger.Error("query usage records", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("usage_unavailable", "usage data is temporarily unavailable"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, snapshot)
}
