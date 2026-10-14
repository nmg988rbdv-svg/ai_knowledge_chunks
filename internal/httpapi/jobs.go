package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"llm-api-demo/internal/ingest"
)

// submitKnowledgeJob 处理提交知识库文档的请求，将文档加入到知识库的 ingestion 作业队列中。
func (h *Handler) submitKnowledgeJob(w http.ResponseWriter, r *http.Request) {
	var req addDocumentRequest

	// 解析请求体为 JSON，并限制请求体的最大字节数，以防止过大的文档上传。
	if err := decodeJSONRequestLimit(w, r, &req, maxDocumentBodyBytes); err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "request body must contain document_id and text"))
		return
	}
	// 去除 document_id 和 text 字段的首尾空白字符，并确保它们非空。
	req.DocumentID = strings.TrimSpace(req.DocumentID)
	req.Text = strings.TrimSpace(req.Text)
	if req.DocumentID == "" || req.Text == "" {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "document_id and text must not be empty"))
		return
	}
	// 创建带超时的上下文，用于提交知识库文档的 ingestion 作业。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	// 提交知识库文档的 ingestion 作业到作业队列。
	status, err := h.jobs.Submit(ctx, req.DocumentID, req.Text)
	if err != nil {
		h.logger.Error("submit knowledge ingestion job", "document_id", req.DocumentID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("job_queue_unavailable", "failed to queue document ingestion"))
		return
	}
	writeJSON(w, http.StatusAccepted, status)
}

// getKnowledgeJob 处理查询知识库文档 ingestion 作业状态的请求，根据 jobID 返回作业的当前状态。
func (h *Handler) getKnowledgeJob(w http.ResponseWriter, r *http.Request) {

	// 创建带超时的上下文，用于查询知识库文档 ingestion 作业的状态。
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// 从请求路径中提取 jobID 参数，用于查询对应的知识库文档 ingestion 作业状态。
	status, err := h.jobs.Get(ctx, r.PathValue("jobID"))
	if errors.Is(err, ingest.ErrJobNotFound) {
		writeJSON(w, http.StatusNotFound, newError("job_not_found", "ingestion job does not exist or has expired"))
		return
	}
	if err != nil {
		h.logger.Error("get knowledge ingestion job", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("job_status_unavailable", "failed to load ingestion status"))
		return
	}
	writeJSON(w, http.StatusOK, status)
}
