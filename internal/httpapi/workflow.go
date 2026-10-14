package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"llm-api-demo/internal/workflow"
)

// workflowRouteRequest 表示工作流路由请求的 JSON 体，包含用户输入的字段。
type workflowRouteRequest struct {
	Input string `json:"input"`
}

// routeWorkflow 处理工作流路由请求，解析输入并调用工作流引擎，返回最终结果或错误信息。
func (h *Handler) routeWorkflow(w http.ResponseWriter, r *http.Request) {
	var req workflowRouteRequest
	// 解析请求体为 JSON，并校验 input 字段是否存在且非空。
	if err := decodeJSONRequest(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "request body must be valid JSON with an input field"))
		return
	}
	// 去除 input 字段的首尾空白字符，并确保其非空。
	req.Input = strings.TrimSpace(req.Input)
	if req.Input == "" {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "input must not be empty"))
		return
	}
	// 调用工作流引擎的路由方法，传入用户的输入字段。
	result, err := h.workflow.Route(r.Context(), req.Input)
	if err != nil {
		status, code, message := classifyWorkflowError(err)
		h.logger.Error("route workflow", "code", code, "error", err)
		writeJSON(w, status, newError(code, message))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// classifyWorkflowError 根据不同的错误类型返回对应的 HTTP 状态码、错误代码和错误信息。
func classifyWorkflowError(err error) (int, string, string) {
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, "request_canceled", "the request was canceled"
	}
	if errors.Is(err, workflow.ErrStepLimit) {
		return http.StatusInternalServerError, "workflow_step_limit", "the workflow exceeded its step limit"
	}
	return http.StatusInternalServerError, "workflow_failed", "the workflow could not complete"
}
