package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// agentRunRequest 表示代理运行请求的 JSON 体，包含用户的目标（goal）字段。
type agentRunRequest struct {
	Goal string `json:"goal"`
}

// runAgent 处理代理运行请求，调用带工具能力的模型，并返回最终结果。
func (h *Handler) runAgent(w http.ResponseWriter, r *http.Request) {

	// 解析并校验代理运行请求的 JSON 体，确保包含非空的 goal 字段。
	req, err := decodeAgentRunRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	// 创建带超时的上下文，确保代理运行不会无限期阻塞。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用带工具能力的模型，传入用户的目标（goal）字段。
	result, err := h.llm.GenerateWithTools(ctx, req.Goal)
	if err != nil {
		status, code, publicMessage := classifyError(err)
		h.logModelError("agent run failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}

	h.logger.Info(
		"agent run completed",
		"response_id", result.ID,
		"model", result.Model,
		"model_calls", result.ModelCalls,
		"tool_calls", len(result.ToolCalls),
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	writeJSON(w, http.StatusOK, result)
}

// decodeAgentRunRequest 解析并校验代理运行请求的 JSON 体，确保包含非空的 goal 字段。
func decodeAgentRunRequest(w http.ResponseWriter, r *http.Request) (agentRunRequest, error) {
	var req agentRunRequest

	// 解析请求体为 JSON，并校验 goal 字段是否存在且非空。
	if err := decodeJSONRequest(w, r, &req); err != nil {
		return agentRunRequest{}, errors.New("request body must be valid JSON with a goal field")
	}

	// 去除 goal 字段的首尾空白字符，并确保其非空。
	req.Goal = strings.TrimSpace(req.Goal)
	if req.Goal == "" {
		return agentRunRequest{}, errors.New("goal must not be empty")
	}
	return req, nil
}
