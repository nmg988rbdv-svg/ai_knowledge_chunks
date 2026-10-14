package httpapi

import (
	"context"
	"net/http"

	"llm-api-demo/internal/llm"
)

const assistantRAGTopK = 3

type assistantRouteEvent struct {
	Path string `json:"path"`
}

type assistantDoneEvent struct {
	Route      string    `json:"route"`
	ID         string    `json:"id"`
	Model      string    `json:"model"`
	Usage      llm.Usage `json:"usage"`
	DurationMS int64     `json:"duration_ms"`
}

type assistantToolsEvent struct {
	ModelCalls int                 `json:"model_calls"`
	ToolCalls  []llm.ToolCallTrace `json:"tool_calls"`
}

// assistantStream 根据输入选择普通问答、RAG 或 Agent，并通过统一 SSE 协议返回结果。
func (h *Handler) assistantStream(w http.ResponseWriter, r *http.Request) {

	// 解析并验证聊天请求。
	req, err := decodeChatRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}
	// 确保响应支持 SSE 流式传输。
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, newError("streaming_unsupported", "streaming is not supported by this server"))
		return
	}

	// 创建带超时的上下文，用于后续的工作流路由和知识库搜索请求。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用工作流路由，确定请求应走的路径。
	route, err := h.workflow.Route(ctx, req.Message)
	if err != nil {
		// 记录路由错误日志，返回给客户端。
		status, code, message := classifyWorkflowError(err)
		h.logger.Error("route assistant request", "code", code, "error", err)
		writeJSON(w, status, newError(code, message))
		return
	}

	// 初始化 SSE 连接，设置必要的 HTTP 头部信息。
	startSSE(w)
	if err := writeSSE(w, "route", assistantRouteEvent{Path: route.Path}); err != nil {
		return
	}
	flusher.Flush()

	// 根据路由路径选择不同的处理逻辑。
	switch route.Path {
	case "rag":
		h.streamAssistantRAG(ctx, w, flusher, req.Message)
	case "agent":
		h.streamAssistantAgent(ctx, w, flusher, req.Message)
	default:
		h.streamAssistantChat(ctx, w, flusher, req.Message)
	}
}

// streamAssistantChat 处理普通聊天请求，调用 LLM 生成回答并通过 SSE 返回。
func (h *Handler) streamAssistantChat(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, message string) {
	// 调用 LLM 生成回答，并通过 SSE 返回增量结果。
	result, err := h.llm.Stream(ctx, message, sseDeltaWriter(w, flusher))
	if err != nil {
		h.writeAssistantModelError(w, flusher, "assistant chat failed", err)
		return
	}
	// 聊天完成，通知客户端最终结果。
	h.writeAssistantDone(w, flusher, "chat", result)
}

// streamAssistantRAG 处理基于知识库的问答请求，先检索相关知识，再调用 LLM 生成答案。
func (h *Handler) streamAssistantRAG(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, question string) {

	// 检查知识库是否可用。
	if h.knowledge == nil {
		writeAssistantError(w, flusher, "knowledge_unavailable", "the knowledge base is unavailable")
		return
	}
	// 在知识库中搜索与问题相关的内容。
	matches, err := h.knowledge.Search(ctx, question, assistantRAGTopK)
	if err != nil {
		_, code, message := classifyKnowledgeError(err)
		h.logger.Error("retrieve assistant knowledge", "code", code, "error", err)
		writeAssistantError(w, flusher, code, message)
		return
	}
	// 如果没有找到匹配的知识，返回错误。
	if len(matches) == 0 {
		writeAssistantError(w, flusher, "knowledge_not_found", "the knowledge base contains no searchable content")
		return
	}

	// 构建知识来源列表，用于在生成答案时提供引用信息。
	sources, groundingSources := buildKnowledgeSources(matches)
	if err := writeSSE(w, "sources", sources); err != nil {
		return
	}
	flusher.Flush()

	// 调用 LLM 生成基于知识来源的答案，并通过 SSE 返回增量结果。
	result, err := h.llm.StreamGrounded(ctx, question, groundingSources, sseDeltaWriter(w, flusher))
	if err != nil {
		h.writeAssistantModelError(w, flusher, "assistant knowledge answer failed", err)
		return
	}
	// 基于知识来源的答案生成完成，通知客户端最终结果。
	h.writeAssistantDone(w, flusher, "rag", result)
}

// streamAssistantAgent 处理基于工具的代理请求，规划并执行工具以完成用户目标。
func (h *Handler) streamAssistantAgent(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, goal string) {
	// 通知客户端代理开始执行。
	if err := writeSSE(w, "status", map[string]string{"message": "Agent 正在规划并执行工具"}); err != nil {
		return
	}
	flusher.Flush()

	// 调用 LLM 执行代理任务，生成工具调用和最终答案。
	result, err := h.llm.GenerateWithTools(ctx, goal)
	if err != nil {
		h.writeAssistantModelError(w, flusher, "assistant agent failed", err)
		return
	}
	// 返回工具调用信息给客户端。
	if err := writeSSE(w, "tools", assistantToolsEvent{ModelCalls: result.ModelCalls, ToolCalls: result.ToolCalls}); err != nil {
		return
	}
	// 返回代理生成的答案增量结果。
	if err := writeSSE(w, "delta", map[string]string{"text": result.Text}); err != nil {
		return
	}
	flusher.Flush()
	// 代理任务完成，通知客户端最终结果。
	h.writeAssistantDone(w, flusher, "agent", llm.Result{
		ID: result.ID, Model: result.Model, Text: result.Text,
		Usage: result.Usage, DurationMS: result.DurationMS,
	})
}

func sseDeltaWriter(w http.ResponseWriter, flusher http.Flusher) func(string) error {
	return func(delta string) error {
		if err := writeSSE(w, "delta", map[string]string{"text": delta}); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
}

// startSSE 初始化 SSE 连接，设置必要的 HTTP 头部信息。
func startSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
}

// writeAssistantDone 记录完成日志并通过 SSE 通知客户端请求已完成。
func (h *Handler) writeAssistantDone(w http.ResponseWriter, flusher http.Flusher, route string, result llm.Result) {
	h.logger.Info(
		"assistant request completed",
		"route", route,
		"response_id", result.ID,
		"model", result.Model,
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	_ = writeSSE(w, "done", assistantDoneEvent{
		Route: route, ID: result.ID, Model: result.Model,
		Usage: result.Usage, DurationMS: result.DurationMS,
	})
	flusher.Flush()
}

// writeAssistantModelError 记录模型错误日志并通过 SSE 返回给客户端。
func (h *Handler) writeAssistantModelError(w http.ResponseWriter, flusher http.Flusher, logMessage string, err error) {
	_, code, publicMessage := classifyError(err)
	h.logModelError(logMessage, code, err)
	writeAssistantError(w, flusher, code, publicMessage)
}

// writeAssistantError 通过 SSE 向客户端返回助手错误信息。
func writeAssistantError(w http.ResponseWriter, flusher http.Flusher, code, message string) {
	_ = writeSSE(w, "error", newError(code, message))
	flusher.Flush()
}
