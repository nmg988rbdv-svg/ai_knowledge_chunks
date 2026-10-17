package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"

	"llm-api-demo/internal/ingest"
	"llm-api-demo/internal/llm"
	"llm-api-demo/internal/rag"
	"llm-api-demo/internal/usage"
	"llm-api-demo/internal/workflow"
)

// 限制客户端请求体，避免超大 JSON 请求占用过多内存。
const (
	maxRequestBodyBytes  = 64 << 10
	maxDocumentBodyBytes = 900 << 10 // 保持在 Kafka 默认 1 MiB 消息限制以内。
)

// Handler 将 HTTP 请求转换为模型调用，并负责响应格式与错误映射。
type Handler struct {
	llm           llm.Generator     // 模型能力接口，隔离 HTTP 层与具体 SDK。
	conversations ConversationStore // Redis 会话状态与跨实例互斥。
	knowledge     *rag.Service      // 文档切块、向量化与相似度检索服务。
	workflow      *workflow.Router  // StateGraph 条件路由器。
	jobs          *ingest.Queue     // Kafka 文档入库队列及 Redis 状态查询。
	usageReader   UsageReader       // PostgreSQL 用量统计与最近调用查询。
	timeout       time.Duration     // 每次模型请求的服务端超时上限。
	logger        *slog.Logger      // 记录内部错误及模型调用元数据。
}

// UsageReader 隔离 HTTP 层与 PostgreSQL 实现，也便于接口测试使用替身。
type UsageReader interface {
	Snapshot(context.Context, int) (usage.Snapshot, error)
	Ping(context.Context) error
}

// HandlerOption 为可选的平台依赖保留向后兼容的注入方式。
type HandlerOption func(*Handler)

// WithUsageReader 启用 Token 用量查询接口与就绪检查。
func WithUsageReader(reader UsageReader) HandlerOption {
	return func(handler *Handler) {
		handler.usageReader = reader
	}
}

// chatRequest 是普通对话和流式对话共用的 JSON 请求格式。
type chatRequest struct {
	Message string `json:"message"`
}

// analyzeRequest 是文本结构化分析接口的 JSON 请求格式。
type analyzeRequest struct {
	Text string `json:"text"`
}

type createConversationResponse struct {
	ConversationID string `json:"conversation_id"`
}

type conversationMessageResponse struct {
	ConversationID string     `json:"conversation_id"`
	Turn           int        `json:"turn"`
	ContextTurns   int        `json:"context_turns"`
	ContextReset   bool       `json:"context_reset"`
	Response       llm.Result `json:"response"`
}

// errorResponse 统一封装所有 JSON 错误响应，保持客户端解析格式稳定。
type errorResponse struct {
	Error errorBody `json:"error"`
}

// errorBody 提供机器可读错误码和适合展示给客户端的说明。
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// NewPlatformHandler 创建一个完整的 HTTP Handler，注册各类路由并注入平台依赖。
func NewPlatformHandler(generator llm.Generator, conversations ConversationStore, knowledge *rag.Service, jobs *ingest.Queue, timeout time.Duration, logger *slog.Logger, options ...HandlerOption) http.Handler {

	// 初始化 Handler 结构体，注入各类平台依赖。
	h := &Handler{
		llm:           generator,
		conversations: conversations,
		knowledge:     knowledge,
		workflow:      workflow.NewRouter(),
		jobs:          jobs,
		timeout:       timeout,
		logger:        logger,
	}
	for _, option := range options {
		option(h)
	}
	// 创建 HTTP 路由器，并注册各类路由处理函数。
	mux := http.NewServeMux()
	// 健康检查路由，确保服务可用性。
	mux.HandleFunc("GET /healthz", h.health)
	// 就绪检查路由，确保依赖服务已初始化。
	mux.HandleFunc("GET /readyz", h.ready)
	// UI 路由，提供前端界面。
	mux.HandleFunc("GET /", h.ui)
	// 会话管理路由，支持创建、追加消息和删除会话。
	mux.HandleFunc("POST /v1/conversations", h.createConversation)
	mux.HandleFunc("POST /v1/conversations/{conversationID}/messages", h.conversationMessage)
	mux.HandleFunc("DELETE /v1/conversations/{conversationID}", h.deleteConversation)
	// 聊天路由，支持普通对话、流式对话和工具调用。
	mux.HandleFunc("POST /v1/assistant/stream", h.assistantStream)
	mux.HandleFunc("POST /v1/chat", h.chat)
	mux.HandleFunc("POST /v1/chat/stream", h.chatStream)
	mux.HandleFunc("POST /v1/chat/tools", h.chatWithTools)
	// 代理与工作流路由，支持运行代理和条件路由。
	mux.HandleFunc("POST /v1/agent/run", h.runAgent)
	mux.HandleFunc("POST /v1/workflow/route", h.routeWorkflow)
	// 文本分析路由，提供结构化分析接口。
	mux.HandleFunc("POST /v1/analyze", h.analyze)
	// 知识库路由，仅在知识库服务可用时注册。
	if knowledge != nil {
		mux.HandleFunc("POST /v1/knowledge/search", h.searchKnowledge)
		mux.HandleFunc("POST /v1/knowledge/ask", h.askKnowledge)
	}
	// 任务队列路由，仅在任务队列可用时注册。
	if jobs != nil {
		mux.HandleFunc("POST /v1/knowledge/jobs", h.submitKnowledgeJob)
		mux.HandleFunc("GET /v1/knowledge/jobs/{jobID}", h.getKnowledgeJob)
	}
	if h.usageReader != nil {
		mux.HandleFunc("GET /v1/usage", h.getUsage)
	}
	return requestMetadataMiddleware(mux)
}

// createConversation 路由，创建一个新的会话并返回会话 ID。
func (h *Handler) createConversation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// 调用会话存储创建新的会话，并返回会话 ID。
	id, err := h.conversations.Create(ctx)
	if err != nil {
		h.logger.Error("create conversation id", "error", err)
		writeJSON(w, http.StatusInternalServerError, newError("conversation_create_failed", "failed to create conversation"))
		return
	}
	// 返回创建的会话 ID 给客户端。
	writeJSON(w, http.StatusCreated, createConversationResponse{ConversationID: id})
}

// conversationMessage 为已有会话追加一条消息， 并在必要时重置上下文窗口。
// 每次请求都需要把上一轮响应 ID 作为 previous_response_id 传给模型，从而形成“上下文链”，
// 这比把所有历史消息逐条拼接更灵活，也更符合 Responses API 的工作方式。
func (h *Handler) conversationMessage(w http.ResponseWriter, r *http.Request) {

	// 从请求路径中提取会话 ID。
	conversationID := r.PathValue("conversationID")

	// 解析请求体，获取用户发送的消息。
	req, err := decodeChatRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	ctx = usage.WithMetadata(ctx, usage.Metadata{ConversationID: conversationID, Route: "conversation"})

	// Redis 锁让多个 API 实例也能串行更新同一会话，避免上下文链分叉。
	unlock, err := h.conversations.Lock(ctx, conversationID, h.timeout+5*time.Second)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, newError("conversation_unavailable", "conversation state is temporarily unavailable"))
		return
	}

	// 确保在函数返回时释放会话锁。
	defer func() {
		unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelUnlock()
		if err := unlock(unlockCtx); err != nil {
			h.logger.Error("unlock conversation", "conversation_id", conversationID, "error", err)
		}
	}()

	// 从会话存储中加载当前会话状态。
	state, err := h.conversations.Get(ctx, conversationID)

	// 如果会话不存在，返回 404 给客户端。
	if errors.Is(err, errConversationNotFound) {
		writeJSON(w, http.StatusNotFound, newError("conversation_not_found", "conversation does not exist"))
		return
	}
	if err != nil {
		h.logger.Error("load conversation", "conversation_id", conversationID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("conversation_unavailable", "conversation state is temporarily unavailable"))
		return
	}

	// 保存当前会话的上一轮响应 ID，以便在必要时传给模型形成上下文链。
	previousResponseID := state.PreviousResponseID
	// 上下文窗口重置的条件是当前会话的 context_turns 达到最大值。
	contextReset := state.ContextTurns >= conversationMaxTurns
	if contextReset {
		// 触发 reset 时，不再继续沿用旧链，这样模型会从新的会话轮次开始，
		// 避免无穷增长的 previous_response_id 造成上下文长度爆炸。
		previousResponseID = ""
	}

	// 调用 LLM 接口生成模型响应，传入用户消息和上一轮响应 ID。
	result, err := h.llm.GenerateInConversation(ctx, req.Message, previousResponseID)
	if err != nil {
		status, code, publicMessage := classifyError(err)
		h.logModelError("llm conversation request failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}
	// 如果触发了上下文重置，清空当前会话的 context_turns。
	if contextReset {
		state.ContextTurns = 0
	}
	// 更新当前会话的上一轮响应 ID 和上下文轮次。
	state.PreviousResponseID = result.ID
	// 保存当前会话的上下文轮次。
	state.TotalTurns++
	// 更新当前会话的上下文轮次。
	state.ContextTurns++

	// 将更新后的会话状态保存到存储中。
	if err := h.conversations.Save(ctx, conversationID, state); err != nil {
		h.logger.Error("save conversation", "conversation_id", conversationID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("conversation_unavailable", "failed to save conversation state"))
		return
	}

	// 记录会话请求完成的日志，包括会话 ID、轮次、上下文轮次、是否触发上下文重置、响应 ID、模型和请求耗时。
	h.logger.Info(
		"llm conversation request completed",
		"conversation_id", conversationID,
		"turn", state.TotalTurns,
		"context_turns", state.ContextTurns,
		"context_reset", contextReset,
		"response_id", result.ID,
		"model", result.Model,
		"duration_ms", result.DurationMS,
	)
	// 将最终的会话状态和模型响应返回给客户端。
	writeJSON(w, http.StatusOK, conversationMessageResponse{
		ConversationID: conversationID,
		Turn:           state.TotalTurns,
		ContextTurns:   state.ContextTurns,
		ContextReset:   contextReset,
		Response:       result,
	})
}

// deleteConversation 处理删除会话的 HTTP 请求，确保会话状态在删除前被锁定，避免并发修改。
func (h *Handler) deleteConversation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	// 从请求路径中提取会话 ID。
	conversationID := r.PathValue("conversationID")
	// 尝试获取会话锁，确保在删除会话期间不会有并发修改。
	unlock, err := h.conversations.Lock(ctx, conversationID, 5*time.Second)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, newError("conversation_unavailable", "conversation state is temporarily unavailable"))
		return
	}
	// 确保在函数返回前释放会话锁，避免死锁。
	defer func() {
		unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelUnlock()
		if err := unlock(unlockCtx); err != nil {
			h.logger.Error("unlock conversation after delete", "conversation_id", conversationID, "error", err)
		}
	}()
	// 调用会话删除接口，尝试删除指定的会话。
	deleted, err := h.conversations.Delete(ctx, conversationID)
	if err != nil {
		h.logger.Error("delete conversation", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, newError("conversation_unavailable", "failed to delete conversation"))
		return
	}
	if !deleted {
		// 如果会话不存在，返回 404 状态码。
		writeJSON(w, http.StatusNotFound, newError("conversation_not_found", "conversation does not exist"))
		return
	}
	// 会话删除成功，返回 204 状态码。
	w.WriteHeader(http.StatusNoContent)
}

// chatWithTools 处理带工具闭环的聊天请求，复用普通聊天的消息校验逻辑，但调用包含工具能力的模型。
func (h *Handler) chatWithTools(w http.ResponseWriter, r *http.Request) {
	// 工具对话复用普通聊天的 message 校验，但调用包含工具闭环的模型能力。
	req, err := decodeChatRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	// 创建带超时的上下文，确保模型调用不会无限阻塞。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用带工具能力的模型进行生成，传入用户消息。
	result, err := h.llm.GenerateWithTools(ctx, req.Message)
	if err != nil {
		// 参数无效、工具执行失败和上游错误都走统一分类，避免泄漏内部错误文本。
		status, code, publicMessage := classifyError(err)
		h.logModelError("llm tool request failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}

	h.logger.Info(
		"llm tool request completed",
		"response_id", result.ID,
		"model", result.Model,
		// 记录实际执行次数，便于区分直接回答与经工具处理的请求。
		"tool_calls", len(result.ToolCalls),
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	writeJSON(w, http.StatusOK, result)
}

// analyze 处理文本分析请求，调用模型的分析接口并返回结果。
func (h *Handler) analyze(w http.ResponseWriter, r *http.Request) {
	// 先验证请求，再创建有超时限制的模型调用上下文。
	req, err := decodeAnalyzeRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用模型的分析接口，传入用户提供的文本。
	result, err := h.llm.Analyze(ctx, req.Text)
	if err != nil {
		// 对外只返回稳定的公共错误信息，详细供应商信息仅写入服务端日志。
		status, code, publicMessage := classifyError(err)
		h.logModelError("llm analysis failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}

	h.logger.Info(
		"llm analysis completed",
		"response_id", result.ID,
		"model", result.Model,
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	writeJSON(w, http.StatusOK, result)
}

// health 路由，提供服务健康检查接口。
func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	// 健康检查只报告 HTTP 服务进程可响应，不探测模型供应商。
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready 路由，提供服务就绪检查接口。
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	checks := map[string]string{"http": "ok"}
	ready := true
	if err := h.conversations.Ping(ctx); err != nil {
		checks["conversation_store"] = "unavailable"
		ready = false
	} else {
		checks["conversation_store"] = "ok"
	}
	if h.knowledge != nil {
		if err := h.knowledge.Ping(ctx); err != nil {
			checks["vector_store"] = "unavailable"
			ready = false
		} else {
			checks["vector_store"] = "ok"
		}
	}
	if h.jobs != nil {
		if err := h.jobs.Ping(ctx); err != nil {
			checks["job_queue"] = "unavailable"
			ready = false
		} else {
			checks["job_queue"] = "ok"
		}
	}
	if h.usageReader != nil {
		if err := h.usageReader.Ping(ctx); err != nil {
			checks["usage_store"] = "unavailable"
			ready = false
		} else {
			checks["usage_store"] = "ok"
		}
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ready": ready, "checks": checks})
}

// chat 处理普通对话请求，等待完整模型结果后一次性返回。
func (h *Handler) chat(w http.ResponseWriter, r *http.Request) {

	// 解码客户端请求，提取对话消息。
	req, err := decodeChatRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用 LLM 模型生成响应，等待完整结果返回。
	result, err := h.llm.Generate(ctx, req.Message)
	if err != nil {
		status, code, publicMessage := classifyError(err)
		h.logModelError("llm request failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}

	// 记录模型调用完成的日志，并返回最终结果给客户端。
	h.logger.Info(
		"llm request completed",
		"response_id", result.ID,
		"model", result.Model,
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	writeJSON(w, http.StatusOK, result)
}

// chatStream 处理增量流式对话请求，逐段返回模型生成的内容。
func (h *Handler) chatStream(w http.ResponseWriter, r *http.Request) {

	// 解码客户端请求，提取对话消息。
	req, err := decodeChatRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}

	// 检查客户端是否支持流式响应。
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, newError("streaming_unsupported", "streaming is not supported by this server"))
		return
	}

	// 关闭代理缓存并保持连接，使模型生成的片段能及时到达客户端，同时设置 SSE 响应头。
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // 立即刷新响应，使客户端能够尽快接收到 SSE 事件。

	// SSE 响应头发送后不能再更改 HTTP 状态码，后续错误通过 error 事件返回。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用 LLM 模型进行流式生成，逐段返回内容给客户端。
	result, err := h.llm.Stream(ctx, req.Message, func(delta string) error {
		// 每个 delta 独立发送并立即刷新，降低客户端看到新文本的延迟。
		if err := writeSSE(w, "delta", map[string]string{"text": delta}); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	})
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		// 将流式生成的错误通过 SSE 返回给客户端。
		_, code, publicMessage := classifyError(err)
		h.logModelError("llm stream failed", code, err)
		_ = writeSSE(w, "error", newError(code, publicMessage))
		flusher.Flush()
		return
	}
	// 记录模型流式生成完成的日志，并通过 SSE 通知客户端完成。
	h.logger.Info(
		"llm stream completed",
		"response_id", result.ID,
		"model", result.Model,
		"duration_ms", result.DurationMS,
		"input_tokens", result.Usage.InputTokens,
		"output_tokens", result.Usage.OutputTokens,
	)
	_ = writeSSE(w, "done", result)
	flusher.Flush()
}

// decodeChatRequest 解析聊天请求，确保消息字段存在且非空。
func decodeChatRequest(w http.ResponseWriter, r *http.Request) (chatRequest, error) {
	// 具体请求类型在此处完成业务字段的清理和非空校验。
	var req chatRequest
	if err := decodeJSONRequest(w, r, &req); err != nil {
		return chatRequest{}, errors.New("request body must be valid JSON with a message field")
	}

	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		return chatRequest{}, errors.New("message must not be empty")
	}

	return req, nil
}

// decodeAnalyzeRequest 解析文本分析请求，确保文本字段存在且非空。
func decodeAnalyzeRequest(w http.ResponseWriter, r *http.Request) (analyzeRequest, error) {
	var req analyzeRequest
	if err := decodeJSONRequest(w, r, &req); err != nil {
		return analyzeRequest{}, errors.New("request body must be valid JSON with a text field")
	}

	req.Text = strings.TrimSpace(req.Text)
	if req.Text == "" {
		return analyzeRequest{}, errors.New("text must not be empty")
	}

	return req, nil
}

// decodeJSONRequest 解析通用 JSON 请求，限制请求体大小并确保只有一个 JSON 值。
func decodeJSONRequest(w http.ResponseWriter, r *http.Request, destination any) error {
	return decodeJSONRequestLimit(w, r, destination, maxRequestBodyBytes)
}

// decodeJSONRequestLimit 解析通用 JSON 请求，限制请求体大小并确保只有一个 JSON 值。
func decodeJSONRequestLimit(w http.ResponseWriter, r *http.Request, destination any, limit int64) error {
	// MaxBytesReader 在读取超限时返回错误；关闭请求体可及时释放底层资源。
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	// 拒绝拼错或不支持的字段，避免客户端误以为这些字段已生效。
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return ensureSingleJSONValue(decoder)
}

// ensureSingleJSONValue 确保 JSON 解码器在解析完第一个值后没有额外的 JSON 值。
func ensureSingleJSONValue(decoder *json.Decoder) error {
	// 第一次 Decode 成功并不保证请求体结束，继续解码以拒绝尾随 JSON 值。
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

// classifyError 将内部取消、超时和上游 API 错误映射为稳定的客户端错误。
func classifyError(err error) (status int, code, message string) {
	// 先检查上下文错误，避免超时/取消被归类成普通上游故障。
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "llm_timeout", "the model request timed out"
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, "request_canceled", "the request was canceled"
	}

	// 流式 API 的错误可能通过事件载荷返回，适配层会将其规范化为 ProviderError。
	var providerErr *llm.ProviderError
	if errors.As(err, &providerErr) {
		return classifyProviderError(providerErr.Code)
	}

	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		if apiErr.Code != "" {
			return classifyProviderError(apiErr.Code)
		}
		switch apiErr.StatusCode {
		case http.StatusTooManyRequests:
			return http.StatusServiceUnavailable, "llm_rate_limited", "the model service is temporarily busy"
		case http.StatusBadRequest:
			return http.StatusBadGateway, "llm_bad_request", "the model rejected the request"
		case http.StatusUnauthorized, http.StatusForbidden:
			return http.StatusBadGateway, "llm_auth_error", "the model service is not configured correctly"
		default:
			return http.StatusBadGateway, "llm_upstream_error", "the model service returned an error"
		}
	}

	return http.StatusBadGateway, "llm_upstream_error", "failed to call the model service"
}

func classifyProviderError(providerCode string) (status int, code, message string) {
	// 将供应商错误码转换为服务自己的稳定错误码和 HTTP 状态码。
	switch providerCode {
	case "credit_balance_exhausted":
		return http.StatusPaymentRequired, "llm_credit_balance_exhausted", "the model account has no available API credits"
	case "organization_spend_limit_exceeded", "project_spend_limit_exceeded":
		return http.StatusServiceUnavailable, "llm_spend_limit_exceeded", "the model account reached its configured spend limit"
	case "organization_usage_limit_exceeded":
		return http.StatusServiceUnavailable, "llm_usage_limit_exceeded", "the model account reached its usage limit"
	case "rate_limit_exceeded", "slow_down":
		return http.StatusServiceUnavailable, "llm_rate_limited", "the model service is temporarily rate limited"
	case "invalid_api_key", "invalid_authentication", "authentication_error":
		return http.StatusBadGateway, "llm_auth_error", "the model service credentials are invalid"
	case "model_not_found":
		return http.StatusBadGateway, "llm_model_unavailable", "the configured model is unavailable to this project"
	case "response_incomplete":
		return http.StatusBadGateway, "llm_response_incomplete", "the model response ended before completion"
	case "invalid_structured_output":
		return http.StatusBadGateway, "llm_invalid_structured_output", "the model returned an invalid structured response"
	case "invalid_tool_arguments":
		return http.StatusBadGateway, "llm_invalid_tool_call", "the model returned invalid tool arguments"
	case "unsupported_tool":
		return http.StatusBadGateway, "llm_unsupported_tool", "the model requested an unsupported tool"
	case "tool_execution_failed":
		return http.StatusBadGateway, "llm_tool_execution_failed", "the requested tool could not complete"
	case "empty_tool_response":
		return http.StatusBadGateway, "llm_empty_tool_response", "the model returned no answer after the tool call"
	case "empty_grounded_response":
		return http.StatusBadGateway, "llm_empty_grounded_response", "the model returned no grounded answer"
	case "agent_step_limit":
		return http.StatusBadGateway, "agent_step_limit", "the agent exceeded its tool-call limit"
	default:
		return http.StatusBadGateway, "llm_upstream_error", "the model service returned an error"
	}
}

func (h *Handler) logModelError(message, code string, err error) {
	// 公共响应不暴露内部错误细节；日志保留供应商字段以便运维排查。
	attrs := []any{"code", code, "error", err}

	var providerErr *llm.ProviderError
	if errors.As(err, &providerErr) {
		attrs = append(attrs, "provider_code", providerErr.Code)
	}
	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		attrs = append(attrs,
			"provider_code", apiErr.Code,
			"provider_type", apiErr.Type,
			"provider_status", apiErr.StatusCode,
		)
	}

	h.logger.Error(message, attrs...)
}

func newError(code, message string) errorResponse {
	// 集中构造错误响应，避免不同路由产生不一致的 JSON 结构。
	return errorResponse{Error: errorBody{Code: code, Message: message}}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	// 先写状态码，再编码 JSON；调用方传入的 value 应为可 JSON 编码的数据。
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write response", "error", err)
	}
}

// writeSSE 向客户端发送 SSE 事件，事件内容为单行 JSON。
func writeSSE(w io.Writer, event string, value any) error {
	// SSE 的 data 内容采用单行 JSON，事件之间以空行分隔。
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}
