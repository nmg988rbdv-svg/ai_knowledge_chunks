package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-api-demo/internal/llm"
)

type fakeGenerator struct {
	result           llm.Result
	analysisResult   llm.AnalysisResult
	toolResult       llm.ToolResult
	err              error
	deltas           []string
	conversationFn   func(context.Context, string, string) (llm.Result, error)
	groundedFn       func(context.Context, string, []llm.GroundingSource) (llm.Result, error)
	groundedStreamFn func(context.Context, string, []llm.GroundingSource, func(string) error) (llm.Result, error)
}

// Generate 返回测试预设结果，不访问真实模型服务。
func (f fakeGenerator) Generate(context.Context, string) (llm.Result, error) {
	return f.result, f.err
}

// GenerateGrounded 允许知识库测试检查实际传入模型的问题和编号来源。
func (f fakeGenerator) GenerateGrounded(ctx context.Context, question string, sources []llm.GroundingSource) (llm.Result, error) {
	if f.groundedFn != nil {
		return f.groundedFn(ctx, question, sources)
	}
	return f.result, f.err
}

// StreamGrounded 模拟基于检索来源的增量回答。
func (f fakeGenerator) StreamGrounded(ctx context.Context, question string, sources []llm.GroundingSource, onDelta func(string) error) (llm.Result, error) {
	if f.groundedStreamFn != nil {
		return f.groundedStreamFn(ctx, question, sources, onDelta)
	}
	for _, delta := range f.deltas {
		if err := onDelta(delta); err != nil {
			return llm.Result{}, err
		}
	}
	return f.result, f.err
}

// GenerateInConversation 可通过回调检查每轮传入的 previous_response_id。
func (f fakeGenerator) GenerateInConversation(ctx context.Context, message, previousResponseID string) (llm.Result, error) {
	if f.conversationFn != nil {
		return f.conversationFn(ctx, message, previousResponseID)
	}
	return f.result, f.err
}

// Stream 按预设顺序触发增量回调，模拟模型的分段输出。
func (f fakeGenerator) Stream(_ context.Context, _ string, onDelta func(string) error) (llm.Result, error) {
	for _, delta := range f.deltas {
		if err := onDelta(delta); err != nil {
			return llm.Result{}, err
		}
	}
	return f.result, f.err
}

// Analyze 返回预设的结构化分析结果，供 HTTP 层测试使用。
func (f fakeGenerator) Analyze(context.Context, string) (llm.AnalysisResult, error) {
	return f.analysisResult, f.err
}

// GenerateWithTools 返回预设的工具调用轨迹和最终回答。
func (f fakeGenerator) GenerateWithTools(context.Context, string) (llm.ToolResult, error) {
	return f.toolResult, f.err
}

func TestConversationCarriesPreviousResponseID(t *testing.T) {
	var previousIDs []string
	call := 0
	generator := fakeGenerator{conversationFn: func(_ context.Context, _ string, previousID string) (llm.Result, error) {
		previousIDs = append(previousIDs, previousID)
		call++
		return llm.Result{ID: fmt.Sprintf("resp_%d", call), Model: "test-model", Text: "ok"}, nil
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	conversationID := createTestConversation(t, handler)
	for _, message := range []string{"我叫小明", "我叫什么？"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/conversations/"+conversationID+"/messages", strings.NewReader(fmt.Sprintf(`{"message":%q}`, message)))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d; body=%s", recorder.Code, recorder.Body.String())
		}
	}

	if len(previousIDs) != 2 || previousIDs[0] != "" || previousIDs[1] != "resp_1" {
		t.Fatalf("previous response IDs = %#v, want [\"\", \"resp_1\"]", previousIDs)
	}
}

func TestConversationResetsContextAfterTurnLimit(t *testing.T) {
	var previousIDs []string
	call := 0
	generator := fakeGenerator{conversationFn: func(_ context.Context, _ string, previousID string) (llm.Result, error) {
		previousIDs = append(previousIDs, previousID)
		call++
		return llm.Result{ID: fmt.Sprintf("resp_%d", call), Model: "test-model", Text: "ok"}, nil
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	conversationID := createTestConversation(t, handler)

	var lastBody string
	for i := 0; i < conversationMaxTurns+1; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/conversations/"+conversationID+"/messages", strings.NewReader(`{"message":"继续"}`))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("turn %d status = %d; body=%s", i+1, recorder.Code, recorder.Body.String())
		}
		lastBody = recorder.Body.String()
	}

	if got := previousIDs[conversationMaxTurns]; got != "" {
		t.Fatalf("previous ID after limit = %q, want empty", got)
	}
	if !strings.Contains(lastBody, `"context_reset":true`) || !strings.Contains(lastBody, `"context_turns":1`) {
		t.Fatalf("unexpected reset response: %s", lastBody)
	}
}

func TestConversationNotFound(t *testing.T) {
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/conversations/conv_missing/messages", strings.NewReader(`{"message":"你好"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "conversation_not_found") {
		t.Fatalf("status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
}

func createTestConversation(t *testing.T, handler http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/conversations", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create conversation status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
	var response createConversationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode create conversation response: %v", err)
	}
	if response.ConversationID == "" {
		t.Fatal("conversation ID is empty")
	}
	return response.ConversationID
}

func TestChatWithToolsSuccess(t *testing.T) {
	// 验证工具对话成功响应包含最终文本、工具名称及计算结果轨迹。
	generator := fakeGenerator{toolResult: llm.ToolResult{
		ID:    "resp_tool",
		Model: "test-model",
		Text:  "12 乘以 7 等于 84。",
		ToolCalls: []llm.ToolCallTrace{{
			Name:      "calculate",
			Arguments: []byte(`{"operation":"multiply","a":12,"b":7}`),
			Output:    []byte(`{"operation":"multiply","a":12,"b":7,"result":84}`),
		}},
		Usage: llm.Usage{InputTokens: 20, OutputTokens: 15, TotalTokens: 35},
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/tools", strings.NewReader(`{"message":"12 乘以 7 是多少？"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	for _, expected := range []string{`"text":"12 乘以 7 等于 84。"`, `"name":"calculate"`, `"result":84`} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Fatalf("body does not contain %q: %s", expected, recorder.Body.String())
		}
	}
}

func TestChatWithToolsMapsExecutionError(t *testing.T) {
	// 工具执行失败应返回网关错误，并映射为稳定的公开错误码。
	handler := newTestHandler(
		fakeGenerator{err: &llm.ProviderError{Code: "tool_execution_failed"}},
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/tools", strings.NewReader(`{"message":"1 除以 0"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "llm_tool_execution_failed") {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestAnalyzeSuccess(t *testing.T) {
	// 验证分析路由能将结构化结果及 token 元数据编码为 JSON。
	generator := fakeGenerator{analysisResult: llm.AnalysisResult{
		ID:    "resp_analysis",
		Model: "test-model",
		Analysis: llm.Analysis{
			Summary:   "Go 服务接入大语言模型。",
			Sentiment: "neutral",
			Keywords:  []string{"Go", "LLM"},
		},
		Usage: llm.Usage{InputTokens: 8, OutputTokens: 12, TotalTokens: 20},
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/analyze", strings.NewReader(`{"text":"使用 Go 构建 LLM 服务"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	for _, expected := range []string{`"summary":"Go 服务接入大语言模型。"`, `"sentiment":"neutral"`, `"keywords":["Go","LLM"]`} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Fatalf("body does not contain %q: %s", expected, recorder.Body.String())
		}
	}
}

func TestAnalyzeRejectsEmptyText(t *testing.T) {
	// 纯空白文本应在调用模型前被判定为无效请求。
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/analyze", strings.NewReader(`{"text":"  "}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestAnalyzeMapsInvalidStructuredOutput(t *testing.T) {
	// 无效的模型结构化输出应映射为网关错误和稳定错误码。
	handler := newTestHandler(
		fakeGenerator{err: &llm.ProviderError{Code: "invalid_structured_output"}},
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/analyze", strings.NewReader(`{"text":"hello"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "llm_invalid_structured_output") {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestChatSuccess(t *testing.T) {
	// 验证普通对话成功时返回模型生成的文本。
	generator := fakeGenerator{result: llm.Result{
		ID: "resp_test", Model: "test-model", Text: "你好！",
		Usage: llm.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
	}}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"你好"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"text":"你好！"`) {
		t.Fatalf("unexpected body: %s", recorder.Body.String())
	}
}

func TestChatRejectsEmptyMessage(t *testing.T) {
	// 去除首尾空白后为空的消息不能进入模型调用。
	handler := newTestHandler(fakeGenerator{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"  "}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestChatMapsTimeout(t *testing.T) {
	// 模型调用超时应映射为 504，而不是通用上游错误。
	handler := newTestHandler(fakeGenerator{err: context.DeadlineExceeded}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"hello"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
}

func TestChatMapsUnknownUpstreamError(t *testing.T) {
	// 未识别的模型错误应统一映射为 502。
	handler := newTestHandler(fakeGenerator{err: errors.New("network failed")}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"hello"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
}

func TestChatStreamSuccess(t *testing.T) {
	// 验证 SSE 响应包含逐段 delta 事件和最终 done 事件。
	generator := fakeGenerator{
		deltas: []string{"你", "好"},
		result: llm.Result{
			ID: "resp_stream", Model: "test-model", Text: "你好",
			Usage: llm.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
		},
	}
	handler := newTestHandler(generator, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/stream", strings.NewReader(`{"message":"你好"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := recorder.Body.String()
	for _, expected := range []string{
		`event: delta`,
		`data: {"text":"你"}`,
		`data: {"text":"好"}`,
		`event: done`,
		`"id":"resp_stream"`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("body does not contain %q: %s", expected, body)
		}
	}
}

func TestChatStreamEmitsErrorEvent(t *testing.T) {
	// 流响应头已发送后发生的模型错误通过 SSE error 事件传递。
	handler := newTestHandler(
		fakeGenerator{err: context.DeadlineExceeded},
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/stream", strings.NewReader(`{"message":"hello"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "event: error") || !strings.Contains(body, "llm_timeout") {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestChatStreamClassifiesCreditBalanceError(t *testing.T) {
	// 验证流式供应商错误也会映射为面向客户端的额度错误码。
	handler := newTestHandler(
		fakeGenerator{err: &llm.ProviderError{Code: "credit_balance_exhausted"}},
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/stream", strings.NewReader(`{"message":"hello"}`))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if body := recorder.Body.String(); !strings.Contains(body, "llm_credit_balance_exhausted") {
		t.Fatalf("unexpected body: %s", body)
	}
}
