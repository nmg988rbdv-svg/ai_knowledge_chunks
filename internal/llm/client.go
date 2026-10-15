package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	usageevent "llm-api-demo/internal/usage"
)

const systemPrompt = "You are a concise and helpful assistant. Reply in the same language as the user."

const analysisPrompt = "Analyze the user's text. Keep the summary concise, classify sentiment as positive, neutral, or negative, and extract the most useful keywords."

// textAnalysisSchema 限制结构化分析响应的字段、类型和允许的情感标签。
// Strict 模式下模型必须返回全部字段，且不能额外添加未声明字段。
var textAnalysisSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"summary": map[string]any{
			"type":        "string",
			"description": "A concise summary in the same language as the input text.",
		},
		"sentiment": map[string]any{
			"type": "string",
			"enum": []string{"positive", "neutral", "negative"},
		},
		"keywords": map[string]any{
			"type": "array",
			"items": map[string]any{
				"type": "string",
			},
		},
	},
	"required":             []string{"summary", "sentiment", "keywords"},
	"additionalProperties": false,
}

// Usage 汇总一次模型调用的 token 用量。
type Usage struct {
	InputTokens       int64 `json:"input_tokens"`        // 输入上下文消耗的 token 数。
	CachedInputTokens int64 `json:"cached_input_tokens"` // 从 Prompt Cache 读取的输入 token 数。
	CacheWriteTokens  int64 `json:"cache_write_tokens"`  // 写入 Prompt Cache 的输入 token 数。
	OutputTokens      int64 `json:"output_tokens"`       // 模型生成内容消耗的 token 数。
	ReasoningTokens   int64 `json:"reasoning_tokens"`    // 输出中用于推理的 token 数。
	TotalTokens       int64 `json:"total_tokens"`        // 本次调用的总 token 数。
}

// Result 是对外返回的模型响应数据。
type Result struct {
	ID         string `json:"id"`          // 供应商生成的响应 ID。
	Model      string `json:"model"`       // 实际处理请求的模型名称。
	Text       string `json:"text"`        // 合并后的完整文本内容。
	Usage      Usage  `json:"usage"`       // 供应商返回的 token 统计。
	DurationMS int64  `json:"duration_ms"` // 从发起请求到完成的耗时，单位为毫秒。
}

// Analysis 是模型按 JSON Schema 返回的强类型文本分析结果。
type Analysis struct {
	Summary   string   `json:"summary"`   // 与输入语言一致的简短摘要。
	Sentiment string   `json:"sentiment"` // 情感分类，只允许 positive/neutral/negative。
	Keywords  []string `json:"keywords"`  // 从文本中提取的关键词。
}

// AnalysisResult 同时保留业务数据和模型调用元数据。
type AnalysisResult struct {
	ID         string   `json:"id"`
	Model      string   `json:"model"`
	Analysis   Analysis `json:"analysis"`
	Usage      Usage    `json:"usage"`
	DurationMS int64    `json:"duration_ms"`
}

// ProviderError 表示模型供应商返回的可识别错误，Code 用于稳定分类。
type ProviderError struct {
	Code    string // 供应商错误码，可能为空。
	Message string // 供应商错误描述，仅供内部诊断使用。
}

// Error 实现 error 接口；优先返回错误码，避免把供应商描述传播到客户端。
func (e *ProviderError) Error() string {
	if e.Code == "" {
		return "model provider returned a streaming error"
	}
	return fmt.Sprintf("model provider error: %s", e.Code)
}

// Generator 抽象普通生成和增量流式生成，便于替换模型客户端及测试。
type Generator interface {
	Generate(ctx context.Context, message string) (Result, error)
	// GenerateGrounded 只根据编号后的检索资料回答问题，并要求答案标注来源。
	GenerateGrounded(ctx context.Context, question string, sources []GroundingSource) (Result, error)
	// StreamGrounded 流式返回严格基于编号检索资料生成的答案。
	StreamGrounded(ctx context.Context, question string, sources []GroundingSource, onDelta func(string) error) (Result, error)
	// GenerateInConversation 使用上一轮响应 ID 延续多轮上下文。
	GenerateInConversation(ctx context.Context, message, previousResponseID string) (Result, error)
	// Stream 在生成过程中逐段调用 onDelta，完成后返回最终响应信息。
	Stream(ctx context.Context, message string, onDelta func(string) error) (Result, error)
	// Analyze 返回符合预定义结构的文本分析结果。
	Analyze(ctx context.Context, text string) (AnalysisResult, error)
	// GenerateWithTools 允许模型选择并调用服务端注册的函数工具。
	GenerateWithTools(ctx context.Context, message string) (ToolResult, error)
}

// OpenAIClient 将项目的生成接口适配到 OpenAI Responses API。
type OpenAIClient struct {
	client         openai.Client
	model          string
	usagePublisher usageevent.Publisher
	logger         *slog.Logger
}

type ClientOption func(*OpenAIClient)

// WithUsagePublisher 让每次真实 Responses API 调用都产生独立的用量事件。
func WithUsagePublisher(publisher usageevent.Publisher, logger *slog.Logger) ClientOption {
	return func(client *OpenAIClient) {
		client.usagePublisher = publisher
		if logger != nil {
			client.logger = logger
		}
	}
}

// NewOpenAIClient 创建带有限次自动重试的 Responses API 客户端。
func NewOpenAIClient(apiKey, model string, options ...ClientOption) *OpenAIClient {
	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithMaxRetries(2),
	)
	result := &OpenAIClient{client: client, model: model, logger: slog.Default()}
	for _, option := range options {
		option(result)
	}
	return result
}

// Generate 调用模型生成响应，等待完整结果返回。
func (c *OpenAIClient) Generate(ctx context.Context, message string) (Result, error) {
	startedAt := time.Now() // 记录请求开始时间，用于计算请求耗时。

	// 调用 OpenAI Responses API 创建新的响应请求。
	response, err := c.client.Responses.New(ctx, responses.ResponseNewParams{
		Model:        shared.ResponsesModel(c.model), // 指定使用的模型
		Instructions: openai.String(systemPrompt),    // 系统指令，指导模型的行为
		Input: responses.ResponseNewParamsInputUnion{ // 用户消息输入
			OfString: openai.String(message), // 用户消息内容
		},
		Store: openai.Bool(false), // 不在供应商侧保存响应，避免本服务请求内容被持久化。
	})
	if err != nil {
		c.publishFailedUsage(ctx, "responses", c.model, 1, startedAt, err)
		return Result{}, err
	}
	c.publishResponseUsage(ctx, *response, 1, startedAt)

	// 将 OpenAI Responses API 的响应转换为统一的 Result 结构，并计算请求耗时。
	return resultFromResponse(*response, startedAt), nil
}

// GenerateInConversation 使用上一轮响应 ID 延续多轮上下文，确保模型能够理解对话历史。
func (c *OpenAIClient) GenerateInConversation(ctx context.Context, message, previousResponseID string) (Result, error) {
	// 记录请求开始时间，用于计算请求耗时。
	startedAt := time.Now()
	// 构建请求参数，包含模型、指令、用户消息以及上一轮响应 ID。
	params := responses.ResponseNewParams{
		Model:        shared.ResponsesModel(c.model), // 指定使用的模型
		Instructions: openai.String(systemPrompt),    // 系统指令，指导模型的行为
		Input: responses.ResponseNewParamsInputUnion{ // 用户消息输入
			OfString: openai.String(message), // 用户消息内容
		},
		// previous_response_id 依赖已存储的上游响应。
		Store: openai.Bool(true), // 在供应商侧保存响应，以便后续通过 previous_response_id 继续上下文
	}

	// 如果提供了上一轮响应 ID，则将其附加到请求参数中，以便模型能够延续上下文。
	if previousResponseID != "" {
		params.PreviousResponseID = openai.String(previousResponseID)
	}

	// 发送请求给 OpenAI Responses API，获取模型生成的响应。
	response, err := c.client.Responses.New(ctx, params)
	if err != nil {
		c.publishFailedUsage(ctx, "responses", c.model, 1, startedAt, err)
		return Result{}, err
	}
	c.publishResponseUsage(ctx, *response, 1, startedAt)
	// 将响应转换为统一的 Result 结构，并计算请求耗时。
	return resultFromResponse(*response, startedAt), nil
}

// Analyze 使用 Structured Outputs，要求模型结果严格匹配 JSON Schema。
func (c *OpenAIClient) Analyze(ctx context.Context, text string) (AnalysisResult, error) {
	startedAt := time.Now()

	// 调用 OpenAI Responses API 创建新的分析请求。
	response, err := c.client.Responses.New(ctx, responses.ResponseNewParams{
		Model:        shared.ResponsesModel(c.model),
		Instructions: openai.String(analysisPrompt),
		Input: responses.ResponseNewParamsInputUnion{
			OfString: openai.String(text),
		},
		// 使用严格 JSON Schema 将自由文本输出约束为预期的数据结构。
		Text: responses.ResponseTextConfigParam{
			Format: responses.ResponseFormatTextConfigUnionParam{
				OfJSONSchema: &responses.ResponseFormatTextJSONSchemaConfigParam{
					Name:        "text_analysis",
					Description: openai.String("Structured analysis of the user's text"),
					Schema:      textAnalysisSchema,
					Strict:      openai.Bool(true),
				},
			},
		},
		Store: openai.Bool(false),
	})
	if err != nil {
		c.publishFailedUsage(ctx, "responses", c.model, 1, startedAt, err)
		return AnalysisResult{}, err
	}
	c.publishResponseUsage(ctx, *response, 1, startedAt)

	// 解析模型返回的文本输出，确保其符合预期的结构化格式。
	analysis, err := parseAnalysis(response.OutputText())
	if err != nil {
		// 即使上游请求成功，也必须验证输出，不能把不符合契约的数据返回给调用方。
		return AnalysisResult{}, &ProviderError{
			Code:    "invalid_structured_output",
			Message: err.Error(),
		}
	}

	return AnalysisResult{
		ID:       response.ID,
		Model:    string(response.Model),
		Analysis: analysis,
		Usage: Usage{
			InputTokens:       response.Usage.InputTokens,
			CachedInputTokens: response.Usage.InputTokensDetails.CachedTokens,
			CacheWriteTokens:  response.Usage.InputTokensDetails.CacheWriteTokens,
			OutputTokens:      response.Usage.OutputTokens,
			ReasoningTokens:   response.Usage.OutputTokensDetails.ReasoningTokens,
			TotalTokens:       response.Usage.TotalTokens,
		},
		DurationMS: time.Since(startedAt).Milliseconds(),
	}, nil
}

// parseAnalysis 解析模型返回的结构化分析结果，确保其符合预期的 JSON Schema。
func parseAnalysis(output string) (Analysis, error) {
	// Schema 模式仍在客户端做防御性校验，确保 API 行为变化时契约不被破坏。
	if output == "" {
		return Analysis{}, errors.New("model returned an empty structured output")
	}

	var analysis Analysis
	if err := json.Unmarshal([]byte(output), &analysis); err != nil {
		return Analysis{}, fmt.Errorf("decode structured output: %w", err)
	}

	// 逐项校验调用方依赖的必填字段及枚举范围。
	switch analysis.Sentiment {
	case "positive", "neutral", "negative":
	default:
		return Analysis{}, fmt.Errorf("unexpected sentiment %q", analysis.Sentiment)
	}
	if analysis.Summary == "" {
		return Analysis{}, errors.New("structured output is missing summary")
	}
	if analysis.Keywords == nil {
		return Analysis{}, errors.New("structured output is missing keywords")
	}

	return analysis, nil
}

// Stream 调用 LLM 模型进行增量流式生成，逐段返回内容给客户端。
func (c *OpenAIClient) Stream(ctx context.Context, message string, onDelta func(string) error) (Result, error) {
	return c.streamResponse(ctx, responses.ResponseNewParams{
		Model:        shared.ResponsesModel(c.model), // 指定使用的模型
		Instructions: openai.String(systemPrompt),    // 系统指令，指导模型的行为
		Input: responses.ResponseNewParamsInputUnion{ // 用户消息输入
			OfString: openai.String(message), // 用户消息文本
		},
		Store: openai.Bool(false), // 不在 OpenAI 端存储响应内容
	}, onDelta)
}

// streamResponse 执行统一的 Responses API 流读取逻辑。
func (c *OpenAIClient) streamResponse(ctx context.Context, params responses.ResponseNewParams, onDelta func(string) error) (result Result, resultErr error) {
	startedAt := time.Now()
	defer func() {
		if resultErr != nil {
			c.publishFailedUsage(ctx, "responses", c.model, 1, startedAt, resultErr)
		}
	}()
	stream := c.client.Responses.NewStreaming(ctx, params)
	defer stream.Close()

	var finalResponse responses.Response

	// 流事件既包含增量文本，也包含最终响应或失败信息；只向上层转发文本增量。
	for stream.Next() {

		// 获取当前流事件，判断事件类型并处理相应逻辑。
		event := stream.Current()
		switch event.Type {
		case "response.output_text.delta":
			if event.Delta != "" {
				// 回调错误通常表示客户端断开或下游写入失败，应立即停止消费上游流。
				if err := onDelta(event.Delta); err != nil {
					return Result{}, err
				}
			}
		case "response.completed":
			// 只有收到完整响应事件后，才有最终文本和用量可供返回。
			finalResponse = event.Response
		case "response.failed":
			return Result{}, &ProviderError{
				Code:    string(event.Response.Error.Code),
				Message: event.Response.Error.Message,
			}
		case "response.incomplete":
			return Result{}, &ProviderError{
				Code:    "response_incomplete",
				Message: event.Response.IncompleteDetails.Reason,
			}
		case "error":
			return Result{}, &ProviderError{Code: event.Code, Message: event.Message}
		}
	}

	// Next 返回 false 既可能代表正常结束，也可能代表流读取失败，必须检查 Err。
	if err := stream.Err(); err != nil {
		return Result{}, normalizeStreamError(err)
	}

	// 检查最终响应是否存在，确保模型流已完整返回。
	if finalResponse.ID == "" {
		return Result{}, fmt.Errorf("model stream ended without a completed response")
	}

	// 将最终响应转换为统一的 Result 结构，并计算请求耗时。
	c.publishResponseUsage(ctx, finalResponse, 1, startedAt)
	return resultFromResponse(finalResponse, startedAt), nil
}

func normalizeStreamError(err error) error {
	// SDK 将部分 SSE 错误载荷包装为 StreamError；尽量提取其 JSON 错误码，
	// 使 HTTP 层能像处理普通 API 错误一样映射错误类别。
	var streamErr *ssestream.StreamError
	if !errors.As(err, &streamErr) {
		return err
	}

	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(streamErr.Event.Data, &payload) != nil {
		// 载荷不符合已知格式时保留原始错误，避免丢失 SDK 提供的诊断信息。
		return err
	}
	if payload.Error.Code != "" || payload.Error.Message != "" {
		return &ProviderError{Code: payload.Error.Code, Message: payload.Error.Message}
	}
	if payload.Code != "" || payload.Message != "" {
		return &ProviderError{Code: payload.Code, Message: payload.Message}
	}
	return err
}

// resultFromResponse 将 OpenAI Responses API 的响应转换为统一的 Result 结构，并计算请求耗时。
func resultFromResponse(response responses.Response, startedAt time.Time) Result {
	return Result{
		ID:    response.ID,
		Model: string(response.Model),
		Text:  response.OutputText(),
		Usage: Usage{
			InputTokens:       response.Usage.InputTokens,
			CachedInputTokens: response.Usage.InputTokensDetails.CachedTokens,
			CacheWriteTokens:  response.Usage.InputTokensDetails.CacheWriteTokens,
			OutputTokens:      response.Usage.OutputTokens,
			ReasoningTokens:   response.Usage.OutputTokensDetails.ReasoningTokens,
			TotalTokens:       response.Usage.TotalTokens,
		},
		DurationMS: time.Since(startedAt).Milliseconds(),
	}
}
