package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
)

const (
	toolSystemPrompt = "You are a concise task-solving agent. Reply in the user's language. For arithmetic, always use the calculate tool. You may call it repeatedly when a task requires multiple operations. Use tool results rather than doing arithmetic yourself, and return a final answer only after the task is complete."
	// 限制一次请求最多执行的工具次数，防止模型因错误规划进入无限循环。
	maxAgentToolCalls = 5
)

// calculatorTool 把 Go 侧可执行的 calculate 函数声明给模型。
// Strict 和 JSON Schema 用于约束参数名称、类型和运算枚举；服务端仍会再次校验，
// 因为模型输出属于外部输入，不能仅依赖模型遵守 Schema。
var calculatorTool = responses.ToolUnionParam{
	OfFunction: &responses.FunctionToolParam{
		Name:        "calculate",
		Description: openai.String("Perform one arithmetic operation on two numbers."),
		Strict:      openai.Bool(true),
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"operation": map[string]any{
					"type": "string",
					"enum": []string{"add", "subtract", "multiply", "divide"},
				},
				"a": map[string]any{"type": "number"},
				"b": map[string]any{"type": "number"},
			},
			"required":             []string{"operation", "a", "b"},
			"additionalProperties": false,
		},
	},
}

// calculatorArguments 是模型传入的工具参数。
// 数值字段使用指针，以区分“字段缺失”和“字段存在且值为 0”。
type calculatorArguments struct {
	Operation string   `json:"operation"`
	A         *float64 `json:"a"`
	B         *float64 `json:"b"`
}

// calculatorOutput 是工具执行结果，也是回传给模型的 JSON 内容。
type calculatorOutput struct {
	Operation string  `json:"operation"`
	A         float64 `json:"a"`
	B         float64 `json:"b"`
	Result    float64 `json:"result"`
}

// ToolCallTrace 记录一次模型发起、服务端执行的函数调用，方便调试与评测。
// Arguments 和 Output 保留为原始 JSON，避免把任意工具载荷强行转换成固定结构。
type ToolCallTrace struct {
	Step      int             `json:"step"`
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Output    json.RawMessage `json:"output"`
}

// ToolResult 汇总最终回答、工具轨迹以及模型交互的总 token 用量。
// ToolCalls 为空时表示模型直接作答，没有执行工具。
type ToolResult struct {
	ID         string          `json:"id"`
	Model      string          `json:"model"`
	Text       string          `json:"text"`
	ToolCalls  []ToolCallTrace `json:"tool_calls"`
	ModelCalls int             `json:"model_calls"`
	Usage      Usage           `json:"usage"`
	DurationMS int64           `json:"duration_ms"`
}

type responseCreator func(context.Context, responses.ResponseNewParams) (*responses.Response, error)

// GenerateWithTools 运行受步数限制的 Agent 循环。
// 它会不断调用模型，执行模型建议的工具操作，直到模型返回最终文本结果。
func (c *OpenAIClient) GenerateWithTools(ctx context.Context, message string) (ToolResult, error) {
	// 调用工具代理循环，处理模型的工具调用，直到得到最终文本结果。
	return runToolAgent(ctx, c.model, message, func(ctx context.Context, params responses.ResponseNewParams) (*responses.Response, error) {

		// 调用 OpenAI Responses API 创建新的响应。
		return c.client.Responses.New(ctx, params)
	})
}

// runToolAgent 重复执行“模型决策 → 工具执行 → 回传结果”，直到模型给出最终文本。
// responseCreator 作为依赖注入点，让循环可以在不访问真实模型的情况下测试。
func runToolAgent(ctx context.Context, model, message string, create responseCreator) (ToolResult, error) {
	startedAt := time.Now()

	// 初始化工具代理循环的起始状态，包括总用量、模型调用次数和工具调用轨迹。
	response, err := create(ctx, responses.ResponseNewParams{
		Model:        shared.ResponsesModel(model),    // 指定使用的模型
		Instructions: openai.String(toolSystemPrompt), // 指定工具代理的系统提示
		Input: responses.ResponseNewParamsInputUnion{
			OfString: openai.String(message), // 用户的输入消息
		},
		Tools:             []responses.ToolUnionParam{calculatorTool}, // 指定可用的工具列表
		ParallelToolCalls: openai.Bool(false),                         // 禁用并行工具调用
		// PreviousResponseID 需要服务端保留上一轮，工具闭环结束后不再继续引用它。
		Store: openai.Bool(true), // 指示服务端存储本轮响应，以便后续引用上一轮结果
	})
	if err != nil {
		return ToolResult{}, err
	}
	// 如果初始化响应失败，直接返回错误；否则提取初始的 token 用量和模型调用次数。
	totalUsage := usageFromResponse(*response)
	modelCalls := 1                                       // 初始化模型调用次数为 1
	traces := make([]ToolCallTrace, 0, maxAgentToolCalls) // 初始化工具调用轨迹切片，容量为最大允许的工具调用次数。

	// 进入工具代理循环，持续处理模型的工具调用，直到模型返回最终文本结果。
	for {
		call, found := findFunctionCall(response.Output)
		if !found {
			if strings.TrimSpace(response.OutputText()) == "" {
				return ToolResult{}, &ProviderError{
					Code:    "empty_tool_response",
					Message: "agent returned neither a tool call nor final text",
				}
			}
			result := toolResultFromResponse(*response, traces, startedAt)
			result.Usage = totalUsage
			result.ModelCalls = modelCalls
			return result, nil
		}

		if len(traces) >= maxAgentToolCalls {
			return ToolResult{}, &ProviderError{
				Code:    "agent_step_limit",
				Message: fmt.Sprintf("agent requested more than %d tool calls", maxAgentToolCalls),
			}
		}

		trace, toolOutput, err := executeFunctionCall(call)
		if err != nil {
			return ToolResult{}, err
		}
		trace.Step = len(traces) + 1
		traces = append(traces, trace)

		// call_id 将本地执行结果与模型发起的那次函数调用精确关联。
		outputItem := responses.ResponseInputItemParamOfFunctionCallOutput(toolOutput)
		outputItem.OfFunctionCallOutput.CallID = openai.String(call.CallID)
		response, err = create(ctx, responses.ResponseNewParams{
			Model:              shared.ResponsesModel(model),
			Instructions:       openai.String(toolSystemPrompt),
			PreviousResponseID: openai.String(response.ID),
			Input: responses.ResponseNewParamsInputUnion{
				OfInputItemList: []responses.ResponseInputItemUnionParam{outputItem},
			},
			Tools:             []responses.ToolUnionParam{calculatorTool},
			ParallelToolCalls: openai.Bool(false),
			Store:             openai.Bool(true),
		})
		if err != nil {
			return ToolResult{}, err
		}
		modelCalls++
		totalUsage = addUsage(totalUsage, usageFromResponse(*response))
	}
}

// findFunctionCall 从模型响应项目中提取第一个函数调用。
func findFunctionCall(items []responses.ResponseOutputItemUnion) (responses.ResponseFunctionToolCall, bool) {
	for _, item := range items {
		if item.Type == "function_call" {
			return item.AsFunctionCall(), true
		}
	}
	return responses.ResponseFunctionToolCall{}, false
}

// executeFunctionCall 校验工具名称和参数，在服务端执行计算并生成可回传结果。
func executeFunctionCall(call responses.ResponseFunctionToolCall) (ToolCallTrace, string, error) {
	// 工具名来自模型响应；只允许执行本服务显式注册的函数，禁止动态调用任意代码。
	if call.Name != "calculate" {
		return ToolCallTrace{}, "", &ProviderError{
			Code:    "unsupported_tool",
			Message: fmt.Sprintf("tool %q is not registered", call.Name),
		}
	}

	var args calculatorArguments
	if err := decodeToolArguments(call.Arguments, &args); err != nil {
		return ToolCallTrace{}, "", &ProviderError{
			Code:    "invalid_tool_arguments",
			Message: err.Error(),
		}
	}
	if args.A == nil || args.B == nil {
		return ToolCallTrace{}, "", &ProviderError{
			Code:    "invalid_tool_arguments",
			Message: "calculator arguments must include a and b",
		}
	}
	// 解引用前确认两个数值都存在，0 是有效数值，不能作为缺失值处理。
	a, b := *args.A, *args.B

	var value float64
	// 运算类型已由 Schema 限制，但此处仍保留 default 防线，处理绕过或异常输入。
	switch args.Operation {
	case "add":
		value = a + b
	case "subtract":
		value = a - b
	case "multiply":
		value = a * b
	case "divide":
		if b == 0 {
			return ToolCallTrace{}, "", &ProviderError{Code: "tool_execution_failed", Message: "division by zero"}
		}
		value = a / b
	default:
		return ToolCallTrace{}, "", &ProviderError{
			Code:    "invalid_tool_arguments",
			Message: fmt.Sprintf("unsupported operation %q", args.Operation),
		}
	}
	// 浮点数溢出以及非法算术可能产生 Inf 或 NaN；这些值无法编码为标准 JSON。
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return ToolCallTrace{}, "", &ProviderError{Code: "tool_execution_failed", Message: "calculation produced a non-finite result"}
	}

	output, err := json.Marshal(calculatorOutput{
		Operation: args.Operation,
		A:         a,
		B:         b,
		Result:    value,
	})
	if err != nil {
		return ToolCallTrace{}, "", fmt.Errorf("encode tool output: %w", err)
	}

	// 同时返回结构化轨迹和 JSON 字符串：前者用于 API 响应，后者作为模型工具结果。
	return ToolCallTrace{
		CallID:    call.CallID,
		Name:      call.Name,
		Arguments: json.RawMessage(call.Arguments),
		Output:    json.RawMessage(output),
	}, string(output), nil
}

// decodeToolArguments 将模型参数解码到目标结构，并拒绝未知字段和尾随 JSON 值。
func decodeToolArguments(raw string, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode tool arguments: %w", err)
	}
	// 一次 Decode 成功后仍需确认已到达输入末尾，避免接受拼接的多个 JSON 值。
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("tool arguments must contain exactly one JSON object")
	}
	return nil
}

// toolResultFromResponse 从某一轮模型响应构建统一结果；nil 轨迹规范化为空数组。
func toolResultFromResponse(response responses.Response, traces []ToolCallTrace, startedAt time.Time) ToolResult {
	if traces == nil {
		traces = []ToolCallTrace{}
	}
	return ToolResult{
		ID:         response.ID,
		Model:      string(response.Model),
		Text:       response.OutputText(),
		ToolCalls:  traces,
		Usage:      usageFromResponse(response),
		DurationMS: time.Since(startedAt).Milliseconds(),
	}
}

// usageFromResponse 将 SDK 用量结构转换为项目对外使用的类型。
func usageFromResponse(response responses.Response) Usage {
	return Usage{
		InputTokens:  response.Usage.InputTokens,
		OutputTokens: response.Usage.OutputTokens,
		TotalTokens:  response.Usage.TotalTokens,
	}
}

// addUsage 按 token 类型相加，汇总工具调用前后的多轮模型请求成本。
func addUsage(left, right Usage) Usage {
	return Usage{
		InputTokens:  left.InputTokens + right.InputTokens,
		OutputTokens: left.OutputTokens + right.OutputTokens,
		TotalTokens:  left.TotalTokens + right.TotalTokens,
	}
}
