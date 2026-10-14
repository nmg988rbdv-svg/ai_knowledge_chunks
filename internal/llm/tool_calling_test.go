package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"
)

func TestRunToolAgentSupportsMultipleToolCalls(t *testing.T) {
	responsesByTurn := []*responses.Response{
		decodeTestResponse(t, `{
			"id":"resp_1","model":"test-model",
			"output":[{"type":"function_call","call_id":"call_1","name":"calculate","arguments":"{\"operation\":\"multiply\",\"a\":12,\"b\":7}"}],
			"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}
		}`),
		decodeTestResponse(t, `{
			"id":"resp_2","model":"test-model",
			"output":[{"type":"function_call","call_id":"call_2","name":"calculate","arguments":"{\"operation\":\"add\",\"a\":84,\"b\":16}"}],
			"usage":{"input_tokens":12,"output_tokens":2,"total_tokens":14}
		}`),
		decodeTestResponse(t, `{
			"id":"resp_3","model":"test-model",
			"output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"结果是 100。"}]}],
			"usage":{"input_tokens":14,"output_tokens":5,"total_tokens":19}
		}`),
	}
	turn := 0
	result, err := runToolAgent(context.Background(), "test-model", "先算 12×7，再加 16", func(_ context.Context, params responses.ResponseNewParams) (*responses.Response, error) {
		if turn > 0 {
			data, marshalErr := json.Marshal(params)
			if marshalErr != nil {
				t.Fatalf("encode turn %d params: %v", turn+1, marshalErr)
			}
			expectedPreviousID := fmt.Sprintf(`"previous_response_id":"resp_%d"`, turn)
			if !strings.Contains(string(data), expectedPreviousID) {
				t.Fatalf("turn %d params missing %s: %s", turn+1, expectedPreviousID, data)
			}
			expectedCallID := fmt.Sprintf("call_%d", turn)
			if !strings.Contains(string(data), expectedCallID) || !strings.Contains(string(data), `"name":"calculate"`) {
				t.Fatalf("turn %d does not return %s with tools: %s", turn+1, expectedCallID, data)
			}
		}
		response := responsesByTurn[turn]
		turn++
		return response, nil
	})
	if err != nil {
		t.Fatalf("run agent: %v", err)
	}
	if result.Text != "结果是 100。" || result.ModelCalls != 3 || len(result.ToolCalls) != 2 {
		t.Fatalf("result=%#v", result)
	}
	if result.ToolCalls[0].Step != 1 || result.ToolCalls[0].CallID != "call_1" || !strings.Contains(string(result.ToolCalls[0].Output), `"result":84`) {
		t.Fatalf("first trace=%#v", result.ToolCalls[0])
	}
	if result.ToolCalls[1].Step != 2 || !strings.Contains(string(result.ToolCalls[1].Output), `"result":100`) {
		t.Fatalf("second trace=%#v", result.ToolCalls[1])
	}
	if result.Usage.TotalTokens != 45 {
		t.Fatalf("usage=%#v", result.Usage)
	}
}

func TestRunToolAgentStopsAtToolCallLimit(t *testing.T) {
	turn := 0
	_, err := runToolAgent(context.Background(), "test-model", "一直计算", func(_ context.Context, _ responses.ResponseNewParams) (*responses.Response, error) {
		turn++
		return decodeTestResponse(t, fmt.Sprintf(`{
			"id":"resp_%d","model":"test-model",
			"output":[{"type":"function_call","call_id":"call_%d","name":"calculate","arguments":"{\"operation\":\"add\",\"a\":1,\"b\":1}"}],
			"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}
		}`, turn, turn)), nil
	})

	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "agent_step_limit" {
		t.Fatalf("error=%#v, want agent_step_limit", err)
	}
	if turn != maxAgentToolCalls+1 {
		t.Fatalf("model calls=%d, want %d", turn, maxAgentToolCalls+1)
	}
}

func decodeTestResponse(t *testing.T, raw string) *responses.Response {
	t.Helper()
	var response responses.Response
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("decode test response: %v", err)
	}
	return &response
}

func TestCalculatorToolUsesStrictSchema(t *testing.T) {
	// 确保声明给模型的工具参数是严格模式，且所有参数必填、禁止额外字段。
	data, err := json.Marshal(calculatorTool)
	if err != nil {
		t.Fatalf("encode calculator tool: %v", err)
	}
	for _, expected := range []string{`"strict":true`, `"additionalProperties":false`, `"required":["operation","a","b"]`} {
		if !strings.Contains(string(data), expected) {
			t.Fatalf("tool schema does not contain %q: %s", expected, data)
		}
	}
}

func TestExecuteCalculateFunction(t *testing.T) {
	// 覆盖有效乘法参数，并检查函数调用轨迹和序列化结果。
	trace, output, err := executeFunctionCall(responses.ResponseFunctionToolCall{
		CallID:    "call_test",
		Name:      "calculate",
		Arguments: `{"operation":"multiply","a":12,"b":7}`,
	})
	if err != nil {
		t.Fatalf("execute function: %v", err)
	}
	if trace.Name != "calculate" || !strings.Contains(output, `"result":84`) {
		t.Fatalf("unexpected trace=%#v output=%s", trace, output)
	}
}

func TestExecuteCalculateRejectsDivisionByZero(t *testing.T) {
	// 除数为零不能产生无效数值，应映射为工具执行错误。
	_, _, err := executeFunctionCall(responses.ResponseFunctionToolCall{
		Name:      "calculate",
		Arguments: `{"operation":"divide","a":1,"b":0}`,
	})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "tool_execution_failed" {
		t.Fatalf("error = %#v, want tool_execution_failed", err)
	}
}

func TestExecuteFunctionRejectsUnknownTool(t *testing.T) {
	// 模型请求未注册的函数时必须拒绝，不能执行任意名称的工具。
	_, _, err := executeFunctionCall(responses.ResponseFunctionToolCall{Name: "delete_database", Arguments: `{}`})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_tool" {
		t.Fatalf("error = %#v, want unsupported_tool", err)
	}
}

func TestDecodeToolArgumentsRejectsUnknownField(t *testing.T) {
	// 严格解码应拒绝工具结构未声明的字段，避免忽略模型的异常参数。
	var args calculatorArguments
	err := decodeToolArguments(`{"operation":"add","a":1,"b":2,"admin":true}`, &args)
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestExecuteCalculateRejectsMissingNumber(t *testing.T) {
	// 数值字段缺失与数值为零不同；缺字段必须在执行前报参数错误。
	_, _, err := executeFunctionCall(responses.ResponseFunctionToolCall{
		Name:      "calculate",
		Arguments: `{"operation":"add","a":1}`,
	})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "invalid_tool_arguments" {
		t.Fatalf("error = %#v, want invalid_tool_arguments", err)
	}
}
