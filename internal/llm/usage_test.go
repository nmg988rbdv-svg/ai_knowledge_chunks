package llm

import (
	"context"
	"testing"
	"time"

	"github.com/openai/openai-go/v3/responses"

	usageevent "llm-api-demo/internal/usage"
)

type captureUsagePublisher struct {
	events []usageevent.Event
}

func (p *captureUsagePublisher) Publish(_ context.Context, event usageevent.Event) error {
	p.events = append(p.events, event)
	return nil
}

func (*captureUsagePublisher) Ping(context.Context) error { return nil }
func (*captureUsagePublisher) Close() error               { return nil }

func TestPublishResponseUsageIncludesTokenBreakdown(t *testing.T) {
	publisher := &captureUsagePublisher{}
	client := &OpenAIClient{model: "configured-model", usagePublisher: publisher}
	ctx := usageevent.WithMetadata(context.Background(), usageevent.Metadata{
		TraceID: "trace-1", RequestID: "request-1", Route: "assistant.agent",
	})
	client.publishResponseUsage(ctx, responses.Response{
		ID:    "resp_1",
		Model: "actual-model",
		Usage: responses.ResponseUsage{
			InputTokens: 10,
			InputTokensDetails: responses.ResponseUsageInputTokensDetails{
				CachedTokens: 4, CacheWriteTokens: 2,
			},
			OutputTokens: 5,
			OutputTokensDetails: responses.ResponseUsageOutputTokensDetails{
				ReasoningTokens: 3,
			},
			TotalTokens: 15,
		},
	}, 2, time.Now().Add(-time.Millisecond))

	if len(publisher.events) != 1 {
		t.Fatalf("events = %d", len(publisher.events))
	}
	event := publisher.events[0]
	if event.CallIndex != 2 || event.ResponseID != "resp_1" || event.Model != "actual-model" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	if event.InputTokens != 10 || event.CachedInputTokens != 4 || event.CacheWriteTokens != 2 || event.OutputTokens != 5 || event.ReasoningTokens != 3 || event.TotalTokens != 15 {
		t.Fatalf("unexpected token breakdown: %#v", event)
	}
}
