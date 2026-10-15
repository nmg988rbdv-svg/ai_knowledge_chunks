package llm

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/openai/openai-go/v3/responses"

	usageevent "llm-api-demo/internal/usage"
)

func (c *OpenAIClient) publishResponseUsage(ctx context.Context, response responses.Response, callIndex int, startedAt time.Time) {
	event := usageevent.NewEvent(usageevent.MetadataFromContext(ctx))
	event.ResponseID = response.ID
	event.Provider = "openai"
	event.Model = string(response.Model)
	event.Operation = "responses"
	event.CallIndex = callIndex
	event.InputTokens = response.Usage.InputTokens
	event.CachedInputTokens = response.Usage.InputTokensDetails.CachedTokens
	event.CacheWriteTokens = response.Usage.InputTokensDetails.CacheWriteTokens
	event.OutputTokens = response.Usage.OutputTokens
	event.ReasoningTokens = response.Usage.OutputTokensDetails.ReasoningTokens
	event.TotalTokens = response.Usage.TotalTokens
	event.DurationMS = time.Since(startedAt).Milliseconds()
	c.publishUsage(ctx, event)
}

func (c *OpenAIClient) publishFailedUsage(ctx context.Context, operation, model string, callIndex int, startedAt time.Time, callErr error) {
	event := usageevent.NewEvent(usageevent.MetadataFromContext(ctx))
	event.Provider = "openai"
	event.Model = model
	event.Operation = operation
	event.CallIndex = callIndex
	event.DurationMS = time.Since(startedAt).Milliseconds()
	event.Status = usageevent.StatusFailed
	event.ErrorCode = usageErrorCode(callErr)
	c.publishUsage(ctx, event)
}

func (c *OpenAIClient) publishUsage(ctx context.Context, event usageevent.Event) {
	if c.usagePublisher == nil {
		return
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.usagePublisher.Publish(publishCtx, event); err != nil {
		logger := c.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("publish model usage event", "event_id", event.ID, "trace_id", event.TraceID, "error", err)
	}
}

func usageErrorCode(err error) string {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) && providerErr.Code != "" {
		return providerErr.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "upstream_error"
}
