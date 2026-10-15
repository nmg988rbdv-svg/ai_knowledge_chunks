package usage

import (
	"context"
	"testing"
)

func TestNewEventUsesContextMetadata(t *testing.T) {
	ctx := WithMetadata(context.Background(), Metadata{
		TraceID:        "trace-1",
		RequestID:      "request-1",
		ConversationID: "conversation-1",
		Route:          "assistant.agent",
	})
	event := NewEvent(MetadataFromContext(ctx))
	event.Provider = "openai"
	event.Model = "test-model"
	event.Operation = "responses"

	if err := event.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if event.TraceID != "trace-1" || event.RequestID != "request-1" {
		t.Fatalf("unexpected IDs: %#v", event)
	}
	if event.ConversationID != "conversation-1" || event.Route != "assistant.agent" {
		t.Fatalf("unexpected metadata: %#v", event)
	}
}

func TestWithMetadataMergesNonEmptyFields(t *testing.T) {
	ctx := WithMetadata(context.Background(), Metadata{TraceID: "trace-1", RequestID: "request-1", Route: "chat"})
	ctx = WithMetadata(ctx, Metadata{ConversationID: "conversation-1", Route: "conversation"})
	metadata := MetadataFromContext(ctx)

	if metadata.TraceID != "trace-1" || metadata.RequestID != "request-1" {
		t.Fatalf("base metadata was lost: %#v", metadata)
	}
	if metadata.ConversationID != "conversation-1" || metadata.Route != "conversation" {
		t.Fatalf("metadata was not merged: %#v", metadata)
	}
}

func TestEventValidateRejectsNegativeUsage(t *testing.T) {
	event := NewEvent(Metadata{TraceID: "trace-1", RequestID: "request-1"})
	event.Provider = "openai"
	event.Model = "test-model"
	event.Operation = "responses"
	event.TotalTokens = -1
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want negative usage error")
	}
}
