package usage

import "context"

type metadataContextKey struct{}

// Metadata 是一次业务请求在所有模型调用间共享的追踪信息。
type Metadata struct {
	TraceID        string
	RequestID      string
	ConversationID string
	UserID         string
	Route          string
}

// WithMetadata 合并非空字段，使下层调用可以补充 route 或 conversation_id。
func WithMetadata(ctx context.Context, update Metadata) context.Context {
	current := MetadataFromContext(ctx)
	if update.TraceID != "" {
		current.TraceID = update.TraceID
	}
	if update.RequestID != "" {
		current.RequestID = update.RequestID
	}
	if update.ConversationID != "" {
		current.ConversationID = update.ConversationID
	}
	if update.UserID != "" {
		current.UserID = update.UserID
	}
	if update.Route != "" {
		current.Route = update.Route
	}
	return context.WithValue(ctx, metadataContextKey{}, current)
}

func MetadataFromContext(ctx context.Context) Metadata {
	metadata, _ := ctx.Value(metadataContextKey{}).(Metadata)
	return metadata
}

func WithRoute(ctx context.Context, route string) context.Context {
	return WithMetadata(ctx, Metadata{Route: route})
}
