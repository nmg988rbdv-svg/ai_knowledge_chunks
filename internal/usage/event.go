package usage

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// Event 表示一次真实的模型或 Embedding 上游调用。
// Agent 的多轮调用分别产生事件，并通过 TraceID 和 CallIndex 关联。
type Event struct {
	ID                string         `json:"id"`
	TraceID           string         `json:"trace_id"`
	RequestID         string         `json:"request_id"`
	ResponseID        string         `json:"response_id,omitempty"`
	ConversationID    string         `json:"conversation_id,omitempty"`
	UserID            string         `json:"user_id,omitempty"`
	Provider          string         `json:"provider"`
	Model             string         `json:"model"`
	Route             string         `json:"route"`
	Operation         string         `json:"operation"`
	CallIndex         int            `json:"call_index"`
	InputTokens       int64          `json:"input_tokens"`
	CachedInputTokens int64          `json:"cached_input_tokens"`
	CacheWriteTokens  int64          `json:"cache_write_tokens"`
	OutputTokens      int64          `json:"output_tokens"`
	ReasoningTokens   int64          `json:"reasoning_tokens"`
	TotalTokens       int64          `json:"total_tokens"`
	DurationMS        int64          `json:"duration_ms"`
	Status            string         `json:"status"`
	ErrorCode         string         `json:"error_code,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	CreatedAt         time.Time      `json:"created_at"`
}

// NewEvent 填充事件必须具备的幂等 ID、时间和请求追踪信息。
func NewEvent(ctxMeta Metadata) Event {
	requestID := strings.TrimSpace(ctxMeta.RequestID)
	if requestID == "" {
		requestID = NewID("req_")
	}
	traceID := strings.TrimSpace(ctxMeta.TraceID)
	if traceID == "" {
		traceID = requestID
	}
	return Event{
		ID:             uuid.NewString(),
		TraceID:        traceID,
		RequestID:      requestID,
		ConversationID: strings.TrimSpace(ctxMeta.ConversationID),
		UserID:         strings.TrimSpace(ctxMeta.UserID),
		Route:          strings.TrimSpace(ctxMeta.Route),
		CallIndex:      1,
		Status:         StatusCompleted,
		CreatedAt:      time.Now().UTC(),
	}
}

// Validate 拒绝无法可靠入库或聚合的事件。
func (e Event) Validate() error {
	if _, err := uuid.Parse(e.ID); err != nil {
		return errors.New("usage event ID must be a UUID")
	}
	if strings.TrimSpace(e.TraceID) == "" || strings.TrimSpace(e.RequestID) == "" {
		return errors.New("usage event trace_id and request_id are required")
	}
	if strings.TrimSpace(e.Provider) == "" || strings.TrimSpace(e.Model) == "" || strings.TrimSpace(e.Operation) == "" {
		return errors.New("usage event provider, model, and operation are required")
	}
	if e.CallIndex < 1 {
		return errors.New("usage event call_index must be greater than zero")
	}
	if e.InputTokens < 0 || e.CachedInputTokens < 0 || e.CacheWriteTokens < 0 || e.OutputTokens < 0 || e.ReasoningTokens < 0 || e.TotalTokens < 0 || e.DurationMS < 0 {
		return errors.New("usage event numeric values must not be negative")
	}
	if e.Status != StatusCompleted && e.Status != StatusFailed {
		return errors.New("usage event status must be completed or failed")
	}
	if e.CreatedAt.IsZero() {
		return errors.New("usage event created_at is required")
	}
	return nil
}

// NewID 创建可读前缀的请求标识；数据库事件主键仍使用标准 UUID。
func NewID(prefix string) string {
	return prefix + strings.ReplaceAll(uuid.NewString(), "-", "")
}
