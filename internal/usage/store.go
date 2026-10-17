package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	usagedb "llm-api-demo/internal/usage/db/sqlc"
)

// Store 使用 PostgreSQL 保存可审计的用量明细。
type Store struct {
	pool    *pgxpool.Pool
	queries *usagedb.Queries
}

// Summary 汇总当前保存的全部模型与 Embedding 调用用量。
type Summary struct {
	TotalCalls        int64 `json:"total_calls"`
	CompletedCalls    int64 `json:"completed_calls"`
	FailedCalls       int64 `json:"failed_calls"`
	InputTokens       int64 `json:"input_tokens"`
	CachedInputTokens int64 `json:"cached_input_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
	ReasoningTokens   int64 `json:"reasoning_tokens"`
	TotalTokens       int64 `json:"total_tokens"`
}

// Record 是面向管理界面返回的单次调用明细，不暴露内部数据库类型。
type Record struct {
	ID                string    `json:"id"`
	TraceID           string    `json:"trace_id"`
	Model             string    `json:"model"`
	Route             string    `json:"route"`
	Operation         string    `json:"operation"`
	CallIndex         int32     `json:"call_index"`
	InputTokens       int64     `json:"input_tokens"`
	CachedInputTokens int64     `json:"cached_input_tokens"`
	OutputTokens      int64     `json:"output_tokens"`
	ReasoningTokens   int64     `json:"reasoning_tokens"`
	TotalTokens       int64     `json:"total_tokens"`
	DurationMS        int64     `json:"duration_ms"`
	Status            string    `json:"status"`
	ErrorCode         string    `json:"error_code,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

// Snapshot 是 UI 一次刷新所需的汇总和最近调用列表。
type Snapshot struct {
	Summary Summary  `json:"summary"`
	Records []Record `json:"records"`
}

// NewStore 创建一个 Store 实例，用于向 PostgreSQL 写入使用事件。
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return &Store{pool: pool, queries: usagedb.New(pool)}, nil
}

// Insert 幂等写入一条用量事件。Kafka 重投相同事件时 RowsAffected 为 0。
func (s *Store) Insert(ctx context.Context, event Event) (bool, error) {
	// 验证使用事件的合法性
	if err := event.Validate(); err != nil {
		return false, err
	}
	// 解析使用事件的 ID 为 UUID
	id, err := uuid.Parse(event.ID)
	if err != nil {
		return false, err
	}
	// 处理使用事件的元数据，将其编码为 JSON
	metadata := event.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return false, fmt.Errorf("encode usage metadata: %w", err)
	}
	// 将使用事件写入 PostgreSQL 存储
	rows, err := s.queries.InsertUsageRecord(ctx, usagedb.InsertUsageRecordParams{
		ID:                id,
		TraceID:           event.TraceID,
		RequestID:         event.RequestID,
		ResponseID:        event.ResponseID,
		ConversationID:    event.ConversationID,
		UserID:            event.UserID,
		Provider:          event.Provider,
		Model:             event.Model,
		Route:             event.Route,
		Operation:         event.Operation,
		CallIndex:         int32(event.CallIndex),
		InputTokens:       event.InputTokens,
		CachedInputTokens: event.CachedInputTokens,
		CacheWriteTokens:  event.CacheWriteTokens,
		OutputTokens:      event.OutputTokens,
		ReasoningTokens:   event.ReasoningTokens,
		TotalTokens:       event.TotalTokens,
		DurationMs:        event.DurationMS,
		Status:            event.Status,
		ErrorCode:         event.ErrorCode,
		Metadata:          metadataJSON,
		CreatedAt:         pgtype.Timestamptz{Time: event.CreatedAt, Valid: true},
	})
	if err != nil {
		return false, fmt.Errorf("insert usage record: %w", err)
	}
	return rows > 0, nil
}

// Snapshot 返回总用量及最近的调用明细。
func (s *Store) Snapshot(ctx context.Context, limit int) (Snapshot, error) {
	if limit < 1 || limit > 100 {
		return Snapshot{}, fmt.Errorf("usage record limit must be between 1 and 100")
	}
	summaryRow, err := s.queries.GetUsageSummary(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("get usage summary: %w", err)
	}
	rows, err := s.queries.ListRecentUsageRecords(ctx, int32(limit))
	if err != nil {
		return Snapshot{}, fmt.Errorf("list recent usage records: %w", err)
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		createdAt := time.Time{}
		if row.CreatedAt.Valid {
			createdAt = row.CreatedAt.Time
		}
		records = append(records, Record{
			ID:                row.ID.String(),
			TraceID:           row.TraceID,
			Model:             row.Model,
			Route:             row.Route,
			Operation:         row.Operation,
			CallIndex:         row.CallIndex,
			InputTokens:       row.InputTokens,
			CachedInputTokens: row.CachedInputTokens,
			OutputTokens:      row.OutputTokens,
			ReasoningTokens:   row.ReasoningTokens,
			TotalTokens:       row.TotalTokens,
			DurationMS:        row.DurationMs,
			Status:            row.Status,
			ErrorCode:         row.ErrorCode,
			CreatedAt:         createdAt,
		})
	}
	return Snapshot{
		Summary: Summary{
			TotalCalls:        summaryRow.TotalCalls,
			CompletedCalls:    summaryRow.CompletedCalls,
			FailedCalls:       summaryRow.FailedCalls,
			InputTokens:       summaryRow.InputTokens,
			CachedInputTokens: summaryRow.CachedInputTokens,
			OutputTokens:      summaryRow.OutputTokens,
			ReasoningTokens:   summaryRow.ReasoningTokens,
			TotalTokens:       summaryRow.TotalTokens,
		},
		Records: records,
	}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }
