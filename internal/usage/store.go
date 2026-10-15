package usage

import (
	"context"
	"encoding/json"
	"fmt"

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

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }
