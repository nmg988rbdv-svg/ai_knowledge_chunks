package rag

import (
	"context"
)

// Chunk 是写入向量数据库的最小知识单元。
type Chunk struct {
	DocumentID string
	Index      int
	Text       string
	Vector     []float64
}

// VectorStore 隔离 RAG 业务与具体向量数据库。
type VectorStore interface {
	ReplaceDocument(ctx context.Context, documentID string, chunks []Chunk) error
	Search(ctx context.Context, queryVector []float64, topK int) ([]SearchResult, error)
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}
