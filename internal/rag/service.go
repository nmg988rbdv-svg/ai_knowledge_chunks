package rag

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	embeddingBatchSize = 64
)

// SearchResult 是向量检索返回给上层的文档片段和相似度。
type SearchResult struct {
	DocumentID string  `json:"document_id"`
	ChunkIndex int     `json:"chunk_index"`
	Text       string  `json:"text"`
	Score      float64 `json:"score"`
}

// Service 编排切块、Embedding 与向量存储。
type Service struct {
	embedder Embedder
	store    VectorStore
	splitter Splitter
}

// NewServiceWithStore 创建一个 Service 实例，并确保嵌入器、向量存储和切分器均已提供。
func NewServiceWithStore(embedder Embedder, store VectorStore, splitter Splitter) (*Service, error) {

	// 校验输入参数，确保嵌入器、向量存储和切分器均已提供。
	if embedder == nil || store == nil || splitter == nil {
		return nil, errors.New("embedder, vector store, and splitter are required")
	}
	return &Service{embedder: embedder, store: store, splitter: splitter}, nil
}

// AddDocument 切分并批量向量化文档；成功后原子替换相同 ID 的旧片段。
func (s *Service) AddDocument(ctx context.Context, documentID, text string) (int, error) {

	// 验证输入参数，确保文档 ID 和文本内容不为空。
	documentID = strings.TrimSpace(documentID)
	text = strings.TrimSpace(text)
	if documentID == "" || text == "" {
		return 0, errors.New("document ID and text must not be empty")
	}

	// 使用切分器将文本切分为多个片段。
	parts, err := s.splitter.Split(text)
	if err != nil {
		return 0, fmt.Errorf("split document: %w", err)
	}
	// 调用 embedInBatches 方法对切分后的文本片段进行批量向量化。
	vectors, err := s.embedInBatches(ctx, parts)
	if err != nil {
		return 0, fmt.Errorf("embed document: %w", err)
	}
	// 验证向量列表的数量和每个向量的维度是否符合预期。
	if err := validateVectors(vectors, len(parts)); err != nil {
		return 0, err
	}

	// 构建新的文档片段列表，每个片段包含文档 ID、索引、文本和向量。
	newChunks := make([]Chunk, len(parts))
	for index := range parts {
		newChunks[index] = Chunk{
			DocumentID: documentID,
			Index:      index,
			Text:       parts[index],
			Vector:     vectors[index],
		}
	}

	// 将新的文档片段列表替换存储中相同文档 ID 的旧片段。
	if err := s.store.ReplaceDocument(ctx, documentID, newChunks); err != nil {
		return 0, fmt.Errorf("store document: %w", err)
	}
	return len(newChunks), nil
}

// embedInBatches 将文本切分为批次并调用嵌入器生成向量表示，返回所有文本的向量列表。
func (s *Service) embedInBatches(ctx context.Context, texts []string) ([][]float64, error) {

	// 初始化用于存储所有文本片段向量的切片。
	vectors := make([][]float64, 0, len(texts))
	for start := 0; start < len(texts); start += embeddingBatchSize {
		// 计算当前批次的结束索引，确保不会超出文本片段列表的范围。
		end := start + embeddingBatchSize
		if end > len(texts) {
			end = len(texts)
		}
		// 调用嵌入器对当前批次的文本片段生成向量表示。
		batch, err := s.embedder.Embed(ctx, texts[start:end])
		if err != nil {
			return nil, err
		}
		// 验证当前批次的向量数量是否与文本片段数量一致。
		if len(batch) != end-start {
			return nil, fmt.Errorf("embedding batch count = %d, want %d", len(batch), end-start)
		}
		// 将当前批次的向量追加到总向量列表中。
		vectors = append(vectors, batch...)
	}
	return vectors, nil
}

// Search 将查询向量与所有文档片段计算余弦相似度，并返回分数最高的 topK 项。
func (s *Service) Search(ctx context.Context, query string, topK int) ([]SearchResult, error) {
	// 去除查询字符串的首尾空白字符，并确保其非空。
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("query must not be empty")
	}
	// 确保 topK 参数大于零。
	if topK <= 0 {
		return nil, errors.New("topK must be greater than zero")
	}
	// 将查询文本转换为向量表示，用于后续的相似度搜索。
	vectors, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	// 验证查询向量是否有效。
	if err := validateVectors(vectors, 1); err != nil {
		return nil, err
	}
	// 执行向量搜索，获取与查询向量最相似的文档片段。
	results, err := s.store.Search(ctx, vectors[0], topK)
	if err != nil {
		return nil, fmt.Errorf("search vector store: %w", err)
	}
	return results, nil
}

// validateVectors 验证向量列表的数量和每个向量的维度是否符合预期。
func validateVectors(vectors [][]float64, expected int) error {

	// 验证向量列表的数量是否与预期一致。
	if len(vectors) != expected {
		return fmt.Errorf("embedding count = %d, want %d", len(vectors), expected)
	}

	// 验证每个向量的维度是否一致。
	dimensions := 0
	for index, vector := range vectors {
		if len(vector) == 0 {
			return fmt.Errorf("embedding %d is empty", index)
		}
		if dimensions == 0 {
			dimensions = len(vector)
		} else if len(vector) != dimensions {
			return fmt.Errorf("embedding %d has %d dimensions, want %d", index, len(vector), dimensions)
		}
	}
	return nil
}

func (s *Service) Ping(ctx context.Context) error  { return s.store.Ping(ctx) }
func (s *Service) Close(ctx context.Context) error { return s.store.Close(ctx) }
