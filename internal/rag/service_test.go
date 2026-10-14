package rag

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
)

type testMemoryVectorStore struct {
	chunks []Chunk
}

func (s *testMemoryVectorStore) ReplaceDocument(_ context.Context, documentID string, chunks []Chunk) error {
	kept := s.chunks[:0]
	for _, chunk := range s.chunks {
		if chunk.DocumentID != documentID {
			kept = append(kept, chunk)
		}
	}
	s.chunks = append(kept, chunks...)
	return nil
}

func (s *testMemoryVectorStore) Search(_ context.Context, query []float64, topK int) ([]SearchResult, error) {
	results := make([]SearchResult, 0, len(s.chunks))
	for _, chunk := range s.chunks {
		score, err := testCosineSimilarity(query, chunk.Vector)
		if err != nil {
			return nil, err
		}
		results = append(results, SearchResult{
			DocumentID: chunk.DocumentID,
			ChunkIndex: chunk.Index,
			Text:       chunk.Text,
			Score:      score,
		})
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	if topK < len(results) {
		results = results[:topK]
	}
	return results, nil
}

func (*testMemoryVectorStore) Ping(context.Context) error  { return nil }
func (*testMemoryVectorStore) Close(context.Context) error { return nil }

type wholeTextSplitter struct{}

func (wholeTextSplitter) Split(text string) ([]string, error) { return []string{text}, nil }

type runeTestSplitter struct{}

func (runeTestSplitter) Split(text string) ([]string, error) {
	runes := []rune(text)
	parts := make([]string, len(runes))
	for i, value := range runes {
		parts[i] = string(value)
	}
	return parts, nil
}

func newTestService(t *testing.T, embedder Embedder, splitter Splitter) *Service {
	t.Helper()
	service, err := NewServiceWithStore(embedder, &testMemoryVectorStore{}, splitter)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type fakeEmbedder struct{}

func (fakeEmbedder) Embed(_ context.Context, texts []string) ([][]float64, error) {
	vectors := make([][]float64, len(texts))
	for index, text := range texts {
		switch {
		case strings.Contains(strings.ToLower(text), "go"), strings.Contains(text, "并发"):
			vectors[index] = []float64{1, 0}
		case strings.Contains(strings.ToLower(text), "php"):
			vectors[index] = []float64{0, 1}
		default:
			vectors[index] = []float64{0.5, 0.5}
		}
	}
	return vectors, nil
}

func TestLangChainSplitterRejectsInvalidOverlap(t *testing.T) {
	if _, err := NewLangChainSplitter(100, 100); err == nil {
		t.Fatal("expected invalid overlap error")
	}
}

func TestSearchReturnsMostSimilarDocument(t *testing.T) {
	service := newTestService(t, fakeEmbedder{}, wholeTextSplitter{})
	if _, err := service.AddDocument(context.Background(), "go-doc", "Go 使用 goroutine 支持并发编程。"); err != nil {
		t.Fatalf("add Go document: %v", err)
	}
	if _, err := service.AddDocument(context.Background(), "php-doc", "PHP 常用于 Web 后端开发。"); err != nil {
		t.Fatalf("add PHP document: %v", err)
	}

	results, err := service.Search(context.Background(), "如何进行并发编程？", 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].DocumentID != "go-doc" || results[0].Score != 1 {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestAddDocumentReplacesExistingDocument(t *testing.T) {
	service := newTestService(t, fakeEmbedder{}, wholeTextSplitter{})
	if _, err := service.AddDocument(context.Background(), "doc", "PHP 文档"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AddDocument(context.Background(), "doc", "Go 并发文档"); err != nil {
		t.Fatal(err)
	}

	results, err := service.Search(context.Background(), "Go", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Text != "Go 并发文档" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

type batchTrackingEmbedder struct {
	calls    int
	maxBatch int
}

func (e *batchTrackingEmbedder) Embed(_ context.Context, texts []string) ([][]float64, error) {
	e.calls++
	if len(texts) > e.maxBatch {
		e.maxBatch = len(texts)
	}
	vectors := make([][]float64, len(texts))
	for i := range vectors {
		vectors[i] = []float64{1, 0}
	}
	return vectors, nil
}

func TestAddDocumentBatchesEmbeddingRequests(t *testing.T) {
	embedder := &batchTrackingEmbedder{}
	service := newTestService(t, embedder, runeTestSplitter{})
	chunks, err := service.AddDocument(context.Background(), "large", strings.Repeat("a", embeddingBatchSize+1))
	if err != nil {
		t.Fatal(err)
	}
	if chunks != embeddingBatchSize+1 || embedder.calls != 2 || embedder.maxBatch != embeddingBatchSize {
		t.Fatalf("chunks=%d calls=%d max_batch=%d", chunks, embedder.calls, embedder.maxBatch)
	}
}

func testCosineSimilarity(left, right []float64) (float64, error) {
	if len(left) == 0 || len(left) != len(right) {
		return 0, errors.New("vectors must have equal non-zero dimensions")
	}
	var dot, leftNorm, rightNorm float64
	for i := range left {
		dot += left[i] * right[i]
		leftNorm += left[i] * left[i]
		rightNorm += right[i] * right[i]
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm)), nil
}
