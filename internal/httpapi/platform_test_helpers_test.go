package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"llm-api-demo/internal/ingest"
	"llm-api-demo/internal/llm"
	"llm-api-demo/internal/rag"
)

type testConversationStore struct {
	mu    sync.Mutex
	items map[string]conversationState
}

func newTestConversationStore() *testConversationStore {
	return &testConversationStore{items: make(map[string]conversationState)}
}

func (s *testConversationStore) Create(context.Context) (string, error) {
	id, err := randomIdentifier("conv_")
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.items[id] = conversationState{}
	s.mu.Unlock()
	return id, nil
}

func (s *testConversationStore) Get(_ context.Context, id string) (conversationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.items[id]
	if !ok {
		return conversationState{}, errConversationNotFound
	}
	return state, nil
}

func (s *testConversationStore) Save(_ context.Context, id string, state conversationState) error {
	s.mu.Lock()
	s.items[id] = state
	s.mu.Unlock()
	return nil
}

func (s *testConversationStore) Delete(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return false, nil
	}
	delete(s.items, id)
	return true, nil
}

func (s *testConversationStore) Lock(context.Context, string, time.Duration) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

func (*testConversationStore) Ping(context.Context) error { return nil }
func (*testConversationStore) Close() error               { return nil }

func newTestHandler(generator llm.Generator, timeout time.Duration, logger *slog.Logger) http.Handler {
	return NewPlatformHandler(generator, newTestConversationStore(), nil, nil, timeout, logger)
}

func newTestHandlerWithKnowledge(generator llm.Generator, knowledge *rag.Service, timeout time.Duration, logger *slog.Logger) http.Handler {
	return NewPlatformHandler(generator, newTestConversationStore(), knowledge, nil, timeout, logger)
}

type testVectorStore struct {
	mu     sync.RWMutex
	chunks []rag.Chunk
}

func (s *testVectorStore) ReplaceDocument(_ context.Context, documentID string, chunks []rag.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.chunks[:0]
	for _, chunk := range s.chunks {
		if chunk.DocumentID != documentID {
			kept = append(kept, chunk)
		}
	}
	s.chunks = append(kept, chunks...)
	return nil
}

func (s *testVectorStore) Search(_ context.Context, query []float64, topK int) ([]rag.SearchResult, error) {
	s.mu.RLock()
	chunks := append([]rag.Chunk(nil), s.chunks...)
	s.mu.RUnlock()
	results := make([]rag.SearchResult, 0, len(chunks))
	for _, chunk := range chunks {
		score, err := testCosine(query, chunk.Vector)
		if err != nil {
			return nil, err
		}
		results = append(results, rag.SearchResult{
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

func (*testVectorStore) Ping(context.Context) error  { return nil }
func (*testVectorStore) Close(context.Context) error { return nil }

type testSplitter struct{}

func (testSplitter) Split(text string) ([]string, error) { return []string{text}, nil }

func newTestKnowledgeService(t *testing.T, embedder rag.Embedder) *rag.Service {
	t.Helper()
	service, err := rag.NewServiceWithStore(embedder, &testVectorStore{}, testSplitter{})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type testJobStatusStore struct {
	mu    sync.RWMutex
	items map[string]ingest.JobStatus
}

func newTestJobStatusStore() *testJobStatusStore {
	return &testJobStatusStore{items: make(map[string]ingest.JobStatus)}
}

func (s *testJobStatusStore) Set(_ context.Context, status ingest.JobStatus) error {
	s.mu.Lock()
	s.items[status.ID] = status
	s.mu.Unlock()
	return nil
}

func (s *testJobStatusStore) Get(_ context.Context, id string) (ingest.JobStatus, error) {
	s.mu.RLock()
	status, ok := s.items[id]
	s.mu.RUnlock()
	if !ok {
		return ingest.JobStatus{}, ingest.ErrJobNotFound
	}
	return status, nil
}

func (*testJobStatusStore) Ping(context.Context) error { return nil }
func (*testJobStatusStore) Close() error               { return nil }

type testJobPublisher struct {
	handle func(context.Context, ingest.DocumentJob) error
}

func (p *testJobPublisher) Publish(ctx context.Context, job ingest.DocumentJob) error {
	err := p.handle(ctx, job)
	if ingest.IsPermanentJobError(err) {
		return nil
	}
	return err
}

func (*testJobPublisher) Ping(context.Context) error { return nil }
func (*testJobPublisher) Close() error               { return nil }

func testCosine(left, right []float64) (float64, error) {
	if len(left) == 0 || len(left) != len(right) {
		return 0, errors.New("vectors must have equal dimensions")
	}
	var dot, leftNorm, rightNorm float64
	for i := range left {
		dot += left[i] * right[i]
		leftNorm += left[i] * left[i]
		rightNorm += right[i] * right[i]
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm)), nil
}
