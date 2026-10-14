package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type testStatusStore struct {
	mu    sync.RWMutex
	items map[string]JobStatus
}

func newTestStatusStore() *testStatusStore {
	return &testStatusStore{items: make(map[string]JobStatus)}
}

func (s *testStatusStore) Set(_ context.Context, status JobStatus) error {
	s.mu.Lock()
	s.items[status.ID] = status
	s.mu.Unlock()
	return nil
}

func (s *testStatusStore) Get(_ context.Context, id string) (JobStatus, error) {
	s.mu.RLock()
	status, ok := s.items[id]
	s.mu.RUnlock()
	if !ok {
		return JobStatus{}, ErrJobNotFound
	}
	return status, nil
}

func (*testStatusStore) Ping(context.Context) error { return nil }
func (*testStatusStore) Close() error               { return nil }

type testPublisher struct {
	handle func(context.Context, DocumentJob) error
}

func (p *testPublisher) Publish(ctx context.Context, job DocumentJob) error {
	err := p.handle(ctx, job)
	if IsPermanentJobError(err) {
		return nil
	}
	return err
}

func (*testPublisher) Ping(context.Context) error { return nil }
func (*testPublisher) Close() error               { return nil }

func TestQueueAndProcessorLifecycle(t *testing.T) {
	statuses := newTestStatusStore()
	processor, err := NewProcessor(statuses, func(_ context.Context, documentID, text string) (int, error) {
		if documentID != "doc-1" || text != "hello" {
			t.Fatalf("unexpected document: %q %q", documentID, text)
		}
		return 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewQueue(&testPublisher{handle: processor.Handle}, statuses)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := queue.Submit(context.Background(), "doc-1", "hello")
	if err != nil {
		t.Fatal(err)
	}
	status, err := queue.Get(context.Background(), queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateCompleted || status.Chunks != 2 {
		t.Fatalf("status=%#v", status)
	}
}

func TestStatusStoreMissingJob(t *testing.T) {
	_, err := newTestStatusStore().Get(context.Background(), "missing")
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("error=%v", err)
	}
}

func TestFailedIndexingStoresSafeTerminalStatus(t *testing.T) {
	statuses := newTestStatusStore()
	processor, err := NewProcessor(statuses, func(context.Context, string, string) (int, error) {
		return 0, errors.New("provider rejected secret credential")
	})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewQueue(&testPublisher{handle: processor.Handle}, statuses)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := queue.Submit(context.Background(), "doc-1", "hello")
	if err != nil {
		t.Fatal(err)
	}
	status, err := queue.Get(context.Background(), queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateFailed || status.Error != "document indexing failed" {
		t.Fatalf("status=%#v", status)
	}
}
