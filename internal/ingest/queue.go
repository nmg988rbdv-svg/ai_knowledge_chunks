package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type JobState string

const (
	StateQueued     JobState = "queued"
	StateProcessing JobState = "processing"
	StateCompleted  JobState = "completed"
	StateFailed     JobState = "failed"
)

type DocumentJob struct {
	ID         string    `json:"id"`
	DocumentID string    `json:"document_id"`
	Text       string    `json:"text"`
	CreatedAt  time.Time `json:"created_at"`
}

type JobStatus struct {
	ID         string    `json:"id"`
	DocumentID string    `json:"document_id"`
	State      JobState  `json:"state"`
	Chunks     int       `json:"chunks,omitempty"`
	Error      string    `json:"error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

var ErrJobNotFound = errors.New("ingestion job not found")

// PermanentJobError 表示消息本身或外部模型调用已明确失败。
// worker 会记录失败状态并提交 Kafka offset，避免一条毒消息无限阻塞分区。
type PermanentJobError struct {
	err error
}

func (e *PermanentJobError) Error() string { return e.err.Error() }
func (e *PermanentJobError) Unwrap() error { return e.err }

// IsPermanentJobError 判断给定的错误是否为 PermanentJobError 类型。
func IsPermanentJobError(err error) bool {
	var target *PermanentJobError
	return errors.As(err, &target)
}

type Publisher interface {
	Publish(ctx context.Context, job DocumentJob) error
	Ping(ctx context.Context) error
	Close() error
}

type StatusStore interface {
	Set(ctx context.Context, status JobStatus) error
	Get(ctx context.Context, id string) (JobStatus, error)
	Ping(ctx context.Context) error
	Close() error
}

// Queue 在 HTTP 层与 Kafka/Redis 之间提供稳定的异步任务接口。
type Queue struct {
	publisher Publisher
	statuses  StatusStore
}

// NewQueue 创建一个任务队列实例，负责将文档提交到 Kafka 并跟踪任务状态。
func NewQueue(publisher Publisher, statuses StatusStore) (*Queue, error) {

	// 确保传入的发布者和状态存储不为空。
	if publisher == nil || statuses == nil {
		return nil, errors.New("publisher and status store are required")
	}
	return &Queue{publisher: publisher, statuses: statuses}, nil
}

// Submit 将文档提交到任务队列，并返回初始的任务状态。
func (q *Queue) Submit(ctx context.Context, documentID, text string) (JobStatus, error) {
	// 规范化文档 ID 和文本内容，去除首尾空白，并确保非空。
	documentID = strings.TrimSpace(documentID)
	text = strings.TrimSpace(text)
	if documentID == "" || text == "" {
		return JobStatus{}, errors.New("document ID and text must not be empty")
	}
	// 生成一个新的任务 ID，用于标识该文档的 ingestion 作业。
	id, err := newJobID()
	if err != nil {
		return JobStatus{}, err
	}
	// 记录当前时间，用于设置任务的创建时间和初始状态的更新时间。
	now := time.Now().UTC()
	// 构建文档任务和初始任务状态对象。
	job := DocumentJob{ID: id, DocumentID: documentID, Text: text, CreatedAt: now}
	status := JobStatus{ID: id, DocumentID: documentID, State: StateQueued, UpdatedAt: now}
	// 将初始任务状态保存到状态存储中，以便后续查询任务状态。
	if err := q.statuses.Set(ctx, status); err != nil {
		return JobStatus{}, fmt.Errorf("save queued status: %w", err)
	}
	// 将文档任务发布到消息队列，以便 worker 可以异步处理该任务。
	if err := q.publisher.Publish(ctx, job); err != nil {
		status.State = StateFailed
		status.Error = "failed to publish ingestion job"
		status.UpdatedAt = time.Now().UTC()
		_ = q.statuses.Set(context.Background(), status)
		return JobStatus{}, fmt.Errorf("publish ingestion job: %w", err)
	}
	return status, nil
}

// Get 根据任务 ID 查询任务的当前状态。
func (q *Queue) Get(ctx context.Context, id string) (JobStatus, error) {
	return q.statuses.Get(ctx, strings.TrimSpace(id))
}

func (q *Queue) Ping(ctx context.Context) error {
	return errors.Join(q.publisher.Ping(ctx), q.statuses.Ping(ctx))
}

func (q *Queue) Close() error {
	return errors.Join(q.publisher.Close(), q.statuses.Close())
}

// Get 根据任务 ID 查询任务的当前状态。
func newJobID() (string, error) {
	data := make([]byte, 16)
	// 生成一个新的唯一任务 ID，用于标识该文档的 ingestion 作业。
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	// 将生成的字节数组编码为十六进制字符串，并加上前缀 "job_" 作为最终的任务 ID。
	return "job_" + hex.EncodeToString(data), nil
}

// Processor 是 Kafka worker 调用的文档入库用例。
type Processor struct {
	statuses StatusStore
	index    func(context.Context, string, string) (int, error)
}

// NewProcessor 创建一个文档处理器实例，负责将文档分片并入库，同时更新任务状态。
func NewProcessor(statuses StatusStore, index func(context.Context, string, string) (int, error)) (*Processor, error) {

	// 验证传入的状态存储和索引函数是否有效，确保文档处理器能够正常工作。
	if statuses == nil || index == nil {
		return nil, errors.New("status store and index function are required")
	}
	return &Processor{statuses: statuses, index: index}, nil
}

// Handle 处理文档任务，将文档分片并入库，同时更新任务状态。
func (p *Processor) Handle(ctx context.Context, job DocumentJob) error {
	// 将文档任务标记为处理中，并更新任务状态存储。
	status := JobStatus{ID: job.ID, DocumentID: job.DocumentID, State: StateProcessing, UpdatedAt: time.Now().UTC()}
	// 保存任务状态为处理中，以便外部系统能够及时获取任务的最新状态。
	if err := p.statuses.Set(ctx, status); err != nil {
		return fmt.Errorf("mark job processing: %w", err)
	}
	// 调用索引函数将文档分片并入库，获取生成的分片数量。
	chunks, err := p.index(ctx, job.DocumentID, job.Text)
	status.UpdatedAt = time.Now().UTC()
	if err != nil {
		status.State = StateFailed
		// 对外状态只保留稳定信息，供应商响应、密钥或内部地址不能进入 API。
		status.Error = "document indexing failed"
		if statusErr := p.statuses.Set(context.Background(), status); statusErr != nil {
			return fmt.Errorf("save failed job status: %w", statusErr)
		}
		return &PermanentJobError{err: err}
	}
	status.State = StateCompleted
	status.Chunks = chunks
	status.Error = ""
	// 保存任务状态为已完成，以便外部系统能够及时获取任务的最新状态。
	return p.statuses.Set(ctx, status)
}
