package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStatusStore 是基于 Redis 的任务状态存储实现。
type RedisStatusStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStatusStore 创建一个 RedisStatusStore 实例，并确保 Redis 地址和 TTL 合法。
func NewRedisStatusStore(address, password string, database int, ttl time.Duration) (*RedisStatusStore, error) {

	// 校验 Redis 地址和 TTL 是否有效。
	if address == "" || ttl <= 0 {
		return nil, fmt.Errorf("Redis address and positive job TTL are required")
	}
	return &RedisStatusStore{
		client: redis.NewClient(&redis.Options{Addr: address, Password: password, DB: database}),
		ttl:    ttl,
	}, nil
}

// Set 将任务的当前状态保存到 Redis 中，使用任务 ID 作为键，状态对象的 JSON 编码作为值，并设置 TTL。
func (s *RedisStatusStore) Set(ctx context.Context, status JobStatus) error {
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, "ai-platform:ingest:"+status.ID, data, s.ttl).Err()
}

// Get 根据任务 ID 查询任务的当前状态。
func (s *RedisStatusStore) Get(ctx context.Context, id string) (JobStatus, error) {
	data, err := s.client.Get(ctx, "ai-platform:ingest:"+id).Bytes()
	if err == redis.Nil {
		return JobStatus{}, ErrJobNotFound
	}
	if err != nil {
		return JobStatus{}, err
	}
	var status JobStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return JobStatus{}, err
	}
	return status, nil
}

func (s *RedisStatusStore) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }
func (s *RedisStatusStore) Close() error                   { return s.client.Close() }
