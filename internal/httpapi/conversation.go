package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const conversationMaxTurns = 10

var errConversationNotFound = errors.New("conversation not found")

type conversationState struct {
	PreviousResponseID string `json:"previous_response_id"`
	TotalTurns         int    `json:"total_turns"`
	ContextTurns       int    `json:"context_turns"`
}

// ConversationStore 保存跨实例共享的会话状态，并为同一会话提供分布式互斥。
type ConversationStore interface {
	Create(ctx context.Context) (string, error)
	Get(ctx context.Context, id string) (conversationState, error)
	Save(ctx context.Context, id string, state conversationState) error
	Delete(ctx context.Context, id string) (bool, error)
	Lock(ctx context.Context, id string, ttl time.Duration) (func(context.Context) error, error)
	Ping(ctx context.Context) error
	Close() error
}

// conversationKey 生成 Redis 中存储会话状态的键。
type RedisConversationStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisConversationStore 创建一个基于 Redis 的会话存储实例。
func NewRedisConversationStore(ctx context.Context, address, password string, database int, ttl time.Duration) (*RedisConversationStore, error) {

	// 验证 Redis 地址和会话 TTL 是否有效。
	if address == "" || ttl <= 0 {
		return nil, errors.New("Redis address and positive conversation TTL are required")
	}
	store := &RedisConversationStore{

		// 初始化 Redis 客户端并设置会话 TTL。
		client: redis.NewClient(&redis.Options{Addr: address, Password: password, DB: database}),
		ttl:    ttl,
	}

	// 测试与 Redis 的连接，确保会话存储可用。
	if err := store.client.Ping(ctx).Err(); err != nil {
		_ = store.client.Close()
		return nil, fmt.Errorf("connect Redis conversation store: %w", err)
	}
	return store, nil
}

// Create 在 Redis 中创建一个新的会话，并返回会话 ID。
func (s *RedisConversationStore) Create(ctx context.Context) (string, error) {

	// 初始化一个空的会话状态，并将其序列化为 JSON。
	data, err := json.Marshal(conversationState{})
	if err != nil {
		return "", err
	}
	for {
		id, err := randomIdentifier("conv_")
		if err != nil {
			return "", err
		}
		// 尝试在 Redis 中创建该会话，如果已存在则重新生成 ID。
		created, err := s.client.SetNX(ctx, conversationKey(id), data, s.ttl).Result()
		if err != nil {
			return "", err
		}
		if created {
			return id, nil
		}
	}
}

// Get 从 Redis 中获取指定会话的状态，如果会话不存在则返回 errConversationNotFound。
func (s *RedisConversationStore) Get(ctx context.Context, id string) (conversationState, error) {
	// 从 Redis 中获取会话数据，如果不存在则返回 errConversationNotFound。
	data, err := s.client.Get(ctx, conversationKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return conversationState{}, errConversationNotFound
	}
	if err != nil {
		return conversationState{}, err
	}
	var state conversationState
	if err := json.Unmarshal(data, &state); err != nil {
		return conversationState{}, fmt.Errorf("decode conversation state: %w", err)
	}
	return state, nil
}

// Save 将指定会话的状态保存到 Redis 中，如果会话不存在则创建新的会话。
func (s *RedisConversationStore) Save(ctx context.Context, id string, state conversationState) error {

	// 将会话状态序列化为 JSON。
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// 将序列化后的会话状态保存到 Redis 中，如果会话不存在则创建新的会话。
	return s.client.Set(ctx, conversationKey(id), data, s.ttl).Err()
}

// Delete 删除指定会话的状态，如果会话不存在则返回 false。
func (s *RedisConversationStore) Delete(ctx context.Context, id string) (bool, error) {
	deleted, err := s.client.Del(ctx, conversationKey(id)).Result()
	return deleted > 0, err
}

// Lock 尝试为指定会话加锁，返回一个解锁函数和可能的错误。
func (s *RedisConversationStore) Lock(ctx context.Context, id string, ttl time.Duration) (func(context.Context) error, error) {
	// 检查锁的 TTL 是否有效。
	if ttl <= 0 {
		return nil, errors.New("conversation lock TTL must be positive")
	}
	// 生成一个唯一的锁标识符，用于后续解锁时验证。
	token, err := randomIdentifier("lock_")
	if err != nil {
		return nil, err
	}
	// 构造锁的 Redis 键名。
	key := conversationLockKey(id)

	// 尝试获取锁的循环开始前，初始化一个 ticker 用于定期重试。
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	// 使用固定间隔的 ticker 轮询尝试获取锁。
	for {
		// 尝试获取锁，如果失败则等待下一次 ticker 触发。
		acquired, err := s.client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return nil, err
		}
		// 如果未获取到锁，则继续等待下一次尝试。
		if acquired {
			return func(unlockCtx context.Context) error {
				// 使用 Lua 脚本确保只有持有锁的客户端才能解锁，避免误删其他客户端的锁。
				const script = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
				return s.client.Eval(unlockCtx, script, []string{key}, token).Err()
			}, nil
		}
		// 等待下一次尝试获取锁的时间间隔。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Ping 检查 Redis 连接是否可用，Close 关闭 Redis 客户端连接。
func (s *RedisConversationStore) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }
func (s *RedisConversationStore) Close() error                   { return s.client.Close() }

// conversationKey 和 conversationLockKey 分别生成会话存储和会话锁在 Redis 中的键名。
func conversationKey(id string) string     { return "ai-platform:conversation:" + id }
func conversationLockKey(id string) string { return "ai-platform:conversation-lock:" + id }

// randomIdentifier 生成一个带有指定前缀的随机标识符。
func randomIdentifier(prefix string) (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(data), nil
}
