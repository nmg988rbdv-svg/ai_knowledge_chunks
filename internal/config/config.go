package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 保存启动服务及调用模型所需的配置。
type Config struct {
	APIKey           string        // 服务端访问模型 API 的凭据，不应写入日志或响应。
	Model            string        // 发起 Responses API 请求时使用的模型名称。
	EmbeddingModel   string        // 文档和查询向量化使用的 Embedding 模型。
	HTTPAddr         string        // HTTP 监听地址，例如 :8080 或 127.0.0.1:8080。
	LLMTimeout       time.Duration // 单次模型调用允许的最长时间。
	MilvusAddress    string        // Milvus 服务地址，例如 127.0.0.1:19530
	MilvusAPIKey     string        // Milvus api key
	MilvusCollection string        // Milvus 集合名称
	MilvusDimension  int           // Milvus 向量维度
	RedisAddress     string        // Redis 服务地址，例如 127.0.0.1:6379
	RedisPassword    string        // Redis 密码
	RedisDatabase    int           // Redis 数据库索引
	KafkaBrokers     []string      // Kafka broker 列表，逗号分隔
	KafkaTopic       string        // Kafka 主题
	KafkaGroup       string        // Kafka 消费组
	UsageKafkaTopic  string        // 模型用量事件主题
	PostgresDSN      string        // Token 用量查询使用的 PostgreSQL 连接串。
	ChunkSize        int           // RAG 文档切分块大小
	ChunkOverlap     int           // RAG 文档切分重叠大小
	JobTTL           time.Duration // 异步入库任务的 TTL
	ConversationTTL  time.Duration // Redis 会话状态的 TTL
}

// Load 从环境变量读取配置，并在启动前校验必填项和超时范围。
func Load() (Config, error) {
	// 先解析超时，确保无效格式在启动阶段就能被清晰报告。
	timeout, err := time.ParseDuration(envOrDefault("LLM_TIMEOUT", "30s"))
	if err != nil {
		return Config{}, errors.New("LLM_TIMEOUT must be a valid duration, for example 30s")
	}
	jobTTL, err := time.ParseDuration(envOrDefault("JOB_TTL", "24h"))
	if err != nil {
		return Config{}, errors.New("JOB_TTL must be a valid duration, for example 24h")
	}
	conversationTTL, err := time.ParseDuration(envOrDefault("CONVERSATION_TTL", "24h"))
	if err != nil {
		return Config{}, errors.New("CONVERSATION_TTL must be a valid duration, for example 24h")
	}
	milvusDimension, err := envInt("MILVUS_DIMENSION", 1536)
	if err != nil {
		return Config{}, err
	}
	redisDatabase, err := envInt("REDIS_DATABASE", 0)
	if err != nil {
		return Config{}, err
	}
	chunkSize, err := envInt("RAG_CHUNK_SIZE", 800)
	if err != nil {
		return Config{}, err
	}
	chunkOverlap, err := envInt("RAG_CHUNK_OVERLAP", 120)
	if err != nil {
		return Config{}, err
	}

	// 构建配置对象。
	cfg := Config{
		APIKey:           os.Getenv("OPENAI_API_KEY"),
		Model:            envOrDefault("OPENAI_MODEL", "gpt-5.2"),
		EmbeddingModel:   envOrDefault("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
		HTTPAddr:         envOrDefault("HTTP_ADDR", ":8080"),
		LLMTimeout:       timeout,
		MilvusAddress:    envOrDefault("MILVUS_ADDRESS", "127.0.0.1:19530"),
		MilvusAPIKey:     envOrDefault("MILVUS_API_KEY", "root:Milvus"),
		MilvusCollection: envOrDefault("MILVUS_COLLECTION", "ai_knowledge_chunks"),
		MilvusDimension:  milvusDimension,
		RedisAddress:     envOrDefault("REDIS_ADDRESS", "127.0.0.1:6379"),
		RedisPassword:    os.Getenv("REDIS_PASSWORD"),
		RedisDatabase:    redisDatabase,
		KafkaBrokers:     splitNonEmpty(envOrDefault("KAFKA_BROKERS", "127.0.0.1:9092")),
		KafkaTopic:       envOrDefault("KAFKA_TOPIC", "knowledge.ingest.v1"),
		KafkaGroup:       envOrDefault("KAFKA_GROUP", "ai-knowledge-workers"),
		UsageKafkaTopic:  envOrDefault("USAGE_KAFKA_TOPIC", "llm.usage.v1"),
		PostgresDSN:      envOrDefault("POSTGRES_DSN", "postgres://ai_platform:ai-platform-local@127.0.0.1:15432/ai_platform?sslmode=disable"),
		ChunkSize:        chunkSize,
		ChunkOverlap:     chunkOverlap,
		JobTTL:           jobTTL,
		ConversationTTL:  conversationTTL,
	}

	// 验证关键配置项，确保服务启动时配置有效。
	if cfg.APIKey == "" {
		return Config{}, errors.New("OPENAI_API_KEY is required")
	}
	if cfg.LLMTimeout <= 0 {
		return Config{}, errors.New("LLM_TIMEOUT must be greater than zero")
	}
	if cfg.MilvusDimension <= 0 || cfg.ChunkSize <= 0 || cfg.ChunkOverlap < 0 || cfg.ChunkOverlap >= cfg.ChunkSize {
		return Config{}, errors.New("Milvus dimension and RAG chunk settings are invalid")
	}
	if cfg.JobTTL <= 0 || cfg.ConversationTTL <= 0 || len(cfg.KafkaBrokers) == 0 || cfg.UsageKafkaTopic == "" || cfg.PostgresDSN == "" {
		return Config{}, errors.New("JOB_TTL, CONVERSATION_TTL, KAFKA_BROKERS, USAGE_KAFKA_TOPIC, and POSTGRES_DSN must be configured")
	}

	return cfg, nil
}

// UsageWorkerConfig 只包含用量消费者需要的最小权限配置，不需要 OpenAI API Key。
type UsageWorkerConfig struct {
	PostgresDSN  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroup   string
}

// LoadUsageWorker 加载独立用量消费者配置。
func LoadUsageWorker() (UsageWorkerConfig, error) {
	cfg := UsageWorkerConfig{
		PostgresDSN:  envOrDefault("POSTGRES_DSN", "postgres://ai_platform:ai-platform-local@127.0.0.1:15432/ai_platform?sslmode=disable"),
		KafkaBrokers: splitNonEmpty(envOrDefault("KAFKA_BROKERS", "127.0.0.1:9092")),
		KafkaTopic:   envOrDefault("USAGE_KAFKA_TOPIC", "llm.usage.v1"),
		KafkaGroup:   envOrDefault("USAGE_KAFKA_GROUP", "ai-usage-workers"),
	}
	if cfg.PostgresDSN == "" || len(cfg.KafkaBrokers) == 0 || cfg.KafkaTopic == "" || cfg.KafkaGroup == "" {
		return UsageWorkerConfig{}, errors.New("POSTGRES_DSN, KAFKA_BROKERS, USAGE_KAFKA_TOPIC, and USAGE_KAFKA_GROUP must be configured")
	}
	return cfg, nil
}

// envInt 输入类型为int的环境变量进行类型转换
func envInt(key string, fallback int) (int, error) {
	// 把整数转成字符串。
	value := envOrDefault(key, strconv.Itoa(fallback))
	// 尝试将字符串解析为整数。
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return parsed, nil
}

// envDuration 输入类型为time.Duration的环境变量进行类型转换
func splitNonEmpty(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

// envOrDefault 在环境变量未设置或为空字符串时返回默认值。
func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
