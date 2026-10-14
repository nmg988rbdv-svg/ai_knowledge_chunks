package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llm-api-demo/internal/config"
	"llm-api-demo/internal/ingest"
	"llm-api-demo/internal/rag"
)

func main() {

	// 初始化日志记录器。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	// 创建上下文，用于启动期间的初始化操作。
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 60*time.Second)

	// 创建文本切分器实例。
	splitter, err := rag.NewLangChainSplitter(cfg.ChunkSize, cfg.ChunkOverlap)
	if err != nil {
		logger.Error("create splitter", "error", err)
		os.Exit(1)
	}

	// 创建 Milvus 向量数据库存储实例。
	store, err := rag.NewMilvusStore(startupCtx, rag.MilvusConfig{
		Address: cfg.MilvusAddress, APIKey: cfg.MilvusAPIKey,
		CollectionName: cfg.MilvusCollection, Dimension: cfg.MilvusDimension,
	})
	cancelStartup()
	if err != nil {
		logger.Error("initialize Milvus", "error", err)
		os.Exit(1)
	}
	defer store.Close(context.Background())

	// 创建知识服务实例，使用 OpenAI 向量嵌入器和 Milvus 存储。
	knowledge, err := rag.NewServiceWithStore(rag.NewOpenAIEmbedder(cfg.APIKey, cfg.EmbeddingModel), store, splitter)
	if err != nil {
		logger.Error("initialize knowledge service", "error", err)
		os.Exit(1)
	}

	// 创建 Redis 状态存储实例，用于跟踪文档处理状态。
	statuses, err := ingest.NewRedisStatusStore(cfg.RedisAddress, cfg.RedisPassword, cfg.RedisDatabase, cfg.JobTTL)
	if err != nil {
		logger.Error("initialize Redis", "error", err)
		os.Exit(1)
	}
	defer statuses.Close()

	// 创建文档处理器实例，负责从 Kafka 消费文档并将其添加到知识库。
	processor, err := ingest.NewProcessor(statuses, knowledge.AddDocument)
	if err != nil {
		logger.Error("initialize processor", "error", err)
		os.Exit(1)
	}

	// 创建 Kafka 消费者实例，用于从指定主题消费文档任务。
	consumer, err := ingest.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup)
	if err != nil {
		logger.Error("initialize Kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// 启动信号监听上下文，用于优雅地处理进程终止信号。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 启动 Kafka 消费者，开始处理文档任务。
	logger.Info("ingestion worker started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	// 运行 Kafka 消费者，处理文档任务。
	if err := consumer.Run(ctx, func(ctx context.Context, job ingest.DocumentJob) error {
		// 处理文档任务前的日志记录。
		logger.Info("processing ingestion job", "job_id", job.ID, "document_id", job.DocumentID)
		// 正式处理文档任务，将文档分片并入库。
		err := processor.Handle(ctx, job)
		if err != nil {
			logger.Error("ingestion job failed", "job_id", job.ID, "error", err)
			return err
		}
		// 处理文档任务成功后的日志记录。
		logger.Info("ingestion job completed", "job_id", job.ID)
		return nil
	}); err != nil && ctx.Err() == nil {
		// 处理 Kafka 消费者运行过程中出现的错误。
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
