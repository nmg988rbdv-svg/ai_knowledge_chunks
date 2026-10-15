package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llm-api-demo/internal/config"
	"llm-api-demo/internal/httpapi"
	"llm-api-demo/internal/ingest"
	"llm-api-demo/internal/llm"
	"llm-api-demo/internal/rag"
	"llm-api-demo/internal/usage"
)

// main 加载配置并启动 HTTP 服务。
func main() {
	// 使用 JSON 格式输出结构化日志，便于日志平台检索字段。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 配置错误属于启动错误，不应让服务以不完整配置继续运行。
	cfg, err := config.Load()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	// 创建模型用量事件发布者。业务请求只负责投递 Kafka，数据库写入由 usage-worker 完成。
	usagePublisher, err := usage.NewKafkaPublisher(cfg.KafkaBrokers, cfg.UsageKafkaTopic)
	if err != nil {
		logger.Error("initialize usage publisher", "error", err)
		os.Exit(1)
	}
	defer usagePublisher.Close()

	// 创建 LLM 客户端，并记录每次真实的 Responses API 调用。
	llmClient := llm.NewOpenAIClient(cfg.APIKey, cfg.Model, llm.WithUsagePublisher(usagePublisher, logger))

	// 创建向量嵌入器，用于将文本转换为向量表示。
	embedder := rag.NewOpenAIEmbedder(cfg.APIKey, cfg.EmbeddingModel, rag.WithUsagePublisher(usagePublisher, logger))

	// 构建平台依赖，包括知识库服务和任务队列。
	knowledge, jobs, conversations, closeResources, err := buildPlatform(cfg, embedder)
	if err != nil {
		logger.Error("initialize platform dependencies", "error", err)
		os.Exit(1)
	}
	defer closeResources()

	// 创建 HTTP Handler，将 LLM 客户端、知识库服务和任务队列注入其中。
	handler := httpapi.NewPlatformHandler(llmClient, conversations, knowledge, jobs, cfg.LLMTimeout, logger)

	// 创建 HTTP 服务器，配置地址、处理器及各类超时。
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.LLMTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 记录服务器启动信息，包括监听地址、使用的模型和嵌入模型。
	logger.Info("server started", "addr", cfg.HTTPAddr, "model", cfg.Model, "embedding_model", cfg.EmbeddingModel)

	// 启动一个 goroutine 监听服务器错误，避免阻塞主线程。
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()

	// 创建一个上下文，用于监听系统中断信号，以便优雅地关闭服务器。
	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 阻塞等待服务器错误或系统中断信号。
	select {
	case err := <-serverErrors:
		if !errorsIsServerClosed(err) {
			logger.Error("server stopped", "error", err)
		}
		return
	case <-shutdownSignal.Done():
		logger.Info("server shutdown requested")
	}

	// 收到系统中断信号后，开始优雅关闭服务器。
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		return
	}
	logger.Info("server stopped gracefully")
}

// buildPlatform 构建平台依赖，包括知识库服务、任务队列及资源关闭函数。
func buildPlatform(cfg config.Config, embedder rag.Embedder) (*rag.Service, *ingest.Queue, httpapi.ConversationStore, func(), error) {

	// 设置超时，确保在构建平台依赖时不会无限等待。
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 创建文本切分器，用于将文档切分为适合向量化的块。
	splitter, err := rag.NewLangChainSplitter(cfg.ChunkSize, cfg.ChunkOverlap)
	if err != nil {
		return nil, nil, nil, func() {}, err
	}

	// 创建 Milvus 存储，用于持久化向量化的文档块。
	store, err := rag.NewMilvusStore(ctx, rag.MilvusConfig{
		Address: cfg.MilvusAddress, APIKey: cfg.MilvusAPIKey,
		CollectionName: cfg.MilvusCollection, Dimension: cfg.MilvusDimension,
	})
	if err != nil {
		return nil, nil, nil, func() {}, err
	}

	// 创建知识库服务，使用 Milvus 存储和文本切分器。
	knowledge, err := rag.NewServiceWithStore(embedder, store, splitter)
	if err != nil {
		_ = store.Close(context.Background())
		return nil, nil, nil, func() {}, err
	}

	// 创建redis任务状态存储，用于跟踪文档处理任务的状态。
	statuses, err := ingest.NewRedisStatusStore(cfg.RedisAddress, cfg.RedisPassword, cfg.RedisDatabase, cfg.JobTTL)
	if err != nil {
		_ = knowledge.Close(context.Background())
		return nil, nil, nil, func() {}, err
	}

	// 创建 Kafka 发布者，用于将任务消息发布到 Kafka。
	publisher, err := ingest.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		_ = statuses.Close()
		_ = knowledge.Close(context.Background())
		return nil, nil, nil, func() {}, err
	}

	// 创建任务队列，使用 Kafka 发布者和 Redis 状态存储。
	queue, err := ingest.NewQueue(publisher, statuses)
	if err != nil {
		_ = publisher.Close()
		_ = statuses.Close()
		_ = knowledge.Close(context.Background())
		return nil, nil, nil, func() {}, err
	}

	// 创建会话存储，用于管理用户的对话状态。
	conversations, err := httpapi.NewRedisConversationStore(ctx, cfg.RedisAddress, cfg.RedisPassword, cfg.RedisDatabase, cfg.ConversationTTL)
	if err != nil {
		_ = queue.Close()
		_ = knowledge.Close(context.Background())
		return nil, nil, nil, func() {}, err
	}

	// 定义资源关闭函数，确保在平台不再使用时能够正确释放资源。
	closeResources := func() {
		_ = conversations.Close()
		_ = queue.Close()
		_ = knowledge.Close(context.Background())
	}
	return knowledge, queue, conversations, closeResources, nil
}

// errorsIsServerClosed 用于区分服务主动关闭与意外的监听错误。
func errorsIsServerClosed(err error) bool {
	return err == http.ErrServerClosed
}
