package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llm-api-demo/internal/config"
	"llm-api-demo/internal/usage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadUsageWorker()
	if err != nil {
		logger.Error("load usage worker configuration", "error", err)
		os.Exit(1)
	}
	// 初始化 PostgreSQL 存储
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	store, err := usage.NewStore(startupCtx, cfg.PostgresDSN)
	cancelStartup()
	if err != nil {
		logger.Error("initialize usage PostgreSQL store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// 初始化 Kafka 消费者
	consumer, err := usage.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, logger)
	if err != nil {
		logger.Error("initialize usage Kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// 处理系统信号以优雅地关闭使用事件工作者
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("usage worker started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	// 启动 Kafka 消费循环，处理使用事件
	err = consumer.Run(ctx, func(ctx context.Context, event usage.Event) error {
		// 将使用事件写入 PostgreSQL 存储
		inserted, err := store.Insert(ctx, event)
		if err != nil {
			return err
		}
		logger.Info(
			"usage event persisted",
			"event_id", event.ID,
			"trace_id", event.TraceID,
			"operation", event.Operation,
			"model", event.Model,
			"total_tokens", event.TotalTokens,
			"inserted", inserted,
		)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		logger.Error("usage worker stopped", "error", err)
		os.Exit(1)
	}
}
