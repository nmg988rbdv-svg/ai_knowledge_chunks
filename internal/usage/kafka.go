package usage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Publisher interface {
	Publish(context.Context, Event) error
	Ping(context.Context) error
	Close() error
}

type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaPublisher 创建一个 KafkaPublisher 实例，用于向指定的 Kafka 主题发布使用事件。
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("usage Kafka brokers and topic are required")
	}
	// 创建 Kafka 客户端实例
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordDeliveryTimeout(10*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()), // 等待所有 ISR 确认
	)
	if err != nil {
		return nil, err
	}
	// 确保 Kafka 主题存在，如果不存在则创建
	startupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := ensureTopic(startupCtx, client, topic); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure usage Kafka topic: %w", err)
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	delivered := make(chan error, 1)
	p.client.Produce(ctx, &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.TraceID),
		Value: data,
	}, func(_ *kgo.Record, err error) {
		delivered <- err
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-delivered:
		return err
	}
}

func (p *KafkaPublisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }
func (p *KafkaPublisher) Close() error {
	p.client.Close()
	return nil
}

type KafkaConsumer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewKafkaConsumer 创建一个 KafkaConsumer 实例，用于从指定的 Kafka 主题消费使用事件。
func NewKafkaConsumer(brokers []string, topic, group string, logger *slog.Logger) (*KafkaConsumer, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("usage Kafka brokers, topic, and group are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{client: client, logger: logger}, nil
}

// Run 启动 Kafka 消费循环，只在数据库写入成功后提交 offset；同一事件重投由数据库主键保证幂等。
func (c *KafkaConsumer) Run(ctx context.Context, handle func(context.Context, Event) error) error {
	for {
		// 拉取 Kafka 消息
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 检查是否有拉取错误
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("poll usage Kafka: %v", errs)
		}
		// 遍历拉取到的 Kafka 消息并处理
		iter := fetches.RecordIter()
		for !iter.Done() {
			// 获取下一条 Kafka 消息
			record := iter.Next()
			var event Event
			// 反序列化 Kafka 消息为使用事件结构体
			if err := json.Unmarshal(record.Value, &event); err != nil {
				c.logger.Error("discard invalid usage event JSON", "partition", record.Partition, "offset", record.Offset, "error", err)
				// 丢弃无效的使用事件 JSON
				if err := c.client.CommitRecords(ctx, record); err != nil {
					return fmt.Errorf("commit invalid usage event: %w", err)
				}
				continue
			}
			if err := event.Validate(); err != nil {
				c.logger.Error("discard invalid usage event", "event_id", event.ID, "error", err)
				if err := c.client.CommitRecords(ctx, record); err != nil {
					return fmt.Errorf("commit invalid usage event %s: %w", event.ID, err)
				}
				continue
			}
			if err := handle(ctx, event); err != nil {
				return fmt.Errorf("persist usage event %s: %w", event.ID, err)
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit usage event %s: %w", event.ID, err)
			}
		}
	}
}

func (c *KafkaConsumer) Close() { c.client.Close() }

func ensureTopic(ctx context.Context, client *kgo.Client, topic string) error {
	request := kmsg.NewPtrCreateTopicsRequest()
	requestTopic := kmsg.NewCreateTopicsRequestTopic()
	requestTopic.Topic = topic
	requestTopic.NumPartitions = 3
	requestTopic.ReplicationFactor = 1
	request.Topics = append(request.Topics, requestTopic)
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	if len(response.Topics) != 1 {
		return errors.New("Kafka returned no usage topic creation result")
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadataRequest := kmsg.NewPtrMetadataRequest()
		metadataTopic := kmsg.NewMetadataRequestTopic()
		metadataTopic.Topic = &topic
		metadataRequest.Topics = append(metadataRequest.Topics, metadataTopic)
		metadataResponse, err := metadataRequest.RequestWith(ctx, client)
		if err == nil && len(metadataResponse.Topics) == 1 {
			result := metadataResponse.Topics[0]
			if result.ErrorCode == 0 && len(result.Partitions) > 0 {
				return nil
			}
			metadataErr := kerr.ErrorForCode(result.ErrorCode)
			if metadataErr != nil && !errors.Is(metadataErr, kerr.LeaderNotAvailable) && !errors.Is(metadataErr, kerr.UnknownTopicOrPartition) {
				return metadataErr
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
