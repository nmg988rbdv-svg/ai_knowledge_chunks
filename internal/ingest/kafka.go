package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaPublisher 是基于 Kafka 的任务发布者实现。
type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaPublisher 创建一个 KafkaPublisher 实例，并确保指定的主题存在。
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {

	// 校验 Kafka brokers 和 topic 是否有效。
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("Kafka brokers and topic are required")
	}
	// 创建 Kafka 客户端，配置默认主题、发送超时和所需的确认级别。
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),               // 设置 Kafka broker 列表
		kgo.DefaultProduceTopic(topic),            // 设置默认的生产主题
		kgo.RecordDeliveryTimeout(10*time.Second), // 设置消息发送超时
		kgo.RequiredAcks(kgo.AllISRAcks()),        // 设置所需的确认级别
	)
	if err != nil {
		return nil, err
	}

	// 先设置超时时间
	startupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 调用 ensureKafkaTopic 确保主题存在。
	if err := ensureKafkaTopic(startupCtx, client, topic); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure Kafka topic: %w", err)
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
}

// ensureKafkaTopic 确保指定的 Kafka 主题存在，如果不存在则创建。
func ensureKafkaTopic(ctx context.Context, client *kgo.Client, topic string) error {
	request := kmsg.NewPtrCreateTopicsRequest()           // 创建创建主题请求对象
	requestTopic := kmsg.NewCreateTopicsRequestTopic()    // 创建主题请求对象
	requestTopic.Topic = topic                            // 设置主题名称
	requestTopic.NumPartitions = 3                        // 设置分区数
	requestTopic.ReplicationFactor = 1                    // 设置副本因子
	request.Topics = append(request.Topics, requestTopic) // 将主题请求添加到请求列表
	response, err := request.RequestWith(ctx, client)     // 发送创建主题请求
	if err != nil {
		return err
	}

	// 检查创建主题的响应，确保主题创建成功。
	if len(response.Topics) != 1 {
		return errors.New("Kafka returned no topic creation result")
	}

	// 检查主题创建是否返回错误，忽略主题已存在的错误。
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}

	// CreateTopics 只表示 controller 已接受请求；等待分区 leader 可用后再接收 HTTP 流量。
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 等待分区 leader 可用，确保主题可以正常接收消息。
	for {
		metadataRequest := kmsg.NewPtrMetadataRequest()                        // 创建元数据请求对象
		metadataTopic := kmsg.NewMetadataRequestTopic()                        // 创建元数据请求的主题对象
		metadataTopic.Topic = &topic                                           // 设置主题名称
		metadataRequest.Topics = append(metadataRequest.Topics, metadataTopic) // 将主题添加到元数据请求中
		// 发送元数据请求，检查主题的分区 leader 是否可用。
		metadataResponse, err := metadataRequest.RequestWith(ctx, client)

		// 检查元数据响应，确保主题的分区 leader 可用。
		if err == nil && len(metadataResponse.Topics) == 1 {

			// 获取主题的元数据结果。
			topicResult := metadataResponse.Topics[0]

			// 检查主题的元数据结果，确保分区 leader 可用。
			if topicResult.ErrorCode == 0 && len(topicResult.Partitions) > 0 {
				return nil
			}

			// 如果分区 leader 不可用，继续等待。
			metadataErr := kerr.ErrorForCode(topicResult.ErrorCode)

			// 如果元数据响应中存在错误且不是 LeaderNotAvailable 或 UnknownTopicOrPartition，则返回错误。
			if metadataErr != nil && !errors.Is(metadataErr, kerr.LeaderNotAvailable) && !errors.Is(metadataErr, kerr.UnknownTopicOrPartition) {
				return metadataErr
			}
		}

		// 等待一段时间后再次检查分区 leader 是否可用。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Publish 将文档任务发布到 Kafka 消息队列，以便 worker 可以异步处理该任务。
func (p *KafkaPublisher) Publish(ctx context.Context, job DocumentJob) error {
	// 将文档任务编码为 JSON 格式，以便发送到 Kafka 消息队列。
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	// 将文档任务发送到 Kafka 消息队列，并等待发送结果。
	delivered := make(chan error, 1)
	p.client.Produce(ctx, &kgo.Record{Topic: p.topic, Key: []byte(job.DocumentID), Value: data}, func(_ *kgo.Record, err error) {
		delivered <- err
	})
	// 等待文档任务发送结果，确保消息已成功发送到 Kafka。
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
}

// NewKafkaConsumer 创建一个 Kafka 消费者实例，用于从指定主题消费文档任务。
func NewKafkaConsumer(brokers []string, topic, group string) (*KafkaConsumer, error) {

	// 验证传入的 Kafka 配置参数是否有效。
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("Kafka brokers, topic, and consumer group are required")
	}
	// 创建 Kafka 客户端实例，用于连接 Kafka 集群并消费消息。
	client, err := kgo.NewClient( // 创建 Kafka 客户端实例
		kgo.SeedBrokers(brokers...), // 设置 Kafka 集群的种子节点
		kgo.ConsumeTopics(topic),    // 设置要消费的 Kafka 主题
		kgo.ConsumerGroup(group),    // 设置 Kafka 消费者组
		kgo.DisableAutoCommit(),     // 禁用自动提交偏移量
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{client: client}, nil
}

// Run 启动 Kafka 消费者，循环消费文档任务并调用处理函数进行处理。
func (c *KafkaConsumer) Run(ctx context.Context, handle func(context.Context, DocumentJob) error) error {

	// 循环消费 Kafka 消息，直到上下文被取消。
	for {
		// 拉取 Kafka 消息。
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 检查是否有拉取 Kafka 消息时的错误。
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("poll Kafka: %v", errs)
		}
		// 获取 Kafka 消息迭代器，准备逐条处理消息。
		iter := fetches.RecordIter()
		// 逐条处理 Kafka 消息。
		for !iter.Done() {
			record := iter.Next()
			var job DocumentJob
			// 将 Kafka 消息解码为文档任务。
			if err := json.Unmarshal(record.Value, &job); err != nil {
				return fmt.Errorf("decode ingestion job: %w", err)
			}
			// 调用处理函数处理文档任务。
			if err := handle(ctx, job); err != nil {
				if !IsPermanentJobError(err) {
					return fmt.Errorf("process ingestion job %s: %w", job.ID, err)
				}
			}
			// 提交 Kafka 消息的偏移量，表示该消息已被成功处理。
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit ingestion job %s: %w", job.ID, err)
			}
		}
	}
}

// Close 关闭 Kafka 消费者，释放相关资源。
func (c *KafkaConsumer) Close() { c.client.Close() }
