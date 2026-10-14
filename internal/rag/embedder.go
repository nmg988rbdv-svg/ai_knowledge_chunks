package rag

import (
	"context"
	"errors"
	"fmt"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

// Embedder 将一批文本转换为相同维度的向量，便于替换供应商及 Mock 测试。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float64, error)
}

// OpenAIEmbedder 使用 OpenAI Embeddings API 生成向量。
type OpenAIEmbedder struct {
	client openai.Client
	model  string
}

// NewOpenAIEmbedder 创建一个 OpenAIEmbedder 实例，使用指定的 API Key 和模型名称。
func NewOpenAIEmbedder(apiKey, model string) *OpenAIEmbedder {
	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithMaxRetries(2), // 设置最大重试次数为 2
	)
	return &OpenAIEmbedder{client: client, model: model}
}

// Embed 将一批文本转换为向量。
func (e *OpenAIEmbedder) Embed(ctx context.Context, texts []string) ([][]float64, error) {
	// 验证输入文本列表，确保其非空且每个文本都不为空。
	if len(texts) == 0 {
		return nil, errors.New("embedding input must not be empty")
	}
	// 遍历每个文本，确保其非空。
	for _, text := range texts {
		if text == "" {
			return nil, errors.New("embedding text must not be empty")
		}
	}

	// 调用 OpenAI Embeddings API 生成向量。
	response, err := e.client.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: texts, // 输入文本列表
		},
		Model: openai.EmbeddingModel(e.model), // 使用指定的嵌入模型
	})
	if err != nil {
		return nil, err
	}
	// 验证返回的向量数量是否与输入文本数量一致。
	if len(response.Data) != len(texts) {
		return nil, fmt.Errorf("embedding count = %d, want %d", len(response.Data), len(texts))
	}

	// API 通过 Index 标识输入顺序，不依赖 Data 数组的偶然排列。
	vectors := make([][]float64, len(texts))
	// 根据 API 返回的 Index 将向量放入正确的位置，确保顺序与输入文本一致。
	for _, item := range response.Data {
		if item.Index < 0 || item.Index >= int64(len(vectors)) {
			return nil, fmt.Errorf("embedding index %d is out of range", item.Index)
		}
		vectors[item.Index] = item.Embedding
	}
	// 最终检查每个向量是否非空，确保嵌入结果有效。
	for index, vector := range vectors {
		if len(vector) == 0 {
			return nil, fmt.Errorf("embedding %d is empty", index)
		}
	}
	return vectors, nil
}
