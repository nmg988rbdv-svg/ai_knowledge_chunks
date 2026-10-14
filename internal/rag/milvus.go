package rag

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
)

const milvusVectorField = "embedding"

type MilvusConfig struct {
	Address        string
	APIKey         string
	CollectionName string
	Dimension      int
}

// MilvusStore 为知识片段提供持久化、标量过滤和 ANN 检索。
type MilvusStore struct {
	client     *milvusclient.Client
	collection string
	dimension  int
}

// NewMilvusStore 创建一个 MilvusStore 实例，并确保集合存在。
func NewMilvusStore(ctx context.Context, cfg MilvusConfig) (*MilvusStore, error) {

	// 校验 Milvus 配置的合法性。
	if cfg.Address == "" || cfg.CollectionName == "" || cfg.Dimension <= 0 {
		return nil, errors.New("Milvus address, collection, and positive dimension are required")
	}

	// 创建 Milvus 客户端并连接到指定地址。
	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: cfg.Address,
		APIKey:  cfg.APIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("connect Milvus: %w", err)
	}

	// 初始化 MilvusStore 实例。
	store := &MilvusStore{client: client, collection: cfg.CollectionName, dimension: cfg.Dimension}
	if err := store.ensureCollection(ctx); err != nil {
		_ = client.Close(context.Background())
		return nil, err
	}
	return store, nil
}

func (s *MilvusStore) ensureCollection(ctx context.Context) error {
	exists, err := s.client.HasCollection(ctx, milvusclient.NewHasCollectionOption(s.collection))
	if err != nil {
		return fmt.Errorf("check Milvus collection: %w", err)
	}
	if !exists {
		schema := entity.NewSchema().
			WithDescription("AI knowledge base document chunks").
			WithField(entity.NewField().WithName("chunk_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(768).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("document_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512)).
			WithField(entity.NewField().WithName("chunk_index").WithDataType(entity.FieldTypeInt64)).
			WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
			WithField(entity.NewField().WithName(milvusVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(int64(s.dimension)))
		if err := s.client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(s.collection, schema)); err != nil {
			return fmt.Errorf("create Milvus collection: %w", err)
		}
	} else if err := s.validateCollection(ctx); err != nil {
		return err
	}

	indexes, err := s.client.ListIndexes(ctx, milvusclient.NewListIndexOption(s.collection).WithFieldName(milvusVectorField))
	if err != nil && !errors.Is(err, milvusclient.ErrIndexNotFound) {
		return fmt.Errorf("list Milvus indexes: %w", err)
	}
	if len(indexes) == 0 {
		task, err := s.client.CreateIndex(ctx, milvusclient.NewCreateIndexOption(
			s.collection,
			milvusVectorField,
			index.NewAutoIndex(entity.COSINE),
		).WithIndexName("embedding_idx"))
		if err != nil {
			return fmt.Errorf("create Milvus index: %w", err)
		}
		if err := task.Await(ctx); err != nil {
			return fmt.Errorf("await Milvus index: %w", err)
		}
	}

	loadTask, err := s.client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(s.collection))
	if err != nil {
		return fmt.Errorf("load Milvus collection: %w", err)
	}
	if err := loadTask.Await(ctx); err != nil {
		return fmt.Errorf("await Milvus collection load: %w", err)
	}
	return nil
}

func (s *MilvusStore) validateCollection(ctx context.Context) error {
	collection, err := s.client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(s.collection))
	if err != nil {
		return fmt.Errorf("describe Milvus collection: %w", err)
	}
	for _, field := range collection.Schema.Fields {
		if field.Name != milvusVectorField {
			continue
		}
		if field.DataType != entity.FieldTypeFloatVector {
			return fmt.Errorf("Milvus field %q has type %s, want FloatVector", milvusVectorField, field.DataType.Name())
		}
		dimension, err := field.GetDim()
		if err != nil {
			return fmt.Errorf("read Milvus vector dimension: %w", err)
		}
		if dimension != int64(s.dimension) {
			return fmt.Errorf("Milvus collection %q dimension = %d, want %d; choose a new collection or reindex documents", s.collection, dimension, s.dimension)
		}
		return nil
	}
	return fmt.Errorf("Milvus collection %q has no %q vector field", s.collection, milvusVectorField)
}

// ReplaceDocument 替换指定文档的所有切片，先删除旧的切片，再批量插入新的切片。
func (s *MilvusStore) ReplaceDocument(ctx context.Context, documentID string, chunks []Chunk) error {
	// 验证文档切片列表是否为空，避免插入空数据。
	if len(chunks) == 0 {
		return errors.New("document chunks must not be empty")
	}
	// 删除旧的文档切片，确保后续插入的新切片不会与旧切片冲突。
	if _, err := s.client.Delete(ctx, milvusclient.NewDeleteOption(s.collection).
		WithExpr("document_id == {document_id}").                   // 使用模板参数而不是拼接表达式，避免文档 ID 中的引号改变过滤语义。
		WithTemplateParam("document_id", documentID)); err != nil { // 使用模板参数删除指定文档 ID 的旧切片。
		return fmt.Errorf("delete old document chunks: %w", err)
	}

	// 准备批量插入的新文档切片数据，包括切片 ID、文档 ID、切片索引、文本和向量。
	chunkIDs := make([]string, len(chunks))
	documentIDs := make([]string, len(chunks))
	chunkIndexes := make([]int64, len(chunks))
	texts := make([]string, len(chunks))
	vectors := make([][]float32, len(chunks))
	// 遍历每个文档切片，填充批量插入的数据结构。
	for i, chunk := range chunks {

		// 验证当前文档切片的向量维度是否与 Milvus 集合的向量维度一致。
		if len(chunk.Vector) != s.dimension {
			return fmt.Errorf("chunk %d vector dimension = %d, want %d", i, len(chunk.Vector), s.dimension)
		}
		// 构建当前文档切片的批量插入数据。
		chunkIDs[i] = fmt.Sprintf("%s:%06d", documentID, chunk.Index)
		documentIDs[i] = documentID
		chunkIndexes[i] = int64(chunk.Index)
		texts[i] = chunk.Text
		vectors[i] = float32Vector(chunk.Vector)
	}

	// 执行批量插入操作，将新的文档切片数据写入 Milvus。
	_, err := s.client.Upsert(ctx, milvusclient.NewColumnBasedInsertOption(s.collection).
		WithVarcharColumn("chunk_id", chunkIDs).
		WithVarcharColumn("document_id", documentIDs).
		WithInt64Column("chunk_index", chunkIndexes).
		WithVarcharColumn("text", texts).
		WithFloatVectorColumn(milvusVectorField, s.dimension, vectors))
	if err != nil {
		return fmt.Errorf("upsert Milvus chunks: %w", err)
	}
	return nil
}

// Search 使用 Milvus 向量数据库执行相似度搜索，返回与查询向量最相似的文档片段。
func (s *MilvusStore) Search(ctx context.Context, queryVector []float64, topK int) ([]SearchResult, error) {

	// 验证查询向量的维度是否与 Milvus 集合的向量维度一致。
	if len(queryVector) != s.dimension {
		return nil, fmt.Errorf("query vector dimension = %d, want %d", len(queryVector), s.dimension)
	}
	// 执行 Milvus 向量搜索，获取与查询向量最相似的文档片段。
	sets, err := s.client.Search(ctx, milvusclient.NewSearchOption(
		s.collection, // Milvus 集合名称
		topK,         // 返回的最相似文档片段数量
		[]entity.Vector{entity.FloatVector(float32Vector(queryVector))}, // 查询向量
	).
		WithANNSField(milvusVectorField).                       // 指定用于近似最近邻搜索的向量字段
		WithOutputFields("document_id", "chunk_index", "text"). // 指定需要返回的字段
		WithConsistencyLevel(entity.ClStrong))                  // 设置一致性级别为强一致性
	if err != nil {
		return nil, fmt.Errorf("search Milvus: %w", err)
	}

	// 定义用于解析 Milvus 搜索结果的结构体。
	type resultRow struct {
		ChunkID    string `milvus:"name:chunk_id"`
		DocumentID string `milvus:"name:document_id"`
		ChunkIndex int64  `milvus:"name:chunk_index"`
		Text       string `milvus:"name:text"`
	}

	// 初始化用于存储最终搜索结果的切片。
	results := make([]SearchResult, 0, topK)

	// 遍历 Milvus 搜索结果集，解析每个结果并构建 SearchResult 列表。
	for _, set := range sets {
		var rows []*resultRow
		if err := set.Unmarshal(&rows); err != nil {
			return nil, fmt.Errorf("decode Milvus results: %w", err)
		}
		// 遍历当前结果集中的每一行，构建最终的搜索结果。
		for i, row := range rows {
			// 检查当前结果的分数索引是否超出范围。
			if i >= len(set.Scores) {
				return nil, errors.New("Milvus result score count mismatch")
			}
			results = append(results, SearchResult{
				DocumentID: row.DocumentID,
				ChunkIndex: int(row.ChunkIndex),
				Text:       row.Text,
				Score:      float64(set.Scores[i]),
			})
		}
	}
	return results, nil
}

func (s *MilvusStore) Ping(ctx context.Context) error {
	_, err := s.client.HasCollection(ctx, milvusclient.NewHasCollectionOption(s.collection))
	return err
}

func (s *MilvusStore) Close(ctx context.Context) error { return s.client.Close(ctx) }

func float32Vector(vector []float64) []float32 {
	result := make([]float32, len(vector))
	for i, value := range vector {
		result[i] = float32(value)
	}
	return result
}
