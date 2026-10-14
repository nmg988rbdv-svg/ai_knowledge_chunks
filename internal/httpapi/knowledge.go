package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"llm-api-demo/internal/llm"
	"llm-api-demo/internal/rag"
)

const maxSearchTopK = 10

type addDocumentRequest struct {
	DocumentID string `json:"document_id"`
	Text       string `json:"text"`
}

type searchKnowledgeRequest struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k"`
}

type searchKnowledgeResponse struct {
	Query   string             `json:"query"`
	Matches []rag.SearchResult `json:"matches"`
}

type askKnowledgeResponse struct {
	Query   string            `json:"query"`
	Answer  llm.Result        `json:"answer"`
	Sources []knowledgeSource `json:"sources"`
}

// knowledgeSource 将模型答案里的 [n] 引用映射回具体文档片段。
type knowledgeSource struct {
	Reference  int     `json:"reference"`
	DocumentID string  `json:"document_id"`
	ChunkIndex int     `json:"chunk_index"`
	Text       string  `json:"text"`
	Score      float64 `json:"score"`
}

// searchKnowledge 处理知识库搜索请求，根据用户的查询返回匹配的文档片段。
func (h *Handler) searchKnowledge(w http.ResponseWriter, r *http.Request) {
	var req searchKnowledgeRequest
	// 解析请求体为 JSON，并校验 query 字段是否存在且非空。
	if err := decodeJSONRequest(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "request body must contain query and optional top_k"))
		return
	}
	// 去除 query 字段的首尾空白字符，并确保其非空。
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "query must not be empty"))
		return
	}
	// 如果 top_k 未设置，则使用默认值 3；同时确保 top_k 在合理范围内。
	if req.TopK == 0 {
		req.TopK = 3
	}

	// 校验 top_k 是否在允许的范围内。
	if req.TopK < 1 || req.TopK > maxSearchTopK {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "top_k must be between 1 and 10"))
		return
	}

	// 创建带超时的上下文，用于后续的知识库搜索请求。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	// 调用知识库的搜索方法，传入用户的查询和 top_k 参数。
	matches, err := h.knowledge.Search(ctx, req.Query, req.TopK)
	if err != nil {
		status, code, message := classifyKnowledgeError(err)
		h.logger.Error("search knowledge", "code", code, "error", err)
		writeJSON(w, status, newError(code, message))
		return
	}
	writeJSON(w, http.StatusOK, searchKnowledgeResponse{Query: req.Query, Matches: matches})
}

// askKnowledge 先检索相关片段，再把编号后的片段交给模型生成有来源约束的答案。
func (h *Handler) askKnowledge(w http.ResponseWriter, r *http.Request) {
	var req searchKnowledgeRequest
	// 解析请求体为 JSON，并校验 query 字段是否存在且非空。
	if err := decodeJSONRequest(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", "request body must contain query and optional top_k"))
		return
	}
	// 规范化查询，去除多余空白并设置默认 top_k。
	if err := normalizeKnowledgeQuery(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, newError("invalid_request", err.Error()))
		return
	}
	// 创建带超时的上下文，用于后续的知识库搜索请求。
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// 调用知识库的搜索方法，获取与查询最相关的文档片段。
	matches, err := h.knowledge.Search(ctx, req.Query, req.TopK)
	if err != nil {
		status, code, message := classifyKnowledgeError(err)
		h.logger.Error("retrieve knowledge for answer", "code", code, "error", err)
		writeJSON(w, status, newError(code, message))
		return
	}
	// 如果没有匹配的文档片段，返回 404 错误。
	if len(matches) == 0 {
		writeJSON(w, http.StatusNotFound, newError("knowledge_not_found", "the knowledge base contains no searchable content"))
		return
	}

	// 构建知识来源列表，用于在生成答案时提供引用信息。
	sources, groundingSources := buildKnowledgeSources(matches)

	// 调用 LLM 生成基于知识来源的答案，确保回答引用了检索到的文档片段。
	answer, err := h.llm.GenerateGrounded(ctx, req.Query, groundingSources)
	if err != nil {
		status, code, publicMessage := classifyError(err)
		h.logModelError("knowledge answer generation failed", code, err)
		writeJSON(w, status, newError(code, publicMessage))
		return
	}
	// 记录生成答案的日志信息，包括响应 ID、模型、引用的知识来源数量、耗时和 token 使用情况。
	h.logger.Info(
		"knowledge answer completed",
		"response_id", answer.ID,
		"model", answer.Model,
		"sources", len(sources),
		"duration_ms", answer.DurationMS,
		"input_tokens", answer.Usage.InputTokens,
		"output_tokens", answer.Usage.OutputTokens,
	)
	writeJSON(w, http.StatusOK, askKnowledgeResponse{Query: req.Query, Answer: answer, Sources: sources})
}

// buildKnowledgeSources 构建知识来源列表，同时生成用于 LLM 的 groundingSources。
func buildKnowledgeSources(matches []rag.SearchResult) ([]knowledgeSource, []llm.GroundingSource) {
	// 初始化知识来源列表和 groundingSources 列表，长度与匹配结果相同。
	sources := make([]knowledgeSource, len(matches))
	groundingSources := make([]llm.GroundingSource, len(matches))
	// 遍历匹配结果，构建知识来源和 groundingSources。
	for index, match := range matches {
		reference := index + 1
		sources[index] = knowledgeSource{
			Reference: reference, DocumentID: match.DocumentID, ChunkIndex: match.ChunkIndex,
			Text: match.Text, Score: match.Score,
		}
		groundingSources[index] = llm.GroundingSource{
			Reference: reference, DocumentID: match.DocumentID, ChunkIndex: match.ChunkIndex, Text: match.Text,
		}
	}
	return sources, groundingSources
}

// normalizeKnowledgeQuery 规范化知识库查询请求，去除多余空白并设置默认 top_k。
func normalizeKnowledgeQuery(req *searchKnowledgeRequest) error {
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return errors.New("query must not be empty")
	}
	if req.TopK == 0 {
		req.TopK = 3
	}
	if req.TopK < 1 || req.TopK > maxSearchTopK {
		return errors.New("top_k must be between 1 and 10")
	}
	return nil
}

// classifyKnowledgeError 根据不同的错误类型返回对应的 HTTP 状态码、错误代码和错误信息。
func classifyKnowledgeError(err error) (int, string, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "embedding_timeout", "the embedding request timed out"
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, "request_canceled", "the request was canceled"
	}
	return http.StatusBadGateway, "embedding_failed", "failed to generate or search embeddings"
}
