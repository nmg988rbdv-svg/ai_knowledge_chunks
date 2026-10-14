# 平台架构说明

## 组件职责

### Go API

API 只负责协议、校验、用例编排和错误映射。生产二进制只装配 Milvus、Redis 和 Kafka；自动化测试使用仅存在于 `_test.go` 的替身，不会把测试存储或同步队列编译进生产程序。

- `internal/httpapi`：REST/SSE、请求体限制、超时、公开错误码、内置 UI
- `internal/llm`：OpenAI Responses API、Embedding、Function Calling、Agent 循环
- `internal/rag`：文档切块、向量化、向量存储、Top-K 检索
- `internal/ingest`：任务发布、状态机和 Worker 处理逻辑

### LangChainGo

平台使用 LangChainGo 的递归字符切分器，优先在段落和句子边界切分，同时保留 overlap。模型调用保持在独立适配层中，避免业务代码与大型框架强耦合。

### Milvus

collection `ai_knowledge_chunks` 默认包含：

| 字段 | 类型 | 作用 |
|---|---|---|
| `chunk_id` | VarChar / PK | `{document_id}:{chunk_index}` |
| `document_id` | VarChar | 文档标识与替换过滤 |
| `chunk_index` | Int64 | 原文顺序 |
| `text` | VarChar | 返回给 RAG 的原始片段 |
| `embedding` | FloatVector | COSINE ANN 检索 |

服务首次连接时创建 collection、AUTOINDEX 并加载 collection。写入同一文档前先删除旧片段，再 upsert 新片段。

### Redis

Redis 保存入库任务状态和跨实例会话状态，key 格式为：

```text
ai-platform:ingest:{job_id}
ai-platform:conversation:{conversation_id}
ai-platform:conversation-lock:{conversation_id}
```

任务和会话都带独立 TTL，默认保存 24 小时。同一会话通过带所有权 token 的 Redis 锁串行更新，避免多个 API 实例覆盖 `previous_response_id`。公开任务失败信息固定为 `document indexing failed`，真实供应商错误只进入 Worker 日志。

### Kafka

API 使用 `document_id` 作为消息 key，使同一文档的任务稳定进入同一分区。Worker 禁用自动提交，只在任务完成或已经写入明确失败终态后提交 offset；基础设施/状态存储故障不会提交，交给进程重启后重试。

API 启动时会显式创建并等待 `knowledge.ingest.v1` 的 3 个分区 leader 就绪，不依赖 broker 的自动建 topic 配置。

本地 Compose 使用 KRaft 单节点，适合开发。生产环境应使用多 broker、鉴权/TLS、重试主题和 DLQ。

## 入库时序

```text
Client        API          Redis        Kafka        Worker      OpenAI       Milvus
  | POST job   |             |            |             |           |            |
  |----------->|             |            |             |           |            |
  |            | SET queued  |            |             |           |            |
  |            |------------>|            |             |           |            |
  |            | produce                  |             |           |            |
  |            |------------------------->|             |           |            |
  | 202 + id   |                          |             |           |            |
  |<-----------|                          | poll        |           |            |
  |                                       |----------->|           |            |
  |                                                  split          |            |
  |                                                    | embed     |            |
  |                                                    |---------->|            |
  |                                                    |<----------|            |
  |                                                    | upsert                 |
  |                                                    |----------------------->|
  |                              SET completed         |                        |
  |                              <---------------------|                        |
  |                                       commit offset|                        |
  |                                       <------------|                        |
```

## RAG 时序

1. API 将问题送到 Embedding API。
2. 使用查询向量在 Milvus 执行 COSINE Top-K。
3. 将命中片段编号为 `[1]`、`[2]` 等。
4. 将问题和片段作为不可信数据交给 grounded generation。
5. 模型被要求仅依据资料回答，资料不足时明确说明。
6. API 同时返回答案和结构化 `sources`，便于 UI 展示和评测。

## 稳定性设计

- OpenAI SDK 最多自动重试 2 次瞬时错误。
- HTTP 模型请求受 `LLM_TIMEOUT` 控制。
- Agent 有工具调用步数上限，防止无限循环。
- 普通请求限制为 64 KiB，文档任务限制为 900 KiB，避免无界内存占用并保持在 Kafka 默认消息限制内。
- Kafka 使用 `acks=all`；消费者显式提交 offset。
- `/healthz` 只检查进程；`/readyz` 检查 Milvus、Redis、Kafka。
- 结构化 JSON 日志记录模型、Token、耗时、任务 ID，不记录 API Key。
- HTTP 响应只返回稳定错误码，供应商细节留在服务端日志。

## 下一阶段演进

1. 文档层：PDF/Word/HTML 解析、对象存储、内容去重和版本管理。
2. 检索层：BM25 + 向量混合检索、metadata filter、reranker、相关性阈值。
3. Agent 层：工具权限、人工确认、幂等键、补偿事务和审计。
4. 可观测性：Prometheus、OpenTelemetry trace、Prompt/检索/工具调用轨迹。
5. 评测层：检索 Recall@K、MRR，回答忠实度/相关性，Agent 成功率与成本回归。
