# AI 智能助理与知识库平台

一个可运行的 Go AI 后端项目，覆盖 LLM API、RAG、Agent、Milvus、LangChainGo、Redis、Kafka 和 PostgreSQL 的完整业务链路，并提供内置 Web UI。

## 已实现能力

- 普通对话、SSE 流式输出和 JSON Schema 结构化输出
- Function Calling 计算工具与有最大步数保护的多步 Agent
- 多轮会话与 `previous_response_id` 上下文链
- LangChainGo 递归切块、OpenAI Embedding、Milvus 向量检索
- 带引用来源的 RAG 问答
- Kafka 异步文档入库、Redis 任务状态、独立 Worker
- Redis 持久化会话状态与跨实例会话锁
- Kafka 异步用量流水、PostgreSQL 持久化及 Agent 逐次模型调用追踪
- 存活/就绪检查、结构化日志、超时、重试和公开错误码
- 响应式 Web UI，以及不调用真实模型的单元测试

## 架构

```text
                         ┌──────────────────────┐
浏览器 / API Client ────▶│ Go API               │
                         │ Chat / RAG / Agent    │
                         └───┬────────┬─────────┘
                             │        │
                Responses /  │        │ 文档任务
                Embeddings   │        ▼
                             │    ┌─────────┐      ┌──────────────┐
                             │    │ Kafka   │─────▶│ Go Worker     │
                             │    └─────────┘      │ LangChainGo   │
                             │                     └──────┬───────┘
                             ▼                            │ Embedding
                      ┌────────────┐                      ▼
                      │ OpenAI API │              ┌──────────────┐
                      └────────────┘              │ Milvus       │
                                                  │ Vector Store │
                         ┌─────────┐              └──────────────┘
                         │ Redis   │ 任务状态
                         └─────────┘
```

两条核心数据流：

```text
异步入库：HTTP 202 → Redis(queued) → Kafka → Worker
         → LangChainGo 切块 → Embedding → Milvus → Redis(completed/failed)

RAG 问答：问题 → Embedding → Milvus Top-K → 带来源上下文的 Responses API
         → 答案 + sources

用量记录：Responses/Embedding → Kafka(llm.usage.v1) → Usage Worker
         → PostgreSQL(llm_usage_records)
```

更详细的设计说明见 [docs/architecture.md](docs/architecture.md)。

## 快速启动：完整平台

前置条件：Docker Desktop/Compose V2，以及可调用模型和 Embedding 的 OpenAI API Key。Milvus Standalone 建议给 Docker 分配至少 8 GB 内存。

首次执行初始化命令：

```bash
make init
```

如果 `.env` 不存在，该命令会从 `.env.example` 创建一份，并提示填写必要配置。它不会覆盖已有的 `.env`。

编辑 `.env`，至少设置：

```dotenv
OPENAI_API_KEY=你的_API_Key
OPENAI_MODEL=你的项目可用模型
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
POSTGRES_PASSWORD=设置一个强密码
POSTGRES_DSN=postgres://ai_platform:相同密码@127.0.0.1:15432/ai_platform?sslmode=disable
```

配置完成后再次执行：

```bash
make init
```

`make init` 会构建并启动所有服务，等待健康检查通过，然后显示服务状态。后续普通启动或应用代码更新可使用：

```bash
make up
docker compose logs -f api worker usage-worker
```

打开 [http://localhost:8080](http://localhost:8080) 即可使用内置 UI。页面提供统一问答输入，后端会自动选择普通问答、RAG 或 Agent，并通过 SSE 流式返回答案；异步文档入库区域仍可独立使用。Milvus WebUI 位于 [http://localhost:19091/webui/](http://localhost:19091/webui/)，MinIO Console 位于 [http://localhost:19001](http://localhost:19001)。

为避免和常见的本地 Milvus 环境冲突，本项目默认把 MinIO 映射到 `19000/19001`，把 Milvus 映射到 `29530/19091`，并且只监听宿主机回环地址。可以通过 `.env` 中的 `MINIO_API_PORT`、`MINIO_CONSOLE_PORT`、`MILVUS_HOST_PORT`、`MILVUS_WEBUI_PORT` 调整。

如果本机 Redis 或 Kafka 占用了 `6379`、`9092`，需要先停止旧服务或修改 Compose 的宿主机端口。

停止全部服务但保留容器和数据：

```bash
make stop
```

删除容器和网络、保留数据卷：

```bash
make down
```

删除本项目全部容器、网络和数据卷：

```bash
make clean
```

`make clean` 会清空 Milvus、Redis、Kafka 和 PostgreSQL 的持久化数据，只应在明确需要重建环境时使用。

## 本地运行 Go 进程

运行时使用 Milvus、Redis、Kafka 和 PostgreSQL。可以只用 Compose 启动基础设施并执行迁移，再在本机运行 Go 进程：

```bash
docker compose up -d etcd minio milvus redis kafka postgres
make migrate

set -a
source .env
set +a

# 终端 1
go run ./cmd/server

# 终端 2
go run ./cmd/worker

# 终端 3
go run ./cmd/usage-worker
```

## API 一览

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 内置 Web UI |
| GET | `/healthz` | 进程存活检查 |
| GET | `/readyz` | Milvus、Redis、Kafka、PostgreSQL 就绪检查 |
| GET | `/v1/usage?limit=30` | Token 汇总与最近调用明细（最多 100 条） |
| POST | `/v1/assistant/stream` | 自动路由 Chat/RAG/Agent 的统一 SSE 问答 |
| POST | `/v1/chat` | 普通对话 |
| POST | `/v1/chat/stream` | SSE 流式对话 |
| POST | `/v1/chat/tools` | Function Calling |
| POST | `/v1/agent/run` | 多步工具 Agent |
| POST | `/v1/analyze` | JSON Schema 结构化输出 |
| POST | `/v1/conversations` | 创建会话 |
| POST | `/v1/conversations/{id}/messages` | 多轮会话 |
| DELETE | `/v1/conversations/{id}` | 删除会话 |
| POST | `/v1/knowledge/jobs` | 异步提交文档，返回 202 |
| GET | `/v1/knowledge/jobs/{job_id}` | 查询入库状态 |
| POST | `/v1/knowledge/search` | Milvus 语义检索 |
| POST | `/v1/knowledge/ask` | 带来源的 RAG 问答 |
| POST | `/v1/workflow/route` | StateGraph 条件路由 |

### 异步导入文档

```bash
curl -sS http://localhost:8080/v1/knowledge/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "document_id":"refund-policy",
    "text":"退款申请需在购买后七天内提交，并提供订单号。"
  }'
```

响应：

```json
{
  "id": "job_...",
  "document_id": "refund-policy",
  "state": "queued",
  "updated_at": "2026-10-13T05:00:00Z"
}
```

使用返回的 `id` 查询：

```bash
curl -sS http://localhost:8080/v1/knowledge/jobs/job_xxx
```

状态按 `queued → processing → completed` 流转；失败时为 `failed`。相同 `document_id` 再次成功导入会替换旧切片。

### 语义检索与 RAG

```bash
curl -sS http://localhost:8080/v1/knowledge/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"什么时候可以退款？","top_k":3}'

curl -sS http://localhost:8080/v1/knowledge/ask \
  -H 'Content-Type: application/json' \
  -d '{"query":"退款需要什么条件？","top_k":3}'
```

RAG 回答中的 `[1]` 对应响应里的 `sources[0].reference`。检索不到资料时接口不会让模型凭空回答。

### Agent

```bash
curl -sS http://localhost:8080/v1/agent/run \
  -H 'Content-Type: application/json' \
  -d '{"goal":"先计算 12 乘以 7，再把结果加上 16"}'
```

响应保留每次工具名称、参数、结果与 step，便于调试和后续做 Agent 评测。当前仅注册无副作用的计算器；接入支付、发消息、删除等工具前必须增加授权确认、幂等和审计。

### Token 用量记录

每次 Responses API 和 Embedding API 调用都会产生独立事件。Agent 的多次模型调用使用相同 `trace_id`，并按 `call_index` 分行保存。Kafka offset 只在 PostgreSQL 写入成功后提交，表主键和上游 `response_id` 唯一索引用于处理消息重投。

查看最近 30 条明细：

```bash
make usage
make usage-summary
```

内置 Web UI 会显示累计 Token、输入/输出 Token、调用次数及最近 30 条调用记录。也可以直接查询：

```bash
curl -sS 'http://localhost:8080/v1/usage?limit=30'
```

表中保存输入、缓存、输出、推理及总 token、耗时、状态和错误码。`estimated_cost_usd` 与 `pricing_version` 暂时保留为空，避免把会变化的供应商价格硬编码进历史记录；后续应通过版本化价格表计算并保存价格快照。

## 关键配置

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `OPENAI_MODEL` | `gpt-5.2` | Responses API 模型 |
| `OPENAI_EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding 模型 |
| `MILVUS_DIMENSION` | `1536` | 必须与 Embedding 输出维度一致 |
| `RAG_CHUNK_SIZE` | `800` | LangChainGo 切片大小 |
| `RAG_CHUNK_OVERLAP` | `120` | 相邻切片重叠 |
| `JOB_TTL` | `24h` | Redis 任务状态保存时间 |
| `CONVERSATION_TTL` | `24h` | Redis 会话状态保存时间 |
| `LLM_TIMEOUT` | `30s` | 单次模型调用超时 |
| `USAGE_KAFKA_TOPIC` | `llm.usage.v1` | 模型用量事件主题 |
| `USAGE_KAFKA_GROUP` | `ai-usage-workers` | 用量消费者组 |
| `POSTGRES_DSN` | 见 `.env.example` | 用量数据库连接串 |

完整配置见 [.env.example](.env.example)。如果修改 Embedding 模型或维度，应使用新的 Milvus collection 或重建原 collection，不能混存不同维度的向量。

## 测试与检查

测试不调用真实模型，不消耗 API 额度：

```bash
go test ./...
go test -race ./...
go vet ./...
docker compose config --quiet
```

## 项目结构

```text
cmd/server/                  HTTP API 进程
cmd/worker/                  Kafka 文档入库 Worker
cmd/usage-worker/            Kafka 用量持久化 Worker
internal/config/             环境配置与启动校验
internal/httpapi/            HTTP、SSE、UI、错误映射
internal/llm/                OpenAI Responses、工具调用、Agent
internal/rag/                Embedding、LangChainGo、Milvus
internal/ingest/             Kafka、Redis、任务状态与处理器
internal/usage/              用量事件、Kafka、PostgreSQL 与 sqlc 查询
internal/workflow/           可测试的状态图和条件路由
migrations/                  golang-migrate PostgreSQL 迁移
sqlc.yaml                    sqlc 代码生成配置
docker-compose.yml           完整本地基础设施与应用编排
```

## 生产化边界

这是一个完整、可运行、可扩展的工程基线，不等同于无需改造即可公网运营的 SaaS。正式上线还应补充：登录与 RBAC、多租户数据隔离、文件解析/病毒扫描、混合检索和重排、Prompt/模型版本管理、指标与分布式追踪、离线/在线评测、敏感信息治理，以及 Kafka DLQ 与数据库备份策略。

参考：[OpenAI Responses API](https://developers.openai.com/api/reference/resources/responses)、[OpenAI Embeddings](https://developers.openai.com/api/reference/resources/embeddings)、[Milvus Docker Compose](https://milvus.io/docs/v2.6.x/install_standalone-docker-compose.md)、[Apache Kafka Docker](https://kafka.apache.org/41/getting-started/docker/)。
