.PHONY: init test vet run worker usage-worker migrate sqlc usage usage-summary up stop down clean logs ps

# 首次初始化：创建本地配置、校验必要密钥，并启动整套服务。
# 已存在的 .env 不会被覆盖。
init:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "已从 .env.example 创建 .env，请填写 OPENAI_API_KEY 后重新执行 make init"; \
		exit 1; \
	fi
	@if ! grep -Eq '^OPENAI_API_KEY=.+$$' .env || grep -Eq '^OPENAI_API_KEY=(replace-with-your-api-key|你的_API_Key)$$' .env; then \
		echo "错误：请先在 .env 中配置有效的 OPENAI_API_KEY"; \
		exit 1; \
	fi
	@if ! grep -Eq '^POSTGRES_PASSWORD=.+$$' .env || grep -Eq '^POSTGRES_PASSWORD=(replace-with-a-strong-password|ai-platform-local)$$' .env; then \
		echo "错误：请先在 .env 中配置非默认的 POSTGRES_PASSWORD"; \
		exit 1; \
	fi
	@if ! grep -Eq '^POSTGRES_DSN=.+$$' .env || grep -Eq '^POSTGRES_DSN=.*replace-with-a-strong-password' .env; then \
		echo "错误：请在 .env 中配置与 POSTGRES_PASSWORD 一致的 POSTGRES_DSN"; \
		exit 1; \
	fi
	docker compose up --build -d --wait
	docker compose ps

test:
	go test ./...

vet:
	go vet ./...

run:
	go run ./cmd/server

worker:
	go run ./cmd/worker

usage-worker:
	go run ./cmd/usage-worker

# 执行 PostgreSQL schema 迁移；重复执行会安全地报告 no change。
migrate:
	docker compose run --rm migrate

# 根据 migrations 与 query.sql 重新生成类型安全的 pgx 查询代码。
sqlc:
	docker run --rm -v "$(CURDIR):/src" -w /src sqlc/sqlc:1.31.1 generate

# 查看最近的模型/Embedding 用量明细，不对外暴露管理 API。
usage:
	docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT created_at, trace_id, route, operation, model, call_index, input_tokens, cached_input_tokens, output_tokens, reasoning_tokens, total_tokens, status FROM llm_usage_records ORDER BY created_at DESC LIMIT 30;"'

# 按天、模型和调用类型汇总 token 用量。
usage-summary:
	docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT date_trunc('\''day'\'', created_at) AS day, model, operation, count(*) AS calls, sum(input_tokens) AS input_tokens, sum(cached_input_tokens) AS cached_input_tokens, sum(output_tokens) AS output_tokens, sum(total_tokens) AS total_tokens FROM llm_usage_records WHERE status = '\''completed'\'' GROUP BY 1, 2, 3 ORDER BY 1 DESC, 2, 3;"'

up:
	docker compose up --build -d

# 停止本项目全部容器，但保留容器和数据卷，可用 make up 恢复。
stop:
	docker compose stop

# 删除本项目容器和网络，但保留持久化数据卷。
down:
	docker compose down --remove-orphans

# 删除本项目容器、网络和持久化数据卷。此操作会清空 Milvus、Redis、Kafka、PostgreSQL 数据。
clean:
	docker compose down --volumes --remove-orphans

logs:
	docker compose logs -f api worker usage-worker

ps:
	docker compose ps
