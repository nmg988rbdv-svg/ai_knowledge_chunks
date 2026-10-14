.PHONY: init test vet run worker up stop down clean logs ps

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

up:
	docker compose up --build -d

# 停止本项目全部容器，但保留容器和数据卷，可用 make up 恢复。
stop:
	docker compose stop

# 删除本项目容器和网络，但保留持久化数据卷。
down:
	docker compose down --remove-orphans

# 删除本项目容器、网络和持久化数据卷。此操作会清空 Milvus、Redis、Kafka 数据。
clean:
	docker compose down --volumes --remove-orphans

logs:
	docker compose logs -f api worker

ps:
	docker compose ps
