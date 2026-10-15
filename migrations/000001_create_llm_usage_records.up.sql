CREATE TABLE llm_usage_records (
    id UUID PRIMARY KEY,
    trace_id VARCHAR(96) NOT NULL,
    request_id VARCHAR(96) NOT NULL,
    response_id VARCHAR(160) NOT NULL DEFAULT '',
    conversation_id VARCHAR(96) NOT NULL DEFAULT '',
    user_id VARCHAR(96) NOT NULL DEFAULT '',
    provider VARCHAR(32) NOT NULL,
    model VARCHAR(128) NOT NULL,
    route VARCHAR(128) NOT NULL DEFAULT '',
    operation VARCHAR(32) NOT NULL,
    call_index INTEGER NOT NULL CHECK (call_index > 0),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    cached_input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (cached_input_tokens >= 0),
    cache_write_tokens BIGINT NOT NULL DEFAULT 0 CHECK (cache_write_tokens >= 0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    reasoning_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
    total_tokens BIGINT NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
    estimated_cost_usd NUMERIC(18, 10),
    pricing_version VARCHAR(64),
    duration_ms BIGINT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'failed')),
    error_code VARCHAR(96) NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_llm_usage_provider_response
    ON llm_usage_records (provider, response_id)
    WHERE response_id <> '';

CREATE INDEX idx_llm_usage_created_at
    ON llm_usage_records (created_at DESC);

CREATE INDEX idx_llm_usage_user_created
    ON llm_usage_records (user_id, created_at DESC)
    WHERE user_id <> '';

CREATE INDEX idx_llm_usage_model_created
    ON llm_usage_records (model, created_at DESC);

CREATE INDEX idx_llm_usage_trace
    ON llm_usage_records (trace_id, call_index);
