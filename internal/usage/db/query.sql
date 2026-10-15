-- name: InsertUsageRecord :execrows
INSERT INTO llm_usage_records (
    id,
    trace_id,
    request_id,
    response_id,
    conversation_id,
    user_id,
    provider,
    model,
    route,
    operation,
    call_index,
    input_tokens,
    cached_input_tokens,
    cache_write_tokens,
    output_tokens,
    reasoning_tokens,
    total_tokens,
    duration_ms,
    status,
    error_code,
    metadata,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
)
ON CONFLICT DO NOTHING;
