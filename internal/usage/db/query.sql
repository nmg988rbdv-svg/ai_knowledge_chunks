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

-- name: GetUsageSummary :one
SELECT
    count(*)::bigint AS total_calls,
    count(*) FILTER (WHERE status = 'completed')::bigint AS completed_calls,
    count(*) FILTER (WHERE status = 'failed')::bigint AS failed_calls,
    coalesce(sum(input_tokens), 0)::bigint AS input_tokens,
    coalesce(sum(cached_input_tokens), 0)::bigint AS cached_input_tokens,
    coalesce(sum(output_tokens), 0)::bigint AS output_tokens,
    coalesce(sum(reasoning_tokens), 0)::bigint AS reasoning_tokens,
    coalesce(sum(total_tokens), 0)::bigint AS total_tokens
FROM llm_usage_records;

-- name: ListRecentUsageRecords :many
SELECT
    id,
    trace_id,
    model,
    route,
    operation,
    call_index,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_tokens,
    total_tokens,
    duration_ms,
    status,
    error_code,
    created_at
FROM llm_usage_records
ORDER BY created_at DESC
LIMIT $1;
