-- ============================================================================
-- Engineer B verification queries.
-- Run with:  docker compose exec postgres psql -U rico -d rico -f /verify.sql
-- ============================================================================

\echo
\echo '--- 1. Schema sanity: required tables and columns exist ---'
\dt+ screens_metadata
\dt+ screens_embeddings
\dt+ screens_review_queue
\dt+ pipeline_runs

\echo
\echo '--- 2. Every row in destination tables has a non-null run_id and fingerprint ---'
SELECT
    'screens_metadata' AS tbl,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE run_id IS NULL) AS null_run_id,
    COUNT(*) FILTER (WHERE source_fingerprint IS NULL) AS null_fingerprint
FROM screens_metadata
UNION ALL
SELECT
    'screens_embeddings',
    COUNT(*),
    COUNT(*) FILTER (WHERE run_id IS NULL),
    COUNT(*) FILTER (WHERE source_fingerprint IS NULL)
FROM screens_embeddings;

\echo
\echo '--- 3. Embedding row counts by (kind, model_version) ---'
SELECT embedding_kind, model_name, model_version, COUNT(*) AS n
FROM screens_embeddings
GROUP BY 1, 2, 3
ORDER BY 1, 2;

\echo
\echo '--- 4. Vector dimensions per kind (should be 512 for image, 384 for text) ---'
SELECT embedding_kind, vector_dims(vector) AS dim, COUNT(*) AS n
FROM screens_embeddings
GROUP BY 1, 2
ORDER BY 1;

\echo
\echo '--- 5. Extraction quality: % non-null payload, % conf>=0.5 ---'
SELECT
    COUNT(*)                                                                 AS rows_in_run,
    COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL)::float / NULLIF(COUNT(*), 0) AS pct_extracted,
    COUNT(*) FILTER (WHERE confidence >= 0.5)::float / NULLIF(COUNT(*), 0)             AS pct_high_conf,
    COUNT(*) FILTER (WHERE prompt_version IS NOT NULL)                                  AS rows_with_prompt
FROM screens_metadata
WHERE run_id IS NOT NULL;

\echo
\echo '--- 6. Review queue (low-confidence + invalid-JSON routes) ---'
SELECT screen_id, run_id, reason, confidence, LEFT(COALESCE(raw_response,''), 120) AS raw_preview
FROM screens_review_queue
ORDER BY created_at DESC
LIMIT 20;

\echo
\echo '--- 7. Pipeline runs (with model versions) ---'
SELECT run_id, status, limit_param, git_sha, clip_version, sbert_version, llm_model, prompt_version, started_at
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 5;

\echo
\echo '--- 8. IDEMPOTENCY: count rows that share a primary key (should be 0) ---'
SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*) AS dup_count
FROM screens_embeddings
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;
