-- ============================================================================
-- pgvector extension
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS vector;


-- ============================================================================
-- pipeline_runs (Engineer A — traceability)
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        UUID PRIMARY KEY,
    dag_id        TEXT,
    dag_run_id    TEXT,
    git_sha       TEXT,
    started_at    TIMESTAMP,
    ended_at      TIMESTAMP,
    status        TEXT,
    limit_param   INT,
    clip_version  TEXT,
    sbert_version TEXT,
    llm_model     TEXT,
    prompt_version TEXT
);

-- additive columns (no-op if already present)
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS dag_run_id     TEXT;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ended_at       TIMESTAMP;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS clip_version   TEXT;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS sbert_version  TEXT;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS llm_model      TEXT;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS prompt_version TEXT;


-- ============================================================================
-- screens_metadata (Engineer A — extended by Engineer B for traceability)
-- ============================================================================
CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id           INT PRIMARY KEY,
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT,
    hierarchy_json_path TEXT,
    run_id              UUID,
    source_fingerprint  TEXT,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    updated_at          TIMESTAMP
);

ALTER TABLE screens_metadata ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;
ALTER TABLE screens_metadata ADD COLUMN IF NOT EXISTS extraction_payload JSONB;
ALTER TABLE screens_metadata ADD COLUMN IF NOT EXISTS prompt_version     TEXT;
ALTER TABLE screens_metadata ADD COLUMN IF NOT EXISTS confidence         DOUBLE PRECISION;
ALTER TABLE screens_metadata ADD COLUMN IF NOT EXISTS updated_at         TIMESTAMP;


-- ============================================================================
-- screens_embeddings (Engineer B — image + text vectors share this table)
-- ============================================================================
CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id          INT       NOT NULL,
    model_name         TEXT      NOT NULL,
    model_version      TEXT      NOT NULL,
    embedding_kind     TEXT      NOT NULL,
    vector             vector    NOT NULL,
    run_id             UUID,
    source_fingerprint TEXT,
    created_at         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
);


-- ============================================================================
-- screens_review_queue (Engineer B — invalid JSON / low-confidence routing)
-- ============================================================================
CREATE TABLE IF NOT EXISTS screens_review_queue (
    id                 BIGSERIAL PRIMARY KEY,
    screen_id          INT       NOT NULL,
    run_id             UUID      NOT NULL,
    reason             TEXT      NOT NULL,
    raw_response       TEXT,
    confidence         DOUBLE PRECISION,
    source_fingerprint TEXT,
    created_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (screen_id, run_id, reason)
);


-- ============================================================================
-- audit_results (Engineer C bonus — store audit history per run)
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- pipeline_metrics (Engineer D — health + data quality per run)
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL,
    metric_name  TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_text  TEXT,
    labels       JSONB,
    created_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, metric_name, labels)
);
