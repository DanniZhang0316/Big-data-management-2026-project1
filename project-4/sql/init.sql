CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id INT PRIMARY KEY,
    app_package TEXT,
    category TEXT,
    png_path TEXT,
    hierarchy_json_path TEXT,
    run_id UUID
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    dag_id TEXT,
    git_sha TEXT,
    started_at TIMESTAMP,
    status TEXT,
    limit_param INT
);