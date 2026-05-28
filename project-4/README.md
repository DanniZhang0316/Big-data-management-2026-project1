# Project 4 — From Notebook to Production Pipeline

Production-style implementation of the RICO multimodal retrieval pipeline using Apache Airflow, PostgreSQL + pgvector, MinIO, and Ollama.

# Architecture Overview

The pipeline processes the RICO Screen2Words dataset through multiple stages:

```
ingest
    ↓
parse
    ↓
 ┌──────────────┬──────────────┬──────────────┐
 │              │              │
embed_image  embed_text     extract
 └──────────────┴──────────────┴──────────────┘
                    ↓
                  load
                    ↓
                  audit
                    ↓
                   eval
```

Infrastructure components:

Apache Airflow — orchestration and scheduling
PostgreSQL + pgvector — metadata and vector storage
MinIO — object storage for raw images
Ollama — local LLM inference
HuggingFace datasets — RICO dataset source

# Infrastructure Stack

The project uses Docker Compose to start the full pipeline environment.

Services:

**Service** **Purpose**
postgres Metadata + pgvector database
minio S3-compatible object storage
ollama Local LLM inference
airflow-webserver Airflow UI
airflow-scheduler DAG execution
airflow-init Airflow database initialization

# Setup Instructions

Build and Start Services

```
docker compose up --build -d
```

Environment Variables

Set these in a `.env` file in the project root or export them in your shell:

```
POSTGRES_DB=project4
POSTGRES_USER=project4
POSTGRES_PASSWORD=project4

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=screens

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

First startup may take several minutes:

Ollama downloads the LLM model
Airflow initializes metadata database
Python dependencies are installed

Accessing Services
Service URL
Airflow UI http://localhost:8080
MinIO Console http://localhost:9001
PostgreSQL localhost:5432
Ollama API http://localhost:11434 (running status visible)

# Evaluation

Eval is computed as Recall@5 using SBERT text embeddings. For each holdout
screen, we retrieve the top-5 nearest neighbors by cosine distance and count
a hit if any neighbor shares the same `app_package`. The metric is reported
as a hit-rate over the holdout set.

Holdout IDs live in:

```
config/holdout_screens.txt
```

# Metrics

Pipeline metrics are written to `pipeline_metrics` and include:

Health metrics:

- task_duration_seconds per task_id
- task_retries per task_id
- rows per task_id and kind (rows_in, rows_inserted, etc.)

Data quality metrics:

- extraction_non_null_pct
- confidence_in_bounds_pct
- embedding_avg_norm
- embedding_norm_outlier_pct
- distinct_app_packages
- distinct_categories

At the end of the run, a one-line summary is printed with key metrics and
the eval Recall@5 numbers.

# Audit

The current DAG includes a placeholder audit task that always passes. When
the real audit task is implemented, it should replace the placeholder and
block eval/metrics if the audit fails.

# Current Pipeline Stages

## 1. Ingestion

The pipeline streams selected screens from the RICO dataset and stores:

MinIO
screens/{id}.png
screens/{id}.json
PostgreSQL

Table: screens_metadata

Stored metadata:

screen_id
app_package
category
object storage paths
run_id

Features:

deterministic object keys
idempotent UPSERT logic
run traceability

## 2. Parsing

The parser:

loads hierarchy JSON from MinIO
performs iterative DFS traversal
extracts UI elements
preserves reading order
creates flattened text representations

# Database Tables

**screens_metadata**

Stores:

screen metadata
MinIO object paths
pipeline run linkage

**pipeline_runs**

Stores:

run_id
git SHA
execution timestamp
DAG metadata
runtime parameters
