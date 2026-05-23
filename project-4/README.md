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

**Service**	            **Purpose**	
postgres	           Metadata + pgvector database
minio	              S3-compatible object storage
ollama	            Local LLM inference
airflow-webserver	  Airflow UI
airflow-scheduler	  DAG execution
airflow-init	      Airflow database initialization

# Setup Instructions

Build and Start Services
```
docker compose up --build -d
```

First startup may take several minutes:

Ollama downloads the LLM model
Airflow initializes metadata database
Python dependencies are installed


Accessing Services
Service	        URL
Airflow UI	    http://localhost:8080
MinIO Console	http://localhost:9001
PostgreSQL	    localhost:5432
Ollama API	    http://localhost:11434 (running status visible)

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