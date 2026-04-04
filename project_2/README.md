# Project 2 — Streaming Lakehouse Pipeline

Build a streaming pipeline that consumes NYC taxi events from Kafka, processes them
with Spark Structured Streaming, and lands data into an Apache Iceberg lakehouse
using the medallion architecture (bronze → silver → gold).

---

## What's in this template

| Path | Description |
|------|-------------|
| `compose.yml` | Kafka, MinIO, Iceberg REST catalog, Jupyter/PySpark |
| `produce.py` | Replays a parquet file as JSON into the `taxi-trips` Kafka topic |
| `REPORT.md` | Template for the report you need to hand in |
| `.env.example` | Template for credentials — copy to `.env` and fill in values |
| `data/` | **Not in git.** Place the provided parquet files here (see Setup) |

The `data/` directory is git-ignored. You will use the same files as in project 1:

| File | Description |
|------|-------------|
| `data/yellow_tripdata_2025-01.parquet` | NYC Yellow Taxi trips — January 2025 |
| `data/yellow_tripdata_2025-02.parquet` | NYC Yellow Taxi trips — February 2025 |
| `data/taxi_zone_lookup.parquet` | Pickup/dropoff zone names (join with `PULocationID` / `DOLocationID`) |

---

## Setup

### 1. Configure credentials

Copy the example env file and set your own secrets:

```bash
cp .env.example .env
# Edit .env — change the passwords before starting the stack
```

The `.env` file is git-ignored and never committed.

You need to change all the default secrets, and provide them in `REPORT.md` section 7 in your project submission. 

### 2. Place the data files

Put the three parquet files provided for the project into the `data/` directory:

```
project_2/
└── data/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── taxi_zone_lookup.parquet
```

### 3. Start the stack

```bash
docker compose up -d
```

Boot order is enforced automatically: MinIO starts → bucket is created → Iceberg REST
catalog starts → Jupyter starts. Allow ~20 seconds for all services to become ready.

### 4. Verify services

```bash
docker ps
```

All five services (`kafka`, `minio`, `minio_init`, `iceberg-rest`, `jupyter`) should
show **running** (or **exited** for `minio_init`, which is a one-shot job).

### 5. Create the topic and start the producer

Create the Kafka topic (do this once after the stack is up):

```bash
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1"
```

Then open a **Jupyter terminal** (File → New Terminal in JupyterLab) and run:

```bash
python project/produce.py             # 5 events/s, single pass (January data)
python project/produce.py --loop      # replay indefinitely
python project/produce.py --rate 20   # faster replay
python project/produce.py --data data/yellow_tripdata_2025-02.parquet  # February data
```

Or from a host terminal:

```bash
docker exec project2_jupyter python /home/jovyan/project/produce.py --loop
```

### 6. Open Jupyter

Navigate to **http://localhost:8888** — token is set in your `.env` file (`JUPYTER_TOKEN`).

Your project files are at `~/project/` inside the notebook.

### 7. Stop the stack

```bash
docker compose down          # keeps MinIO data (named volume)
docker compose down -v       # also deletes stored Iceberg tables
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | token: value of `JUPYTER_TOKEN` in `.env` |
| Spark UI | http://localhost:4040 | — |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Iceberg REST API | http://localhost:8181/v1/namespaces | — |

---

## Kafka topic schema

`produce.py` publishes one JSON message per taxi trip. Each message has:

| Field | Type | Notes |
|-------|------|-------|
| `VendorID` | int | 1 or 2 — used as Kafka message key |
| `tpep_pickup_datetime` | ISO-8601 string | event time for windowing |
| `tpep_dropoff_datetime` | ISO-8601 string | |
| `passenger_count` | int | |
| `trip_distance` | float | miles |
| `PULocationID` | int | join with `taxi_zone_lookup.parquet` |
| `DOLocationID` | int | join with `taxi_zone_lookup.parquet` |
| `fare_amount` | float | |
| `tip_amount` | float | |
| `total_amount` | float | |
| `payment_type` | int | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `congestion_surcharge` | float | |
| *(+ other TLC fields)* | | |

---

## Troubleshooting

**`Failed to find data source: kafka`**
The Kafka connector jar failed to download. Check `PYSPARK_SUBMIT_ARGS` in
`compose.yml` — the version must match your Spark version (see `spark.version`).

**`Failed to find data source: iceberg`**
Same as above but for the Iceberg runtime jar.

**`Connection refused` to MinIO or iceberg-rest**
Services may still be starting. Wait 20–30 seconds and retry.
Check `docker compose logs iceberg-rest` for errors.

**Iceberg table not found after restart**
Tables are stored in MinIO (persistent named volume). They survive container restarts
as long as you don't run `docker compose down -v`.

**Wrong Spark / Scala version**
`pyspark-notebook:2025-12-31` ships Spark 4.1.0 (Scala 2.13). If `spark.version` shows a
different version, update the package coordinates in `PYSPARK_SUBMIT_ARGS` in `compose.yml`.
Spark 3.5.x uses `_2.12`; Spark 4.x uses `_2.13`. Also update the Iceberg artifact:
`iceberg-spark-runtime-4.0_2.13` for Spark 4.x, `iceberg-spark-runtime-3.5_2.12` for Spark 3.5.x.
