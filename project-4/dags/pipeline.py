import os
import urllib.parse
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from src.audit import run_audit
from src.clients import get_postgres, get_s3
from src.embedding_image import (
    embed_screens as embed_image_screens,
    CLIP_MODEL_NAME,
    CLIP_MODEL_VERSION,
)
from src.embedding_text import (
    embed_screens as embed_text_screens,
    SBERT_MODEL_NAME,
    SBERT_MODEL_VERSION,
)
from src.evaluation import compute_recall_at_k, load_holdout_ids
from src.extraction import (
    extract_screens,
    PROMPT_VERSION,
    OLLAMA_MODEL,
)
from src.ingestion import (
    stream_rico,
    load_chosen_ids,
    run_ingestion,
)
from src.loading import (
    upsert_embeddings,
    upsert_extractions,
    stamp_run_versions,
)
from src.metrics import (
    Metric,
    collect_data_quality_metrics,
    collect_task_metrics,
    format_summary,
    upsert_metrics,
)
from src.run_context import create_run
from src.slack import (
    notify_failed_audit,
    notify_finished,
    notify_run_failed,
    notify_started,
)


BUCKET = os.environ.get("MINIO_BUCKET", "screens")

# Base URL for constructing Airflow task-log links sent in Slack messages.
# Set AIRFLOW__WEBSERVER__BASE_URL in your environment / .env to override.
_AIRFLOW_BASE_URL = os.environ.get(
    "AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080"
).rstrip("/")


def _airflow_task_url(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Return the Airflow grid URL for a specific task instance."""
    return (
        f"{_AIRFLOW_BASE_URL}/dags/{dag_id}/grid"
        f"?dag_run_id={urllib.parse.quote(dag_run_id, safe='')}"
        f"&task_id={urllib.parse.quote(task_id, safe='')}"
    )


# -------------------------
# DAG-level failure callback
# Fires when any task fails and the DAG run transitions to failed.
# The audit task handles its own richer notify_failed_audit; this
# callback covers every other failure (ingest crash, embed OOM, etc.).
# -------------------------
def _on_dag_failure(context):
    ti = context.get("task_instance") or context.get("ti")
    failed_task_id = ti.task_id if ti else "unknown"

    # Audit failures are already handled inside the audit task itself.
    if failed_task_id == "audit":
        return

    run_id = None
    try:
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
    except Exception:
        pass

    if run_id:
        conn = get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s,
                        ended_at = %s
                    WHERE run_id = %s
                    """,
                    ("FAILED", datetime.utcnow(), run_id),
                )
            conn.commit()
        finally:
            conn.close()

    notify_run_failed(
        run_id=run_id or "(run_id unavailable)",
        failed_task_id=failed_task_id,
        exception=str(context.get("exception", "unknown error")),
    )


# -------------------------
# DAG
# -------------------------
default_args = {"owner": "engineer_a"}

with DAG(
    dag_id="rico_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "LIMIT": Param(5, type="integer")
    },
    default_args=default_args,
    on_failure_callback=_on_dag_failure,
) as dag:

    # -------------------------
    # RUN INIT (TRACEABILITY)
    # -------------------------
    def init_run(**context):
        limit = context["params"]["LIMIT"]
        run = create_run(limit)
        context["ti"].xcom_push("run_id", run.run_id)
        context["ti"].xcom_push("limit", limit)

        # Stamp model versions so the pipeline_runs row is a complete witness.
        conn = get_postgres()
        try:
            stamp_run_versions(
                conn,
                run.run_id,
                clip_version=CLIP_MODEL_VERSION,
                sbert_version=SBERT_MODEL_VERSION,
                llm_model=OLLAMA_MODEL,
                prompt_version=PROMPT_VERSION,
            )
            conn.commit()
        finally:
            conn.close()

        # Determine how this run was triggered ("manual", "scheduled", etc.)
        dag_run = context.get("dag_run")
        trigger = str(getattr(dag_run, "run_type", "manual"))

        if dag_run is not None:
            conn = get_postgres()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE pipeline_runs
                        SET dag_run_id = %s
                        WHERE run_id = %s
                        """,
                        (dag_run.run_id, run.run_id),
                    )
                conn.commit()
            finally:
                conn.close()

        notify_started(run.run_id, limit, trigger)

    init_task = PythonOperator(
        task_id="init_run",
        python_callable=init_run,
    )

    # -------------------------
    # INGEST
    # -------------------------
    def ingest(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        limit  = context["ti"].xcom_pull(task_ids="init_run", key="limit")

        ds  = stream_rico()
        ids = load_chosen_ids("/opt/airflow/config/chosen_screens.txt")
        ids = ids[:limit]

        s3   = get_s3()
        conn = get_postgres()
        try:
            run_ingestion(ds, ids, s3, conn, run_id, BUCKET)
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("screen_ids", ids)

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    # -------------------------
    # PARSE (Engineer A minimal version hook)
    # -------------------------
    def parse(**context):
        # Parsing logic lives in src/parsing.py and is invoked lazily by the
        # text-embedding and extraction tasks (one parse per screen, two consumers).
        return "parse_stage_placeholder"

    parse_task = PythonOperator(
        task_id="parse",
        python_callable=parse,
    )

    # -------------------------
    # EMBED IMAGE  (CLIP)
    # -------------------------
    def embed_image(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        ids    = context["ti"].xcom_pull(task_ids="ingest",   key="screen_ids")

        s3    = get_s3()
        items = embed_image_screens(s3, BUCKET, ids)

        conn = get_postgres()
        try:
            inserted = upsert_embeddings(
                conn,
                run_id,
                model_name=CLIP_MODEL_NAME,
                model_version=CLIP_MODEL_VERSION,
                embedding_kind="image",
                items=items,
            )
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("rows_in",       len(ids))
        context["ti"].xcom_push("rows_inserted", inserted)

    embed_image_task = PythonOperator(
        task_id="embed_image",
        python_callable=embed_image,
    )

    # -------------------------
    # EMBED TEXT  (SBERT)
    # -------------------------
    def embed_text(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        ids    = context["ti"].xcom_pull(task_ids="ingest",   key="screen_ids")

        s3    = get_s3()
        items = embed_text_screens(s3, BUCKET, ids)

        conn = get_postgres()
        try:
            inserted = upsert_embeddings(
                conn,
                run_id,
                model_name=SBERT_MODEL_NAME,
                model_version=SBERT_MODEL_VERSION,
                embedding_kind="text",
                items=items,
            )
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("rows_in",       len(ids))
        context["ti"].xcom_push("rows_inserted", inserted)

    embed_text_task = PythonOperator(
        task_id="embed_text",
        python_callable=embed_text,
    )

    # -------------------------
    # EXTRACT  (Ollama LLM)
    # -------------------------
    def extract(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        ids    = context["ti"].xcom_pull(task_ids="ingest",   key="screen_ids")

        s3      = get_s3()
        results = extract_screens(s3, BUCKET, ids)

        conn = get_postgres()
        try:
            counts = upsert_extractions(conn, run_id, PROMPT_VERSION, results)
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("rows_in",          len(ids))
        context["ti"].xcom_push("rows_updated",      counts["updated"])
        context["ti"].xcom_push("review_queue_rows", counts["review_queue"])

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # -------------------------
    # LOAD  (sync barrier)
    # Destination writes already happened idempotently inside each compute
    # task; this task verifies row counts and is the synchronisation point
    # before the audit.
    # -------------------------
    def load(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")

        conn = get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM screens_metadata WHERE run_id = %s",
                    (run_id,),
                )
                n_meta = cur.fetchone()[0]
                cur.execute(
                    "SELECT count(*) FROM screens_embeddings WHERE run_id = %s",
                    (run_id,),
                )
                n_emb = cur.fetchone()[0]
                cur.execute(
                    "SELECT count(*) FROM screens_review_queue WHERE run_id = %s",
                    (run_id,),
                )
                n_review = cur.fetchone()[0]
        finally:
            conn.close()

        print(
            f"[load] run_id={run_id} screens_metadata={n_meta} "
            f"screens_embeddings={n_emb} screens_review_queue={n_review}"
        )
        context["ti"].xcom_push("screens_metadata_rows",     n_meta)
        context["ti"].xcom_push("screens_embeddings_rows",   n_emb)
        context["ti"].xcom_push("screens_review_queue_rows", n_review)

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # -------------------------
    # AUDIT  (Engineer C — duplicate-detection circuit-breaker)
    #
    # -------------------------
    def audit(**context):
        run_id  = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        dag_run = context["dag_run"]
        log_url = _airflow_task_url(dag.dag_id, dag_run.run_id, "audit")

        conn = get_postgres()
        try:
            results = run_audit(conn, run_id)
        finally:
            conn.close()

        failed = [r for r in results if not r.passed]

        if not failed:
            print(
                f"[audit] ALL PASSED for run_id={run_id}  "
                f"checks: {', '.join(r.name for r in results)}"
            )
            return

        # Log every duplicate key in full to the Airflow task log
        for r in failed:
            print(f"[audit] FAILED check={r.name}")
            print(f"[audit]   details={r.details}")

        # Send scannable Slack alert BEFORE raising so it always fires
        notify_failed_audit(
            run_id=run_id,
            failed_checks=[{"name": r.name, "details": r.details} for r in failed],
            log_url=log_url,
        )

        # Halt the DAG — eval must not run on bad data
        check_names = ", ".join(r.name for r in failed)
        raise AirflowException(
            f"[audit] {len(failed)} check(s) FAILED for run_id={run_id}: "
            f"{check_names} — see task log and audit_results table for details"
        )

    audit_task = PythonOperator(
        task_id="audit",
        python_callable=audit,
    )

    # -------------------------
    # EVAL (Recall@5)
    # -------------------------
    def evaluate(**context):
        run_id      = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        holdout_ids = load_holdout_ids("/opt/airflow/config/holdout_screens.txt")

        conn = get_postgres()
        try:
            result = compute_recall_at_k(conn, holdout_ids, k=5)
            upsert_metrics(
                conn,
                run_id,
                [
                    Metric(
                        name="recall_at_5",
                        value=result.recall_at_k,
                        labels={
                            "relevance":      "same_app_package",
                            "embedding_kind": "text",
                        },
                    ),
                    Metric(name="eval_total",   value=float(result.total)),
                    Metric(name="eval_skipped", value=float(result.skipped)),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("recall_at_5",  result.recall_at_k)
        context["ti"].xcom_push("eval_total",   result.total)
        context["ti"].xcom_push("eval_skipped", result.skipped)

    eval_task = PythonOperator(
        task_id="eval",
        python_callable=evaluate,
    )

    # -------------------------
    # METRICS (health + data quality)
    # -------------------------
    def collect_metrics(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")

        conn = get_postgres()
        try:
            task_metrics         = collect_task_metrics(context)
            data_quality_metrics = collect_data_quality_metrics(conn, run_id)
            upsert_metrics(conn, run_id, task_metrics + data_quality_metrics)

            # Compute wall-clock duration from the pipeline_runs row
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT started_at FROM pipeline_runs WHERE run_id = %s",
                    (run_id,),
                )
                row = cur.fetchone()

            conn.commit()
        finally:
            conn.close()

        duration_s = 0.0
        if row and row[0]:
            # started_at is stored as a naive UTC timestamp
            duration_s = (datetime.utcnow() - row[0]).total_seconds()

        recall_at_5  = context["ti"].xcom_pull(task_ids="eval", key="recall_at_5")
        eval_total   = context["ti"].xcom_pull(task_ids="eval", key="eval_total")
        eval_skipped = context["ti"].xcom_pull(task_ids="eval", key="eval_skipped")
        n_meta       = context["ti"].xcom_pull(task_ids="load", key="screens_metadata_rows")
        n_emb        = context["ti"].xcom_pull(task_ids="load", key="screens_embeddings_rows")

        summary_line = format_summary(data_quality_metrics)
        print(
            f"{summary_line} recall_at_5={recall_at_5} "
            f"eval_total={eval_total} eval_skipped={eval_skipped}"
        )

        conn = get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s,
                        ended_at = %s
                    WHERE run_id = %s
                    """,
                    ("SUCCESS", datetime.utcnow(), run_id),
                )
            conn.commit()
        finally:
            conn.close()

        notify_finished(
            run_id=run_id,
            duration_s=duration_s,
            summary_line=summary_line,
            recall_at_5=recall_at_5,
            n_meta=n_meta,
            n_emb=n_emb,
        )

    metrics_task = PythonOperator(
        task_id="metrics",
        python_callable=collect_metrics,
    )

    # -------------------------
    # DAG FLOW
    # -------------------------
    init_task >> ingest_task >> parse_task
    parse_task >> [embed_image_task, embed_text_task, extract_task] >> load_task
    load_task >> audit_task >> eval_task >> metrics_task
