from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from src.clients import get_postgres, get_s3
from src.ingestion import (
    stream_rico,
    load_chosen_ids,
    run_ingestion,
)
from src.run_context import create_run
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
from src.extraction import (
    extract_screens,
    PROMPT_VERSION,
    OLLAMA_MODEL,
)
from src.loading import (
    upsert_embeddings,
    upsert_extractions,
    stamp_run_versions,
)


BUCKET = "rico-raw"


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
    default_args=default_args
) as dag:

    # -------------------------
    # RUN INIT (TRACEABILITY)
    # -------------------------
    def init_run(**context):
        limit = context["params"]["LIMIT"]
        run = create_run(limit)
        context["ti"].xcom_push("run_id", run.run_id)
        context["ti"].xcom_push("limit", limit)

        # stamp model versions on pipeline_runs so the row is a complete witness
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

    init_task = PythonOperator(
        task_id="init_run",
        python_callable=init_run,
    )

    # -------------------------
    # INGEST
    # -------------------------
    def ingest(**context):
        run_id = context["ti"].xcom_pull(task_ids="init_run", key="run_id")
        limit = context["ti"].xcom_pull(task_ids="init_run", key="limit")

        ds = stream_rico()
        ids = load_chosen_ids("/opt/airflow/config/chosen_screens.txt")
        ids = ids[:limit]

        s3 = get_s3()
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
        # parsing logic lives in src/parsing.py and is invoked lazily by the
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
        ids = context["ti"].xcom_pull(task_ids="ingest", key="screen_ids")

        s3 = get_s3()
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

        context["ti"].xcom_push("rows_in", len(ids))
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
        ids = context["ti"].xcom_pull(task_ids="ingest", key="screen_ids")

        s3 = get_s3()
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

        context["ti"].xcom_push("rows_in", len(ids))
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
        ids = context["ti"].xcom_pull(task_ids="ingest", key="screen_ids")

        s3 = get_s3()
        results = extract_screens(s3, BUCKET, ids)

        conn = get_postgres()
        try:
            counts = upsert_extractions(conn, run_id, PROMPT_VERSION, results)
            conn.commit()
        finally:
            conn.close()

        context["ti"].xcom_push("rows_in", len(ids))
        context["ti"].xcom_push("rows_updated", counts["updated"])
        context["ti"].xcom_push("review_queue_rows", counts["review_queue"])

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # -------------------------
    # LOAD  (sync barrier — destination writes already happened idempotently
    # inside each compute task; this task verifies row counts for the run
    # and is the synchronization point before audit).
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
        context["ti"].xcom_push("screens_metadata_rows", n_meta)
        context["ti"].xcom_push("screens_embeddings_rows", n_emb)
        context["ti"].xcom_push("screens_review_queue_rows", n_review)

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # -------------------------
    # DAG FLOW
    # -------------------------
    init_task >> ingest_task >> parse_task
    parse_task >> [embed_image_task, embed_text_task, extract_task] >> load_task
