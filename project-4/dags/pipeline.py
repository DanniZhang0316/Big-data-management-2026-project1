from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from src.clients import get_postgres, get_s3, get_s3
from src.ingestion import (
    stream_rico,
    load_chosen_ids,
    run_ingestion,
)
from src.run_context import create_run


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

        s3 = get_s3()
        conn = get_postgres()

        run_ingestion(ds, ids, s3, conn, run_id, "rico-raw")

        conn.commit()
        conn.close()

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    # -------------------------
    # PARSE (Engineer A minimal version hook)
    # -------------------------
    def parse(**context):
        # kept intentionally minimal (logic lives in src/parsing.py)
        return "parse_stage_placeholder"

    parse_task = PythonOperator(
        task_id="parse",
        python_callable=parse,
    )

    # -------------------------
    # DAG FLOW
    # -------------------------
    init_task >> ingest_task >> parse_task