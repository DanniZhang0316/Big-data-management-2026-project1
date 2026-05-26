"""
Idempotent destination-table writes.

This is the single place in the codebase where rows land in the destination
tables. Every INSERT is run-aware (carries run_id) and carries a source
fingerprint, and every INSERT is wrapped in ON CONFLICT logic so that a
re-run of the DAG with the same LIMIT produces zero new rows.
"""

import json
from typing import Iterable, Mapping, Sequence

from pgvector.psycopg import register_vector


# ---------------------------------------------------------------------------
# IMAGE EMBEDDINGS — CLIP vectors land here, kind='image'
# ---------------------------------------------------------------------------
INSERT_EMBEDDING_SQL = """
INSERT INTO screens_embeddings
    (screen_id, model_name, model_version, embedding_kind, vector,
     run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
DO NOTHING
"""


def upsert_embeddings(
    conn,
    run_id: str,
    model_name: str,
    model_version: str,
    embedding_kind: str,
    items: Sequence[Mapping],
) -> int:
    """
    items: iterable of {"screen_id": int, "vector": np.ndarray, "fingerprint": str}.

    Returns the number of rows the database considered new (rowcount sum).
    ON CONFLICT DO NOTHING means a re-run lands zero new rows.
    """
    register_vector(conn)
    inserted = 0
    with conn.cursor() as cur:
        for item in items:
            cur.execute(
                INSERT_EMBEDDING_SQL,
                (
                    int(item["screen_id"]),
                    model_name,
                    model_version,
                    embedding_kind,
                    item["vector"],
                    run_id,
                    item["fingerprint"],
                ),
            )
            inserted += cur.rowcount
    return inserted


# ---------------------------------------------------------------------------
# EXTRACTION — UPDATE in place on screens_metadata (every row already exists
# from the ingest stage), and route bad / low-confidence rows to the review
# queue.
# ---------------------------------------------------------------------------
UPDATE_EXTRACTION_SQL = """
UPDATE screens_metadata
SET extraction_payload = %s::jsonb,
    prompt_version     = %s,
    confidence         = %s,
    run_id             = %s,
    updated_at         = NOW()
WHERE screen_id = %s
"""

INSERT_REVIEW_QUEUE_SQL = """
INSERT INTO screens_review_queue
    (screen_id, run_id, reason, raw_response, confidence, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id, run_id, reason) DO NOTHING
"""


def upsert_extractions(
    conn,
    run_id: str,
    prompt_version: str,
    items: Iterable[Mapping],
    low_confidence_threshold: float = 0.5,
) -> dict:
    """
    items: iterable of dicts with keys:
        screen_id, ok (bool), payload (dict|None), confidence (float|None),
        raw_response (str), fingerprint (str), error (str|None)

    Successful extractions UPDATE screens_metadata.
    Failed (invalid JSON) or low-confidence extractions are routed to
    screens_review_queue.
    """
    n_ok = 0
    n_review = 0

    with conn.cursor() as cur:
        for item in items:
            sid = int(item["screen_id"])
            ok = bool(item.get("ok"))
            confidence = item.get("confidence")
            payload = item.get("payload")
            fingerprint = item.get("fingerprint")
            raw_response = item.get("raw_response")
            error = item.get("error")

            if ok and payload is not None:
                # Strip confidence from payload body — it has its own column.
                body = {k: v for k, v in payload.items() if k != "confidence"}
                cur.execute(
                    UPDATE_EXTRACTION_SQL,
                    (
                        json.dumps(body),
                        prompt_version,
                        float(confidence) if confidence is not None else None,
                        run_id,
                        sid,
                    ),
                )
                n_ok += 1

                if (
                    confidence is not None
                    and float(confidence) < low_confidence_threshold
                ):
                    cur.execute(
                        INSERT_REVIEW_QUEUE_SQL,
                        (
                            sid,
                            run_id,
                            "low_confidence",
                            raw_response,
                            float(confidence),
                            fingerprint,
                        ),
                    )
                    n_review += 1
            else:
                cur.execute(
                    INSERT_REVIEW_QUEUE_SQL,
                    (
                        sid,
                        run_id,
                        error or "invalid_json",
                        raw_response,
                        None,
                        fingerprint,
                    ),
                )
                n_review += 1

    return {"updated": n_ok, "review_queue": n_review}


# ---------------------------------------------------------------------------
# Run-level model-version stamping — record which models a run used so the
# pipeline_runs row is a complete witness of "what code/models produced
# these rows?".
# ---------------------------------------------------------------------------
UPDATE_RUN_MODEL_VERSIONS_SQL = """
UPDATE pipeline_runs
SET clip_version   = COALESCE(%s, clip_version),
    sbert_version  = COALESCE(%s, sbert_version),
    llm_model      = COALESCE(%s, llm_model),
    prompt_version = COALESCE(%s, prompt_version)
WHERE run_id = %s
"""


def stamp_run_versions(
    conn,
    run_id: str,
    clip_version: str | None = None,
    sbert_version: str | None = None,
    llm_model: str | None = None,
    prompt_version: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            UPDATE_RUN_MODEL_VERSIONS_SQL,
            (clip_version, sbert_version, llm_model, prompt_version, run_id),
        )
