"""
Evaluation utilities for the pipeline (Engineer D).

Current definition: Recall@K using SBERT text embeddings.
Relevance: a hit occurs if any of the top-K nearest neighbors shares
 the same app_package as the query screen.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from pgvector.psycopg import register_vector

from src.embedding_text import SBERT_MODEL_NAME, SBERT_MODEL_VERSION


@dataclass
class RecallResult:
    recall_at_k: float
    total: int
    hits: int
    skipped: int


def load_holdout_ids(path: str) -> list[int]:
    out: list[int] = []
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            out.append(int(line))
    return out


def _fetch_query_vector(conn, screen_id: int) -> list[float] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT vector
            FROM screens_embeddings
            WHERE screen_id = %s
              AND model_name = %s
              AND model_version = %s
              AND embedding_kind = 'text'
            """,
            (screen_id, SBERT_MODEL_NAME, SBERT_MODEL_VERSION),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _fetch_app_package(conn, screen_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT app_package FROM screens_metadata WHERE screen_id = %s",
            (screen_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _top_k_neighbors(
    conn,
    query_vector: list[float],
    k: int,
) -> list[tuple[int, str | None]]:
    register_vector(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT se.screen_id, sm.app_package
            FROM screens_embeddings se
            JOIN screens_metadata sm
              ON sm.screen_id = se.screen_id
            WHERE se.model_name = %s
              AND se.model_version = %s
              AND se.embedding_kind = 'text'
            ORDER BY se.vector <=> %s
            LIMIT %s
            """,
            (SBERT_MODEL_NAME, SBERT_MODEL_VERSION, query_vector, k),
        )
        return cur.fetchall()


def compute_recall_at_k(
    conn,
    holdout_ids: Iterable[int],
    k: int = 5,
) -> RecallResult:
    holdout_list = [int(sid) for sid in holdout_ids]
    if not holdout_list:
        return RecallResult(recall_at_k=0.0, total=0, hits=0, skipped=0)

    hits = 0
    skipped = 0

    for sid in holdout_list:
        query_vector = _fetch_query_vector(conn, sid)
        app_package = _fetch_app_package(conn, sid)

        if query_vector is None or app_package is None:
            skipped += 1
            continue

        neighbors = _top_k_neighbors(conn, query_vector, k + 1)
        filtered = [(nid, pkg) for nid, pkg in neighbors if int(nid) != int(sid)]
        top_k = filtered[:k]

        if any(pkg == app_package for _, pkg in top_k if pkg is not None):
            hits += 1

    total = len(holdout_list) - skipped
    recall_at_k = (hits / total) if total > 0 else 0.0

    return RecallResult(
        recall_at_k=recall_at_k,
        total=total,
        hits=hits,
        skipped=skipped,
    )
