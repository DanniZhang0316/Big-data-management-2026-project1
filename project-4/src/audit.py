"""
Audit module — duplicate-detection circuit-breaker for the rico pipeline.

This module owns the SQL and persistence for audit checks.  It does NOT
raise — raising AirflowException is the DAG task's responsibility so that
the task can pass structured failure data to Slack before halting.

Checks performed
----------------
1. embeddings_dup_check  (required by spec)
   No (screen_id, model_name, model_version, embedding_kind) combination
   may appear more than once in screens_embeddings for the current run.
   The PRIMARY KEY already prevents this at the DB level; this check is a
   belt-and-suspenders witness that ON CONFLICT DO NOTHING did its job.

2. metadata_dup_check  (required by spec)
   No screen_id may appear more than once in screens_metadata for the
   current run.

3. orphan_embeddings_check  (bonus)
   No embedding row for the current run may reference a screen_id absent
   from screens_metadata.  Guards against partial-ingest anomalies.

Persistence (bonus)
-------------------
Every result — pass or fail — is written to audit_results before the
function returns, so audit history is always queryable.

Usage
-----
    results = run_audit(conn, run_id)        # returns list[AuditResult]
    failed  = [r for r in results if not r.passed]
    if failed:
        # caller handles Slack notification and AirflowException
        ...
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL — parameterised with %(run_id)s (psycopg dict-style)
# ---------------------------------------------------------------------------

# Required check 1 — duplicate composite key in screens_embeddings
_EMBEDDINGS_DUP_SQL = """
SELECT
    screen_id,
    model_name,
    model_version,
    embedding_kind,
    COUNT(*) AS cnt
FROM   screens_embeddings
WHERE  run_id = %(run_id)s
GROUP  BY screen_id, model_name, model_version, embedding_kind
HAVING COUNT(*) > 1
ORDER  BY cnt DESC, screen_id
"""

# Required check 2 — duplicate screen_id in screens_metadata
_METADATA_DUP_SQL = """
SELECT
    screen_id,
    COUNT(*) AS cnt
FROM   screens_metadata
WHERE  run_id = %(run_id)s
GROUP  BY screen_id
HAVING COUNT(*) > 1
ORDER  BY cnt DESC, screen_id
"""

# Bonus check 3 — embeddings with no corresponding metadata row
_ORPHAN_EMBEDDINGS_SQL = """
SELECT DISTINCT se.screen_id
FROM   screens_embeddings se
WHERE  se.run_id = %(run_id)s
  AND  NOT EXISTS (
           SELECT 1
           FROM   screens_metadata sm
           WHERE  sm.screen_id = se.screen_id
       )
ORDER  BY se.screen_id
"""

# Bonus persistence
_INSERT_AUDIT_RESULT_SQL = """
INSERT INTO audit_results (run_id, audit_name, passed, details)
VALUES (%(run_id)s, %(audit_name)s, %(passed)s, %(details)s::jsonb)
"""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AuditResult:
    """Outcome of a single audit check."""
    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_embeddings_duplicates(cur, run_id: str) -> AuditResult:
    """Required check 1 — duplicate (screen_id, model_name, model_version, embedding_kind)."""
    cur.execute(_EMBEDDINGS_DUP_SQL, {"run_id": run_id})
    rows = cur.fetchall()
    if rows:
        duplicates = [
            {
                "screen_id":      r[0],
                "model_name":     r[1],
                "model_version":  r[2],
                "embedding_kind": r[3],
                "count":          r[4],
            }
            for r in rows
        ]
        return AuditResult(
            name="embeddings_dup_check",
            passed=False,
            details={"duplicate_count": len(duplicates), "duplicates": duplicates},
        )
    return AuditResult(
        name="embeddings_dup_check",
        passed=True,
        details={"duplicate_count": 0},
    )


def _check_metadata_duplicates(cur, run_id: str) -> AuditResult:
    """Required check 2 — duplicate screen_id in screens_metadata."""
    cur.execute(_METADATA_DUP_SQL, {"run_id": run_id})
    rows = cur.fetchall()
    if rows:
        duplicates = [{"screen_id": r[0], "count": r[1]} for r in rows]
        return AuditResult(
            name="metadata_dup_check",
            passed=False,
            details={"duplicate_count": len(duplicates), "duplicates": duplicates},
        )
    return AuditResult(
        name="metadata_dup_check",
        passed=True,
        details={"duplicate_count": 0},
    )


def _check_orphan_embeddings(cur, run_id: str) -> AuditResult:
    """Bonus check 3 — embedding rows with no corresponding metadata row."""
    cur.execute(_ORPHAN_EMBEDDINGS_SQL, {"run_id": run_id})
    rows = cur.fetchall()
    if rows:
        orphan_ids = [r[0] for r in rows]
        return AuditResult(
            name="orphan_embeddings_check",
            passed=False,
            details={
                "orphan_count":      len(orphan_ids),
                "orphan_screen_ids": orphan_ids,
            },
        )
    return AuditResult(
        name="orphan_embeddings_check",
        passed=True,
        details={"orphan_count": 0},
    )


# ---------------------------------------------------------------------------
# Persistence helper
# ---------------------------------------------------------------------------

def _save_audit_results(cur, run_id: str, results: list[AuditResult]) -> None:
    """Write every result to audit_results (fresh rows per run, no conflict clause)."""
    for r in results:
        cur.execute(
            _INSERT_AUDIT_RESULT_SQL,
            {
                "run_id":     run_id,
                "audit_name": r.name,
                "passed":     r.passed,
                "details":    json.dumps(r.details),
            },
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_audit(conn, run_id: str) -> list[AuditResult]:
    """
    Execute all audit checks for *run_id*.

    All results (pass and fail) are written to audit_results and committed
    before returning, so the audit history is queryable regardless of what
    the caller does next.

    Returns the complete list of AuditResult objects.
    The caller is responsible for inspecting .passed, formatting Slack
    messages, and raising AirflowException if appropriate.
    """
    with conn.cursor() as cur:
        results = [
            _check_embeddings_duplicates(cur, run_id),
            _check_metadata_duplicates(cur, run_id),
            _check_orphan_embeddings(cur, run_id),
        ]
        _save_audit_results(cur, run_id, results)

    conn.commit()

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        log.info("[audit] %-30s %s  details=%s", r.name, status, r.details)

    return results
