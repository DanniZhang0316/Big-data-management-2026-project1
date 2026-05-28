"""
Slack Notifications — incoming-webhook wrapper for the rico pipeline.

Environment
-----------
SLACK_WEBHOOK_URL   Incoming-webhook URL (set in .env, which is gitignored).
                    If absent all calls are no-ops with a logged warning —
                    a missing Slack config never crashes the pipeline.

Design rule
-----------
Every public function wraps its send call in a broad try/except.
A Slack outage, a misconfigured URL, or a transient network error must
never propagate upward and fail the DAG.  We log the error and move on.
Notifications are observability, not a pipeline dependency.

Public API
----------
notify_started(run_id, limit, trigger)
    Fire from init_run right after the pipeline_runs row is committed.
    trigger comes from dag_run.run_type ("manual", "scheduled", ...).

notify_failed_audit(run_id, failed_checks, log_url)
    Fire from the audit task BEFORE raising AirflowException.
    failed_checks is a list of plain dicts:
        [{"name": str, "details": dict}, ...]
    The message renders every duplicate key in full so an on-call engineer
    can assess severity from Slack alone without opening Airflow.

notify_finished(run_id, duration_s, summary_line, recall_at_5, n_meta, n_emb)
    Fire from the metrics task on successful completion.
    Includes wall-clock duration and the one-line data-quality summary.

notify_run_failed(run_id, failed_task_id, exception)
    Fire from the DAG-level on_failure_callback for non-audit failures.
"""

import json
import logging
import os
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

_WEBHOOK_URL: str | None = os.environ.get("SLACK_WEBHOOK_URL") or None


# ---------------------------------------------------------------------------
# Low-level send — never raises
# ---------------------------------------------------------------------------

def _send(payload: dict[str, Any]) -> None:
    """POST payload as JSON to the Slack webhook. Logs and returns on any error."""
    if not _WEBHOOK_URL:
        log.warning("[slack] SLACK_WEBHOOK_URL not configured — notification skipped")
        return

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(
                    "[slack] webhook returned HTTP %s — message may not have delivered",
                    resp.status,
                )
            else:
                log.debug("[slack] notification delivered (HTTP 200)")
    except Exception as exc:  # noqa: BLE001 — intentional broad catch
        log.warning("[slack] failed to deliver notification: %s", exc)


# ---------------------------------------------------------------------------
# 1. Run started
# ---------------------------------------------------------------------------

def notify_started(run_id: str, limit: int, trigger: str) -> None:
    """
    Pipeline initialised and about to begin processing.

    Parameters
    ----------
    run_id:   pipeline run UUID
    limit:    LIMIT param — number of screens this run will process
    trigger:  Airflow run type, e.g. "manual", "scheduled", "backfill"
    """
    _send({
        "text": (
            f":rocket:  *rico_pipeline started*\n"
            f">  `run_id`   {run_id}\n"
            f">  `LIMIT`    {limit} screens\n"
            f">  `trigger`  {trigger}"
        )
    })


# ---------------------------------------------------------------------------
# 2. Audit failed  (most important — must be scannable without opening Airflow)
# ---------------------------------------------------------------------------

def _fmt_embeddings_dup(details: dict) -> list[str]:
    """Render embeddings_dup_check duplicates, one bullet per offending key."""
    lines = []
    dups = details.get("duplicates", [])
    for d in dups[:20]:  # cap so the message stays readable
        lines.append(
            f"    • `screen_id={d['screen_id']}`  "
            f"`{d['model_name']}`  `v{d['model_version']}`  "
            f"`{d['embedding_kind']}`  →  *{d['count']} rows*"
        )
    if len(dups) > 20:
        lines.append(
            f"    … and {len(dups) - 20} more (see audit_results table)"
        )
    return lines


def _fmt_metadata_dup(details: dict) -> list[str]:
    """Render metadata_dup_check duplicates."""
    lines = []
    dups = details.get("duplicates", [])
    for d in dups[:20]:
        lines.append(
            f"    • `screen_id={d['screen_id']}`  →  *{d['count']} rows*"
        )
    if len(dups) > 20:
        lines.append(
            f"    … and {len(dups) - 20} more (see audit_results table)"
        )
    return lines


def _fmt_orphan(details: dict) -> list[str]:
    """Render orphan_embeddings_check screen_ids."""
    ids = details.get("orphan_screen_ids", [])
    preview = ", ".join(str(i) for i in ids[:10])
    suffix = f"  … and {len(ids) - 10} more" if len(ids) > 10 else ""
    return [f"    • orphan screen_ids: `{preview}`{suffix}"]


_CHECK_FORMATTERS = {
    "embeddings_dup_check":    _fmt_embeddings_dup,
    "metadata_dup_check":      _fmt_metadata_dup,
    "orphan_embeddings_check": _fmt_orphan,
}


def notify_failed_audit(
    run_id: str,
    failed_checks: list[dict],
    log_url: str,
) -> None:
    """
    Audit circuit-breaker tripped — pipeline is being halted.

    Call BEFORE raising AirflowException so the Slack message always fires
    even if the task is retried by Airflow.

    Parameters
    ----------
    run_id:        pipeline run UUID
    failed_checks: list of dicts with keys "name" (str) and "details" (dict).
                   Build it with::
                       [{"name": r.name, "details": r.details}
                        for r in results if not r.passed]
    log_url:       Airflow grid URL pointing at the audit task instance
    """
    n = len(failed_checks)
    plural = "check" if n == 1 else "checks"

    lines: list[str] = [
        ":rotating_light:  *Audit FAILED — pipeline halted*",
        f">  `run_id`   {run_id}",
        f">  `failed`   {n} {plural}",
        "",
    ]

    for check in failed_checks:
        name    = check.get("name", "unknown_check")
        details = check.get("details", {})
        dup_count = (
            details.get("duplicate_count")
            or details.get("orphan_count")
            or "?"
        )
        s = "" if dup_count == 1 else "s"
        lines.append(f"*{name}*  ({dup_count} violation{s})")
        formatter = _CHECK_FORMATTERS.get(name)
        if formatter:
            lines.extend(formatter(details))
        else:
            lines.append(f"    {details}")
        lines.append("")  # blank line between checks

    lines.append(f":mag_right:  <{log_url}|Open audit task log in Airflow>")

    _send({"text": "\n".join(lines)})


# ---------------------------------------------------------------------------
# 3. Run finished (successful completion)
# ---------------------------------------------------------------------------

def notify_finished(
    run_id: str,
    duration_s: float,
    summary_line: str,
    recall_at_5: float | None,
    n_meta: int | None,
    n_emb: int | None,
) -> None:
    """
    Pipeline completed successfully.
    Call from the metrics task after the summary has been logged.

    Parameters
    ----------
    run_id:       pipeline run UUID
    duration_s:   wall-clock seconds for the entire run
    summary_line: one-line string from format_summary(), e.g.
                  "[metrics] extraction_non_null_pct=0.980 ..."
    recall_at_5:  Recall@5 float, or None if eval was skipped
    n_meta:       row count in screens_metadata for this run
    n_emb:        row count in screens_embeddings for this run
    """
    recall_str = f"{recall_at_5:.3f}" if recall_at_5 is not None else "n/a"
    n_meta_str = str(n_meta) if n_meta is not None else "n/a"
    n_emb_str  = str(n_emb)  if n_emb  is not None else "n/a"

    mins, secs = divmod(int(duration_s), 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    _send({
        "text": (
            f":white_check_mark:  *rico_pipeline finished*  (succeeded)\n"
            f">  `run_id`      {run_id}\n"
            f">  `duration`    {duration_str}\n"
            f">  `recall@5`    {recall_str}\n"
            f">  `metadata`    {n_meta_str} rows   "
            f"`embeddings`  {n_emb_str} rows\n"
            f">  {summary_line}"
        )
    })


# ---------------------------------------------------------------------------
# 4. Run failed (non-audit failure — wired via DAG on_failure_callback)
# ---------------------------------------------------------------------------

def notify_run_failed(
    run_id: str,
    failed_task_id: str,
    exception: str,
) -> None:
    """
    A non-audit task failed, causing the pipeline to abort.
    Wired as the DAG-level on_failure_callback.

    Parameters
    ----------
    run_id:         pipeline run UUID (or "(run_id unavailable)" if XCom missed)
    failed_task_id: task_id of the task that failed
    exception:      str(context["exception"]) from the Airflow callback context
    """
    short_exc = exception[:300] + "…" if len(exception) > 300 else exception
    _send({
        "text": (
            f":x:  *rico_pipeline FAILED*\n"
            f">  `run_id`   {run_id}\n"
            f">  `task`     {failed_task_id}\n"
            f">  `error`    {short_exc}"
        )
    })
