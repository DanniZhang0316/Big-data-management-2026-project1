"""
Pipeline metrics collection (Engineer D).

Stores metrics in pipeline_metrics with labels to disambiguate per-task values.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable


INSERT_METRIC_SQL = """
INSERT INTO pipeline_metrics
    (run_id, metric_name, metric_value, metric_text, labels)
VALUES (%s, %s, %s, %s, %s::jsonb)
ON CONFLICT (run_id, metric_name, labels)
DO UPDATE SET
    metric_value = EXCLUDED.metric_value,
    metric_text = EXCLUDED.metric_text,
    created_at = NOW()
"""


@dataclass
class Metric:
    name: str
    value: float | None = None
    text: str | None = None
    labels: dict[str, Any] | None = None


def upsert_metrics(conn, run_id: str, metrics: Iterable[Metric]) -> None:
    with conn.cursor() as cur:
        for metric in metrics:
            labels = metric.labels or {}
            cur.execute(
                INSERT_METRIC_SQL,
                (
                    run_id,
                    metric.name,
                    metric.value,
                    metric.text,
                    json.dumps(labels),
                ),
            )


def collect_task_metrics(context: dict[str, Any]) -> list[Metric]:
    metrics: list[Metric] = []
    dag_run = context.get("dag_run")

    if dag_run is not None:
        for ti in dag_run.get_task_instances():
            duration = None
            if ti.start_date and ti.end_date:
                duration = (ti.end_date - ti.start_date).total_seconds()
            retries = max(int(ti.try_number or 1) - 1, 0)

            metrics.append(
                Metric(
                    name="task_duration_seconds",
                    value=duration,
                    labels={"task_id": ti.task_id},
                )
            )
            metrics.append(
                Metric(
                    name="task_retries",
                    value=float(retries),
                    labels={"task_id": ti.task_id},
                )
            )

    xcom_task_keys = [
        ("embed_image", "rows_in"),
        ("embed_image", "rows_inserted"),
        ("embed_text", "rows_in"),
        ("embed_text", "rows_inserted"),
        ("extract", "rows_in"),
        ("extract", "rows_updated"),
        ("extract", "review_queue_rows"),
        ("load", "screens_metadata_rows"),
        ("load", "screens_embeddings_rows"),
        ("load", "screens_review_queue_rows"),
    ]

    ti = context.get("ti")
    if ti is not None:
        for task_id, key in xcom_task_keys:
            value = ti.xcom_pull(task_ids=task_id, key=key)
            if value is None:
                continue
            metrics.append(
                Metric(
                    name="rows",
                    value=float(value),
                    labels={"task_id": task_id, "kind": key},
                )
            )

    return metrics


def collect_data_quality_metrics(conn, run_id: str) -> list[Metric]:
    metrics: list[Metric] = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL) AS extracted,
                COUNT(*) FILTER (
                    WHERE confidence IS NOT NULL
                      AND confidence >= 0.0
                      AND confidence <= 1.0
                ) AS confidence_in_bounds,
                COUNT(DISTINCT app_package) AS distinct_apps,
                COUNT(DISTINCT category) AS distinct_categories
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        total, extracted, conf_in_bounds, distinct_apps, distinct_categories = cur.fetchone()

        total = total or 0
        extracted = extracted or 0
        conf_in_bounds = conf_in_bounds or 0

        extraction_pct = (extracted / total) if total else 0.0
        conf_bounds_pct = (conf_in_bounds / total) if total else 0.0

        metrics.extend(
            [
                Metric(name="extraction_non_null_pct", value=extraction_pct),
                Metric(name="confidence_in_bounds_pct", value=conf_bounds_pct),
                Metric(name="distinct_app_packages", value=float(distinct_apps or 0)),
                Metric(name="distinct_categories", value=float(distinct_categories or 0)),
            ]
        )

        cur.execute(
            """
            SELECT
                AVG(SQRT(-(vector <#> vector))) AS avg_norm,
                SUM(
                    CASE
                        WHEN SQRT(-(vector <#> vector)) < 0.9
                          OR SQRT(-(vector <#> vector)) > 1.1
                        THEN 1 ELSE 0
                    END
                )::float / NULLIF(COUNT(*), 0) AS outlier_pct
            FROM screens_embeddings
            WHERE run_id = %s
            """,
            (run_id,),
        )
        avg_norm, outlier_pct = cur.fetchone()

        if avg_norm is not None:
            metrics.append(Metric(name="embedding_avg_norm", value=float(avg_norm)))
        if outlier_pct is not None:
            metrics.append(Metric(name="embedding_norm_outlier_pct", value=float(outlier_pct)))

    return metrics


def format_summary(metrics: Iterable[Metric]) -> str:
    lookup = {m.name: m.value for m in metrics if m.value is not None}
    extraction_pct = lookup.get("extraction_non_null_pct")
    conf_bounds = lookup.get("confidence_in_bounds_pct")
    avg_norm = lookup.get("embedding_avg_norm")
    outliers = lookup.get("embedding_norm_outlier_pct")

    parts = ["[metrics]"]
    if extraction_pct is not None:
        parts.append(f"extraction_non_null_pct={extraction_pct:.3f}")
    if conf_bounds is not None:
        parts.append(f"confidence_in_bounds_pct={conf_bounds:.3f}")
    if avg_norm is not None:
        parts.append(f"embedding_avg_norm={avg_norm:.3f}")
    if outliers is not None:
        parts.append(f"embedding_norm_outlier_pct={outliers:.3f}")

    return " ".join(parts)
