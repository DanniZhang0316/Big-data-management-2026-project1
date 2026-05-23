import uuid
import time
import subprocess

from dataclasses import dataclass
from datetime import datetime

from src.clients import get_postgres


@dataclass
class RunContext:
    run_id: str
    git_sha: str
    limit: int
    started_at: float


def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"]
        ).decode().strip()
    except Exception:
        return "unknown"


def create_run(limit: int) -> RunContext:
    run = RunContext(
        run_id=str(uuid.uuid4()),
        git_sha=get_git_sha(),
        limit=limit,
        started_at=time.time()
    )

    conn = get_postgres()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
            (run_id, dag_id, git_sha, started_at, status, limit_param)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run.run_id,
                "rico_pipeline",
                run.git_sha,
                datetime.utcnow(),
                "RUNNING",
                run.limit
            )
        )

    conn.commit()
    conn.close()

    return run