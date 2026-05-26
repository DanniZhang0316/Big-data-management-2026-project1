import itertools
from io import BytesIO
from datasets import load_dataset

from src.fingerprint import sha256_bytes

DATASET_NAME = "rootsautomation/RICO-Screen2Words"


def stream_rico():
    return load_dataset(
        DATASET_NAME,
        split="train",
        streaming=True,
    )

def load_chosen_ids(path: str):
    with open(path) as f:
        return sorted({
            int(line)
            for line in f
            if line.strip() and not line.startswith("#")
        })


def collect_rows(ds, chosen_ids, limit=200):
    target = set(chosen_ids)
    raw = {}

    for row in itertools.islice(ds, limit):
        sid = int(row["screenId"])
        if sid in target:
            raw[sid] = row
            if len(raw) == len(target):
                break

    return raw


def ingest_screen(sid, row, s3, conn, run_id, bucket):
    png_key = f"screens/{sid}.png"
    json_key = f"screens/{sid}.json"

    # PNG (re-encode once so the bytes — and therefore the fingerprint — are deterministic)
    buf = BytesIO()
    row["image"].save(buf, format="PNG")
    png_bytes = buf.getvalue()

    source_fingerprint = sha256_bytes(png_bytes)

    s3.put_object(Bucket=bucket, Key=png_key, Body=png_bytes)

    # JSON
    s3.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=row["view_hierarchy"].encode("utf-8")
    )

    # POSTGRES (RUN-AWARE + IDEMPOTENT)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO screens_metadata
            (screen_id, app_package, category, png_path, hierarchy_json_path,
             run_id, source_fingerprint, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (screen_id)
            DO UPDATE SET
                png_path = EXCLUDED.png_path,
                hierarchy_json_path = EXCLUDED.hierarchy_json_path,
                run_id = EXCLUDED.run_id,
                source_fingerprint = EXCLUDED.source_fingerprint,
                updated_at = NOW()
            """,
            (
                sid,
                row["app_package_name"],
                row["category"],
                png_key,
                json_key,
                run_id,
                source_fingerprint,
            )
        )


def run_ingestion(ds, chosen_ids, s3, conn, run_id, bucket):
    raw = collect_rows(ds, chosen_ids)


    for sid in chosen_ids:
        ingest_screen(sid, raw[sid], s3, conn, run_id, bucket)