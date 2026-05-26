"""
SBERT text embedding — ported from the lab notebook (Section 4).

Fetches each screen's view-hierarchy JSON from MinIO, runs it through
Engineer A's parsing.parse_hierarchy / text_representation pipeline,
batch-encodes with sentence-transformers/all-MiniLM-L6-v2, L2-normalizes,
and returns per-screen 384-d vectors paired with the SHA-256 of the
*text input* that fed the embedder.
"""

from typing import Sequence

from src.fingerprint import sha256_text
from src.parsing import parse_hierarchy, text_representation


SBERT_MODEL_NAME = "sentence-transformers"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"


_sbert_cache: dict = {}


def _load_sbert():
    if "model" in _sbert_cache:
        return _sbert_cache["model"]

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(SBERT_MODEL_VERSION)
    _sbert_cache["model"] = model
    return model


def build_text_reps(s3, bucket: str, screen_ids: Sequence[int]) -> dict[int, str]:
    """Fetch hierarchy JSON from MinIO and produce flattened text per screen."""
    reps: dict[int, str] = {}
    for sid in screen_ids:
        raw = (
            s3.get_object(Bucket=bucket, Key=f"screens/{sid}.json")["Body"]
            .read()
            .decode("utf-8")
        )
        reps[int(sid)] = text_representation(parse_hierarchy(raw))
    return reps


def embed_screens(s3, bucket: str, screen_ids: Sequence[int]) -> list[dict]:
    """
    Returns a list of {"screen_id", "vector", "fingerprint", "text"} dicts.
    `text` is exposed so the extract task can reuse it (one parse, two consumers).
    """
    if not screen_ids:
        return []

    sbert = _load_sbert()
    reps = build_text_reps(s3, bucket, screen_ids)

    corpus = [reps[int(sid)] for sid in screen_ids]
    vectors = sbert.encode(corpus, normalize_embeddings=True).astype("float32")

    return [
        {
            "screen_id": int(sid),
            "vector": vectors[i],
            "fingerprint": sha256_text(reps[int(sid)]),
            "text": reps[int(sid)],
        }
        for i, sid in enumerate(screen_ids)
    ]
