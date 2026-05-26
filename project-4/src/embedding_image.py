"""
CLIP image embedding — ported from the lab notebook (Section 3).

Loads `open_clip` ViT-B/32 (laion2b), batches the chosen screen PNGs from
MinIO, embeds them in one forward pass, L2-normalizes, and returns
per-screen vectors paired with the SHA-256 of the input PNG bytes.

The model is loaded inside the function (not at module-import time) so
Airflow workers that never run this task don't pay the import cost.
"""

from io import BytesIO
from typing import Sequence

from src.fingerprint import sha256_bytes


CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_NAME = "open-clip"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"


_clip_cache: dict = {}


def _load_clip():
    """Cache the model in-process so repeated calls in one Airflow task don't reload weights."""
    if "model" in _clip_cache:
        return _clip_cache["model"], _clip_cache["preprocess"]

    import open_clip

    model, _, preprocess = open_clip.create_model_and_transforms(
        CLIP_ARCH, pretrained=CLIP_PRETRAINED
    )
    model.eval()
    _clip_cache["model"] = model
    _clip_cache["preprocess"] = preprocess
    return model, preprocess


def embed_screens(s3, bucket: str, screen_ids: Sequence[int]) -> list[dict]:
    """
    Returns a list of {"screen_id", "vector", "fingerprint"} dicts —
    one per screen_id, in input order. Vectors are L2-normalized float32
    of length 512 (so cosine similarity == dot product).
    """
    if not screen_ids:
        return []

    import torch
    from PIL import Image

    model, preprocess = _load_clip()

    tensors = []
    fingerprints: list[str] = []
    for sid in screen_ids:
        blob = s3.get_object(Bucket=bucket, Key=f"screens/{sid}.png")["Body"].read()
        fingerprints.append(sha256_bytes(blob))
        img = Image.open(BytesIO(blob)).convert("RGB")
        tensors.append(preprocess(img))

    batch = torch.stack(tensors)
    with torch.no_grad():
        vecs = model.encode_image(batch)
        vecs = vecs / vecs.norm(dim=-1, keepdim=True)
    arr = vecs.cpu().numpy().astype("float32")

    return [
        {"screen_id": int(sid), "vector": arr[i], "fingerprint": fingerprints[i]}
        for i, sid in enumerate(screen_ids)
    ]
