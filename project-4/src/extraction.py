"""
LLM extraction — ported from the lab notebook (Section 5).

Calls Ollama for each screen with a versioned prompt asking for
{title, elements, confidence}. Unlike the notebook (which crashes on
invalid JSON), this module catches parse errors and emits a structured
"failed" result for the load stage to route into screens_review_queue.
"""

import json
import os
from typing import Sequence

import requests

from src.embedding_text import build_text_reps
from src.fingerprint import sha256_text


OLLAMA_URL = os.environ.get("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

PROMPT_VERSION = "v1"
PROMPT_V1 = """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings",
  "Search results"). Empty string if unclear.
- "elements": a list of {"type": string, "text": string} objects, one
  per salient interactive or informational element you can identify.
- "confidence": a number in [0.0, 1.0] expressing how confident you are
  in the extraction.

Visible text:
{hierarchy_text}

Respond with valid JSON only — no commentary, no Markdown fences.
"""


def _extract_one(text_rep: str) -> dict:
    """One Ollama call → raw response text + parsed dict (or error)."""
    prompt = PROMPT_V1.replace("{hierarchy_text}", text_rep)
    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=180,
    )
    response.raise_for_status()
    raw = response.json()["response"]

    out = {"raw_response": raw, "ok": False, "payload": None, "error": None, "confidence": None}
    try:
        payload = json.loads(raw)
        out["payload"] = payload
        out["ok"] = True
        out["confidence"] = float(payload.get("confidence", 0.0))
    except json.JSONDecodeError as exc:
        out["error"] = f"invalid_json:{exc.msg}"
    except (TypeError, ValueError) as exc:
        out["error"] = f"bad_confidence:{exc}"
    return out


def extract_screens(s3, bucket: str, screen_ids: Sequence[int]) -> list[dict]:
    """
    Returns a list of {"screen_id", "ok", "payload", "confidence",
    "raw_response", "fingerprint", "error"} dicts.

    The fingerprint is SHA-256 of the text input that fed the LLM —
    matches embedding_text's fingerprint for the same screen, so
    "did the LLM see exactly this byte sequence?" is answerable in SQL.
    """
    if not screen_ids:
        return []

    reps = build_text_reps(s3, bucket, screen_ids)

    out: list[dict] = []
    for sid in screen_ids:
        text = reps[int(sid)]
        fingerprint = sha256_text(text)
        try:
            result = _extract_one(text)
        except requests.RequestException as exc:
            result = {
                "raw_response": None,
                "ok": False,
                "payload": None,
                "confidence": None,
                "error": f"ollama_error:{exc}",
            }
        result["screen_id"] = int(sid)
        result["fingerprint"] = fingerprint
        out.append(result)

    return out
