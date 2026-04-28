from __future__ import annotations

import hashlib
import hmac
import io
import threading
import time
import zipfile
from collections import deque
from dataclasses import dataclass
from typing import Deque
from urllib.parse import urlparse


def verify_hmac_signature(*, body: bytes, secret: str, provided_signature: str) -> bool:
    normalized_secret = (secret or "").strip()
    normalized_sig = (provided_signature or "").strip()
    if not normalized_secret or not normalized_sig:
        return False
    if normalized_sig.startswith("sha256="):
        normalized_sig = normalized_sig[len("sha256=") :]
    expected = hmac.new(
        normalized_secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, normalized_sig)


def is_same_origin(*, origin: str, host_url: str) -> bool:
    if not origin or not host_url:
        return False
    try:
        o = urlparse(origin)
        h = urlparse(host_url)
    except Exception:
        return False
    return (o.scheme.lower(), o.netloc.lower()) == (h.scheme.lower(), h.netloc.lower())


@dataclass
class RateLimitBucket:
    hits: Deque[float]
    limit: int
    window_seconds: int

    def allow(self) -> bool:
        now = time.monotonic()
        border = now - self.window_seconds
        while self.hits and self.hits[0] < border:
            self.hits.popleft()
        if len(self.hits) >= self.limit:
            return False
        self.hits.append(now)
        return True


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[str, RateLimitBucket] = {}

    def allow(self, *, key: str, limit: int, window_seconds: int) -> bool:
        normalized_key = f"{key}:{limit}:{window_seconds}"
        with self._lock:
            bucket = self._buckets.get(normalized_key)
            if bucket is None:
                bucket = RateLimitBucket(hits=deque(), limit=limit, window_seconds=window_seconds)
                self._buckets[normalized_key] = bucket
            return bucket.allow()


def extract_upload_image_bytes(content: bytes) -> tuple[bool, str]:
    if not content:
        return False, ""
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return True, ".png"
    if content.startswith(b"\xff\xd8"):
        return True, ".jpg"
    if content.startswith(b"GIF87a") or content.startswith(b"GIF89a"):
        return True, ".gif"
    if content.startswith(b"RIFF") and b"WEBP" in content[:16]:
        return True, ".webp"
    return False, ""


def is_safe_image(*, filename: str, content: bytes, max_bytes: int) -> tuple[bool, str]:
    normalized = (filename or "").strip().lower()
    if not normalized:
        return False, "empty_filename"
    allowed_ext = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
    if "." not in normalized:
        return False, "missing_extension"
    ext = "." + normalized.rsplit(".", 1)[-1]
    if ext not in allowed_ext:
        return False, "unsupported_extension"
    if len(content) <= 0:
        return False, "empty_file"
    if len(content) > int(max_bytes):
        return False, "file_too_large"
    is_valid, detected_ext = extract_upload_image_bytes(content)
    if not is_valid:
        return False, "invalid_signature"
    if ext == ".jpeg":
        ext = ".jpg"
    if detected_ext != ext:
        return False, "extension_mismatch"
    return True, ""


def is_safe_document(*, filename: str, content: bytes, max_bytes: int) -> tuple[bool, str]:
    normalized = (filename or "").strip().lower()
    if not normalized:
        return False, "empty_filename"
    if "." not in normalized:
        return False, "missing_extension"
    ext = "." + normalized.rsplit(".", 1)[-1]
    allowed_ext = {".pdf", ".txt", ".csv", ".docx", ".xlsx", ".pptx"}
    if ext not in allowed_ext:
        return False, "unsupported_extension"
    if len(content) <= 0:
        return False, "empty_file"
    if len(content) > int(max_bytes):
        return False, "file_too_large"

    if ext == ".pdf":
        if not content.startswith(b"%PDF-"):
            return False, "invalid_signature"
        return True, ""

    if ext in {".txt", ".csv"}:
        if b"\x00" in content:
            return False, "binary_detected"
        return True, ""

    # OpenXML documents are zip containers with known directory markers.
    if not content.startswith(b"PK"):
        return False, "invalid_signature"
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = {str(name or "").strip() for name in archive.namelist()}
    except Exception:
        return False, "invalid_zip"
    if "[Content_Types].xml" not in names:
        return False, "invalid_openxml"
    if ext == ".docx" and not any(name.startswith("word/") for name in names):
        return False, "invalid_openxml"
    if ext == ".xlsx" and not any(name.startswith("xl/") for name in names):
        return False, "invalid_openxml"
    if ext == ".pptx" and not any(name.startswith("ppt/") for name in names):
        return False, "invalid_openxml"
    return True, ""


def safe_json_dumps(payload: dict) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
