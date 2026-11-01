from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
from typing import Optional
import traceback
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from app.config import settings


@dataclass
class StoredMedia:
    storage_provider: str
    storage_key: str
    public_url: str
    byte_size: int
    mime_type: str
    local_file_path: str | None = None


class MediaStorage(ABC):
    @abstractmethod
    def store_bytes(self, *, content: bytes, file_name: str, mime_type: str) -> StoredMedia:
        raise NotImplementedError

    @abstractmethod
    def resolve_local_file(self, *, storage_key: str) -> Path | None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, *, storage_key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def public_url_for_key(self, *, storage_key: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def exists(self, *, storage_key: str) -> bool:
        raise NotImplementedError


class LocalMediaStorage(MediaStorage):
    provider = "local"

    def __init__(self, *, uploads_dir: Path) -> None:
        self.uploads_dir = uploads_dir
        self.uploads_dir.mkdir(parents=True, exist_ok=True)

    def _safe_key_from_name(self, file_name: str) -> str:
        ext = Path(str(file_name or "")).suffix.lower()
        return f"uploads/{uuid4().hex}{ext}"

    def store_bytes(self, *, content: bytes, file_name: str, mime_type: str) -> StoredMedia:
        key = self._safe_key_from_name(file_name)
        target = Path("app/static") / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return StoredMedia(
            storage_provider=self.provider,
            storage_key=key,
            public_url=f"/static/{key}",
            byte_size=int(len(content)),
            mime_type=str(mime_type or "").strip(),
            local_file_path=str(target),
        )

    def resolve_local_file(self, *, storage_key: str) -> Path | None:
        key = str(storage_key or "").strip().removeprefix("/static/")
        if not key:
            return None
        path = Path("app/static") / key
        return path if path.exists() else None

    def delete(self, *, storage_key: str) -> bool:
        file_path = self.resolve_local_file(storage_key=storage_key)
        if file_path is None:
            return False
        try:
            file_path.unlink()
            return True
        except Exception:
            return False

    def public_url_for_key(self, *, storage_key: str) -> str:
        key = str(storage_key or "").strip().removeprefix("/static/")
        return f"/static/{key}" if key else ""

    def exists(self, *, storage_key: str) -> bool:
        return self.resolve_local_file(storage_key=storage_key) is not None


class S3CompatibleMediaStorage(MediaStorage):
    provider = "s3"

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str,
        public_base_url: str,
        prefix: str,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.bucket = bucket.strip()
        self.access_key = access_key.strip()
        self.secret_key = secret_key.strip()
        self.region = region.strip()
        self.public_base_url = public_base_url.rstrip("/")
        self.prefix = str(prefix or "uploads").strip().strip("/")

    def _safe_key_from_name(self, file_name: str) -> str:
        ext = Path(str(file_name or "")).suffix.lower()
        base = self.prefix or "uploads"
        return f"{base}/{uuid4().hex}{ext}"

    def _target_url(self, key: str) -> str:
        return f"{self.endpoint}/{self.bucket}/{key.lstrip('/')}"

    def store_bytes(self, *, content: bytes, file_name: str, mime_type: str) -> StoredMedia:
        # NOTE: this is intentionally minimal and dependency-free.
        # For production-grade signing/chunked uploads use boto3-compatible SDK.
        key = self._safe_key_from_name(file_name)
        target_url = self._target_url(key)
        headers = {"Content-Type": str(mime_type or "application/octet-stream")}
        auth = (self.access_key, self.secret_key) if self.access_key and self.secret_key else None
        with httpx.Client(timeout=60.0) as client:
            response = client.put(target_url, content=content, headers=headers, auth=auth)
            response.raise_for_status()
        public_url = (
            f"{self.public_base_url}/{key.lstrip('/')}"
            if self.public_base_url
            else target_url
        )
        return StoredMedia(
            storage_provider=self.provider,
            storage_key=key,
            public_url=public_url,
            byte_size=int(len(content)),
            mime_type=str(mime_type or "").strip(),
            local_file_path=None,
        )

    def resolve_local_file(self, *, storage_key: str) -> Path | None:
        return None

    def delete(self, *, storage_key: str) -> bool:
        key = str(storage_key or "").strip().lstrip("/")
        if not key:
            return False
        target_url = self._target_url(key)
        auth = (self.access_key, self.secret_key) if self.access_key and self.secret_key else None
        with httpx.Client(timeout=30.0) as client:
            response = client.delete(target_url, auth=auth)
        return response.status_code in {200, 202, 204, 404}

    def public_url_for_key(self, *, storage_key: str) -> str:
        key = str(storage_key or "").strip().lstrip("/")
        if not key:
            return ""
        return f"{self.public_base_url}/{key}" if self.public_base_url else self._target_url(key)

    def exists(self, *, storage_key: str) -> bool:
        key = str(storage_key or "").strip().lstrip("/")
        if not key:
            return False
        target_url = self._target_url(key)
        with httpx.Client(timeout=15.0) as client:
            response = client.head(target_url)
        return response.status_code in {200, 204}


_storage_instance: Optional[MediaStorage] = None


def get_media_storage() -> MediaStorage:
    global _storage_instance
    if _storage_instance is not None:
        return _storage_instance

    backend = str(getattr(settings, "media_storage_provider", "local") or "local").strip().lower()
    if (
        backend in {"s3", "r2", "minio"}
        and getattr(settings, "media_storage_endpoint", "")
        and getattr(settings, "media_storage_bucket", "")
    ):
        _storage_instance = S3CompatibleMediaStorage(
            endpoint=str(getattr(settings, "media_storage_endpoint", "")),
            bucket=str(getattr(settings, "media_storage_bucket", "")),
            access_key=str(getattr(settings, "media_storage_access_key", "") or ""),
            secret_key=str(getattr(settings, "media_storage_secret_key", "") or ""),
            region=str(getattr(settings, "media_storage_region", "") or "auto"),
            public_base_url=str(getattr(settings, "media_storage_public_base_url", "") or "").strip(),
            prefix=str(getattr(settings, "media_storage_prefix", "") or "uploads"),
        )
    else:
        _storage_instance = LocalMediaStorage(uploads_dir=Path("app/static/uploads"))
    return _storage_instance


def normalize_storage_public_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("/static/"):
        return raw
    base = str(settings.public_base_url or "").strip().rstrip("/")
    if base and raw.startswith(f"{base}/static/"):
        return raw[len(base):]
    return None


def upload_file_public_url(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = normalize_storage_public_url(raw)
    if normalized:
        return normalized
    return raw


def local_upload_abspath(value: str | None) -> Path | None:
    normalized = normalize_storage_public_url(value)
    if not normalized:
        return None
    relative = normalized.removeprefix("/static/")
    if not relative:
        return None
    path = Path("app/static") / relative
    return path


def iter_local_upload_files() -> list[Path]:
    root = Path("app/static/uploads")
    if not root.exists():
        return []
    return [path for path in root.iterdir() if path.is_file()]


def _extract_storage_key_from_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = normalize_storage_public_url(raw)
    if normalized:
        return normalized.removeprefix("/static/")

    configured_public_base = str(getattr(settings, "media_storage_public_base_url", "") or "").strip().rstrip("/")
    if configured_public_base and raw.startswith(f"{configured_public_base}/"):
        return raw[len(configured_public_base) + 1 :].lstrip("/")

    endpoint = str(getattr(settings, "media_storage_endpoint", "") or "").strip().rstrip("/")
    bucket = str(getattr(settings, "media_storage_bucket", "") or "").strip().strip("/")
    if endpoint and bucket and raw.startswith(f"{endpoint}/{bucket}/"):
        return raw[len(endpoint) + len(bucket) + 2 :].lstrip("/")

    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        path = parsed.path.lstrip("/")
        if bucket and path.startswith(f"{bucket}/"):
            return path[len(bucket) + 1 :]
        return path
    return raw


def delete_by_public_url(value: str | None) -> bool:
    storage = get_media_storage()
    raw = str(value or "").strip()
    if not raw:
        return False
    key = _extract_storage_key_from_url(raw)
    if not key:
        return False
    try:
        return bool(storage.delete(storage_key=key))
    except Exception:
        return False


def save_upload_bytes(*, file_name: str, content: bytes) -> str:
    ext = Path(str(file_name or "")).suffix.lower()
    mime_mapping = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
    }
    mime = mime_mapping.get(ext, "application/octet-stream")
    storage = get_media_storage()
    try:
        stored = storage.store_bytes(
            content=bytes(content),
            file_name=str(file_name or "file.bin"),
            mime_type=mime,
        )
    except Exception:
        allow_fallback = bool(getattr(settings, "media_storage_fallback_to_local_on_error", True))
        if not allow_fallback or isinstance(storage, LocalMediaStorage):
            raise
        stored = LocalMediaStorage(uploads_dir=Path("app/static/uploads")).store_bytes(
            content=bytes(content),
            file_name=str(file_name or "file.bin"),
            mime_type=mime,
        )
    return upload_file_public_url(stored.public_url)


def storage_public_url_for_key(*, storage_key: str) -> str:
    key = str(storage_key or "").strip().lstrip("/")
    if not key:
        return ""
    return upload_file_public_url(get_media_storage().public_url_for_key(storage_key=key))


def ensure_storage_ready() -> None:
    get_media_storage()


def storage_health_report(*, check_write: bool = True) -> dict[str, object]:
    configured_provider = str(getattr(settings, "media_storage_provider", "local") or "local").strip().lower()
    fallback_enabled = bool(getattr(settings, "media_storage_fallback_to_local_on_error", True))
    storage = get_media_storage()
    effective_provider = str(getattr(storage, "provider", "unknown") or "unknown").strip().lower()
    checked_at = datetime.now(UTC).isoformat()
    details: list[str] = []
    ok = True
    status = "ok"
    message = "Хранилище доступно."

    if configured_provider in {"s3", "r2", "minio"} and effective_provider == "local":
        status = "warning"
        message = "Внешнее хранилище не сконфигурировано, используется локальное."
        details.append("Проверьте endpoint/bucket и учетные данные S3-compatible.")

    if isinstance(storage, LocalMediaStorage):
        uploads_root = Path("app/static/uploads")
        try:
            uploads_root.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            ok = False
            status = "error"
            message = "Не удалось подготовить локальное хранилище."
            details.append(str(exc))
        if ok and not os.access(str(uploads_root), os.W_OK):
            ok = False
            status = "error"
            message = "Локальное хранилище недоступно для записи."
            details.append(f"Нет прав на запись: {uploads_root}")
        if ok and check_write:
            test_name = f"healthcheck-{uuid4().hex}.txt"
            test_path = uploads_root / test_name
            try:
                test_path.write_bytes(b"ok")
                test_path.unlink(missing_ok=True)
            except Exception as exc:
                ok = False
                status = "error"
                message = "Проверка записи в локальное хранилище не пройдена."
                details.append(str(exc))
        details.append(f"Папка uploads: {uploads_root}")
    elif isinstance(storage, S3CompatibleMediaStorage):
        endpoint = str(getattr(settings, "media_storage_endpoint", "") or "").strip()
        bucket = str(getattr(settings, "media_storage_bucket", "") or "").strip()
        if not endpoint or not bucket:
            ok = False
            status = "error"
            message = "S3 storage настроен неполностью."
            details.append("Не заполнены endpoint или bucket.")
        elif check_write:
            test_key = ""
            try:
                stored = storage.store_bytes(
                    content=b"ok",
                    file_name=f"healthcheck-{uuid4().hex}.txt",
                    mime_type="text/plain",
                )
                test_key = str(stored.storage_key or "").strip()
                if test_key:
                    storage.delete(storage_key=test_key)
            except Exception as exc:
                if fallback_enabled:
                    # Validate local fallback availability too.
                    try:
                        local = LocalMediaStorage(uploads_dir=Path("app/static/uploads"))
                        fallback_stored = local.store_bytes(
                            content=b"ok",
                            file_name=f"healthcheck-fallback-{uuid4().hex}.txt",
                            mime_type="text/plain",
                        )
                        local.delete(storage_key=fallback_stored.storage_key)
                        status = "warning"
                        message = "S3 недоступен, запись продолжит работать через local fallback."
                        details.append(f"S3 ошибка: {exc}")
                        ok = True
                    except Exception as local_exc:
                        ok = False
                        status = "error"
                        message = "S3 недоступен и fallback local тоже не работает."
                        details.append(f"S3 ошибка: {exc}")
                        details.append(f"Local fallback ошибка: {local_exc}")
                else:
                    ok = False
                    status = "error"
                    message = "S3 недоступен для записи."
                    details.append(str(exc))
            finally:
                if test_key:
                    try:
                        storage.delete(storage_key=test_key)
                    except Exception:
                        pass
        details.append(f"S3 endpoint: {endpoint or '—'}")
        details.append(f"S3 bucket: {bucket or '—'}")
    else:
        ok = False
        status = "error"
        message = "Неизвестный storage provider."
        details.append(f"provider={effective_provider}")

    if not ok and status != "error":
        status = "error"
    if status == "error" and not message:
        message = "Проверка storage завершилась ошибкой."
    if not details and status == "ok":
        details.append("Ошибок не обнаружено.")

    return {
        "ok": bool(ok),
        "status": status,
        "message": message,
        "configured_provider": configured_provider,
        "effective_provider": effective_provider,
        "fallback_enabled": fallback_enabled,
        "write_check_performed": bool(check_write),
        "checked_at": checked_at,
        "details": details,
    }


def get_storage_health(*, check_write: bool = True) -> dict[str, object]:
    report = storage_health_report(check_write=check_write)
    status = str(report.get("status") or "").strip().lower()
    summary_map = {
        "ok": "OK",
        "warning": "Требуется внимание",
        "error": "Ошибка",
    }
    details = report.get("details")
    error_text = ""
    if isinstance(details, list):
        for item in details:
            text = str(item or "").strip()
            if "ошиб" in text.lower() or "error" in text.lower():
                error_text = text
                break
    if not error_text and status == "error":
        error_text = str(report.get("message") or "").strip()
    return {
        "ok": bool(report.get("ok", False)),
        "status": status or ("ok" if bool(report.get("ok", False)) else "error"),
        "summary": summary_map.get(status or "", "OK" if bool(report.get("ok", False)) else "Ошибка"),
        "provider": str(report.get("effective_provider") or "local"),
        "mode": (
            "fallback-local"
            if str(report.get("configured_provider") or "").strip().lower() in {"s3", "r2", "minio"}
            and str(report.get("effective_provider") or "").strip().lower() == "local"
            else "primary"
        ),
        "error": error_text,
        "checked_at": str(report.get("checked_at") or ""),
    }


def get_storage_health_snapshot() -> dict[str, object]:
    try:
        return get_storage_health(check_write=False)
    except Exception:
        return {
            "ok": False,
            "status": "error",
            "summary": "Ошибка",
            "provider": "unknown",
            "mode": "primary",
            "error": "Не удалось получить статус storage.",
            "checked_at": datetime.now(UTC).isoformat(),
        }

