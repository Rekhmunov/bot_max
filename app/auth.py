from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import struct
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict

from fastapi import Depends, HTTPException, Request, status
from itsdangerous import BadData, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import ServiceUser, UserSession, Workspace

_MANAGER_MINI_SALT = "manager-mini-app"
_MANAGER_INVITE_SALT = "manager-onboarding-link"
_PASSWORD_SCHEME = "pbkdf2_sha256"
_PASSWORD_ITERATIONS = 120_000
_SERVICE_SESSION_COOKIE = "tenant_session"


class ServiceSignInResult(TypedDict):
    status: Literal["ok", "invalid_credentials", "workspace_suspended", "workspace_inactive", "blocked"]
    user: ServiceUser | None
    workspace: Workspace | None


def validate_admin_credentials(username: str, password: str) -> bool:
    return secrets.compare_digest(username, settings.admin_username) and secrets.compare_digest(
        password,
        settings.admin_password,
    )


def sign_in_admin(request: Request, username: str, password: str) -> bool:
    if not validate_admin_credentials(username=username, password=password):
        return False
    request.session["is_admin"] = True
    request.session["admin_username"] = username
    return True


def get_current_admin(
    request: Request,
    db: Session = Depends(get_db),
) -> str | None:
    if request.session.get("is_admin"):
        return request.session.get("admin_username", settings.admin_username)
    # Backward-compatible access: allow SaaS superadmin to open legacy /admin routes
    # using the same tenant session from /app/login.
    service_user = get_current_service_user(request=request, db=db)
    if service_user is not None and service_user.role == "superadmin":
        return service_user.username
    return None


def require_admin(current_admin: str | None = Depends(get_current_admin)) -> str:
    if current_admin is None:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/admin/login"},
        )
    return current_admin


def _manager_mini_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=settings.secret_key, salt=_MANAGER_MINI_SALT)


def _manager_invite_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=settings.secret_key, salt=_MANAGER_INVITE_SALT)


def hash_password(password: str) -> str:
    raw = (password or "").encode("utf-8")
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", raw, salt, _PASSWORD_ITERATIONS, dklen=32)
    salt_b64 = base64.urlsafe_b64encode(salt).decode("ascii")
    digest_b64 = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"{_PASSWORD_SCHEME}${_PASSWORD_ITERATIONS}${salt_b64}${digest_b64}"


def verify_password(password: str, password_hash: str) -> bool:
    value = (password_hash or "").strip()
    if not value:
        return False
    parts = value.split("$", 3)
    if len(parts) != 4:
        return False
    scheme, iterations_raw, salt_b64, digest_b64 = parts
    if scheme != _PASSWORD_SCHEME:
        return False
    try:
        iterations = int(iterations_raw)
        salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
    except Exception:
        return False
    computed = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, iterations, dklen=32)
    return hmac.compare_digest(computed, expected)


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _sha256(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def create_service_session(
    db: Session,
    *,
    user_id: int,
    ip_address: str = "",
    user_agent: str = "",
    max_age_days: int = 30,
) -> str:
    raw_token = secrets.token_urlsafe(48)
    session_row = UserSession(
        user_id=user_id,
        session_token_hash=_sha256(raw_token),
        is_revoked=False,
        ip_address=(ip_address or "")[:128],
        user_agent=(user_agent or "")[:512],
        created_at=_now(),
        last_seen_at=_now(),
        expires_at=_now() + timedelta(days=max(max_age_days, 1)),
    )
    db.add(session_row)
    db.commit()
    return raw_token


def set_service_session_cookie(response: Any, raw_token: str) -> None:
    response.set_cookie(
        _SERVICE_SESSION_COOKIE,
        raw_token,
        max_age=60 * 60 * 24 * 30,
        httponly=True,
        samesite="lax",
        secure=bool(settings.secure_cookies),
        path="/",
    )


def clear_service_session_cookie(response: Any) -> None:
    response.delete_cookie(_SERVICE_SESSION_COOKIE, path="/")


def get_current_service_user(
    request: Request,
    db: Session = Depends(get_db),
) -> ServiceUser | None:
    token = str(request.cookies.get(_SERVICE_SESSION_COOKIE, "")).strip()
    if not token:
        return None
    token_hash = _sha256(token)
    session_row = (
        db.query(UserSession)
        .filter(
            UserSession.session_token_hash == token_hash,
            UserSession.is_revoked.is_(False),
            UserSession.expires_at > _now(),
        )
        .first()
    )
    if session_row is None:
        return None
    user = db.query(ServiceUser).filter(ServiceUser.id == session_row.user_id).first()
    if user is None:
        return None
    if not user.is_active or user.is_blocked:
        return None
    if user.workspace_id:
        workspace = db.query(Workspace).filter(Workspace.id == user.workspace_id).first()
        if workspace is None or not workspace.is_active or workspace.is_suspended:
            return None
    session_row.last_seen_at = _now()
    db.add(session_row)
    db.commit()
    return user


def require_service_user(current_user: ServiceUser | None = Depends(get_current_service_user)) -> ServiceUser:
    if current_user is None:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/"},
        )
    return current_user


def sign_in_service_user(
    db: Session,
    *,
    username: str,
    password: str,
) -> ServiceUser | None:
    result = sign_in_service_user_with_reason(db, username=username, password=password)
    return result["user"] if result["status"] == "ok" else None


def sign_in_service_user_with_reason(
    db: Session,
    *,
    username: str,
    password: str,
) -> ServiceSignInResult:
    normalized = (username or "").strip().lower()
    if not normalized:
        return {"status": "invalid_credentials", "user": None, "workspace": None}
    user = (
        db.query(ServiceUser)
        .filter(ServiceUser.username == normalized, ServiceUser.is_active.is_(True))
        .first()
    )
    if user is None or user.is_blocked:
        return {"status": "invalid_credentials", "user": None, "workspace": None}
    if not verify_password(password=password, password_hash=user.password_hash):
        return {"status": "invalid_credentials", "user": None, "workspace": None}
    if user.role != "superadmin" and user.workspace_id:
        workspace = db.query(Workspace).filter(Workspace.id == user.workspace_id).first()
        if workspace is None:
            return {"status": "workspace_inactive", "user": None, "workspace": None}
        if workspace.is_suspended:
            return {"status": "workspace_suspended", "user": None, "workspace": workspace}
        if not workspace.is_active:
            return {"status": "workspace_inactive", "user": None, "workspace": workspace}
    user.last_login_at = _now()
    db.add(user)
    db.commit()
    return {"status": "ok", "user": user, "workspace": None}


def revoke_user_sessions(db: Session, *, user_id: int) -> int:
    rows = (
        db.query(UserSession)
        .filter(UserSession.user_id == user_id, UserSession.is_revoked.is_(False))
        .all()
    )
    for row in rows:
        row.is_revoked = True
        row.revoked_at = _now()
        db.add(row)
    db.commit()
    return len(rows)


def revoke_workspace_sessions(db: Session, *, workspace_id: int) -> int:
    user_ids = [
        row[0]
        for row in db.query(ServiceUser.id)
        .filter(ServiceUser.workspace_id == workspace_id, ServiceUser.role != "superadmin")
        .all()
    ]
    if not user_ids:
        return 0
    rows = (
        db.query(UserSession)
        .filter(UserSession.user_id.in_(user_ids), UserSession.is_revoked.is_(False))
        .all()
    )
    for row in rows:
        row.is_revoked = True
        row.revoked_at = _now()
        db.add(row)
    db.commit()
    return len(rows)


def revoke_all_service_sessions(db: Session) -> int:
    rows = db.query(UserSession).filter(UserSession.is_revoked.is_(False)).all()
    for row in rows:
        row.is_revoked = True
        row.revoked_at = _now()
        db.add(row)
    db.commit()
    return len(rows)


def create_manager_mini_token(
    manager_account_id: str,
    *,
    workspace_id: int = 1,
    service_user_id: int | None = None,
) -> str:
    payload: dict[str, Any] = {
        "manager_id": manager_account_id.strip(),
        "workspace_id": int(workspace_id or 1),
    }
    if service_user_id is not None:
        payload["service_user_id"] = int(service_user_id)
    return _manager_mini_serializer().dumps(payload)


def verify_manager_mini_token(token: str, *, max_age_seconds: int = 60 * 60 * 24) -> str | None:
    claims = verify_manager_mini_claims(token=token, max_age_seconds=max_age_seconds)
    if not claims:
        return None
    manager_id = str(claims.get("manager_id", "")).strip()
    return manager_id or None


def verify_manager_mini_claims(token: str, *, max_age_seconds: int = 60 * 60 * 24) -> dict[str, Any] | None:
    if not token:
        return None
    try:
        payload = _manager_mini_serializer().loads(token, max_age=max_age_seconds)
    except (BadData, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    manager_id = str(payload.get("manager_id", "")).strip()
    if not manager_id:
        return None
    payload["manager_id"] = manager_id
    return payload


def create_manager_invite_token(
    *,
    invite_id: int,
    workspace_id: int,
    max_account_id: str,
    manager_user_id: int | None = None,
) -> str:
    payload: dict[str, Any] = {
        "invite_id": int(invite_id),
        "workspace_id": int(workspace_id),
        "max_account_id": (max_account_id or "").strip(),
    }
    if manager_user_id is not None:
        payload["manager_user_id"] = int(manager_user_id)
    return _manager_invite_serializer().dumps(payload)


def verify_manager_invite_token(token: str, *, max_age_seconds: int = 60 * 60 * 24 * 7) -> dict[str, Any] | None:
    if not token:
        return None
    try:
        payload = _manager_invite_serializer().loads(token, max_age=max_age_seconds)
    except (BadData, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    invite_id = payload.get("invite_id")
    workspace_id = payload.get("workspace_id")
    if not isinstance(invite_id, int) or not isinstance(workspace_id, int):
        return None
    result: dict[str, Any] = {
        "invite_id": invite_id,
        "workspace_id": workspace_id,
        "max_account_id": str(payload.get("max_account_id", "")).strip(),
    }
    manager_user_id = payload.get("manager_user_id")
    if isinstance(manager_user_id, int):
        result["manager_user_id"] = manager_user_id
    return result


def verify_totp_code(*, code: str, secret_b32: str, window: int = 1) -> bool:
    normalized_code = (code or "").strip().replace(" ", "")
    if len(normalized_code) != 6 or not normalized_code.isdigit():
        return False
    normalized_secret = (secret_b32 or "").strip().replace(" ", "").upper()
    if not normalized_secret:
        return False
    try:
        key = base64.b32decode(normalized_secret, casefold=True)
    except Exception:
        return False
    current_step = int(datetime.now(UTC).timestamp()) // 30
    for offset in range(-max(window, 0), max(window, 0) + 1):
        counter = current_step + offset
        msg = struct.pack(">Q", counter)
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        pos = digest[-1] & 0x0F
        chunk = digest[pos : pos + 4]
        value = struct.unpack(">I", chunk)[0] & 0x7FFFFFFF
        otp = str(value % 1_000_000).zfill(6)
        if secrets.compare_digest(otp, normalized_code):
            return True
    return False
