from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Request, status
from itsdangerous import BadData, SignatureExpired, URLSafeTimedSerializer

from app.config import settings

_MANAGER_MINI_SALT = "manager-mini-app"


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


def get_current_admin(request: Request) -> str | None:
    if request.session.get("is_admin"):
        return request.session.get("admin_username", settings.admin_username)
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


def create_manager_mini_token(manager_account_id: str) -> str:
    return _manager_mini_serializer().dumps({"manager_id": manager_account_id.strip()})


def verify_manager_mini_token(token: str, *, max_age_seconds: int = 60 * 60 * 24) -> str | None:
    if not token:
        return None
    try:
        payload = _manager_mini_serializer().loads(token, max_age=max_age_seconds)
    except (BadData, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    manager_id = str(payload.get("manager_id", "")).strip()
    return manager_id or None
