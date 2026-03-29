from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Request, status

from app.config import settings


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
