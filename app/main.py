from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import string
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy import func
from sqlalchemy.orm import Session
from itsdangerous import URLSafeTimedSerializer, BadData, SignatureExpired

from app.auth import (
    clear_service_session_cookie,
    create_manager_mini_token,
    create_manager_invite_token,
    create_service_session,
    hash_password,
    verify_password,
    verify_totp_code,
    get_current_admin,
    get_current_service_user,
    require_admin,
    require_service_user,
    revoke_all_service_sessions,
    revoke_user_sessions,
    revoke_workspace_sessions,
    set_service_session_cookie,
    sign_in_admin,
    sign_in_service_user,
    verify_manager_invite_token,
    verify_manager_mini_claims,
    verify_manager_mini_token,
)
from app.config import settings
from app.database import get_db, init_db
from app.manager_bridge import (
    TEMPLATE_AFTER_PHONE,
    TEMPLATE_PRESTART,
    TEMPLATE_START,
    assign_conversation_to_folder,
    create_chat_folder,
    delete_conversation,
    ensure_default_templates,
    get_chat_metrics,
    get_delivery_metrics,
    get_template_text,
    list_active_quick_replies,
    list_chat_folders,
    load_chat_messages,
    load_chat_threads,
    mark_thread_unread,
    mark_thread_read,
    handle_customer_event,
    handle_manager_message,
    process_outbox_queue,
    remove_chat_message,
    retry_failed_outbox_message,
    set_template_text,
    send_admin_chat_message,
    send_admin_quick_reply,
    update_chat_message_text,
)
from app.max_client import MaxClient
from app.email_utils import send_email_verification
from app.models import (
    AuditLog,
    BillingEvent,
    BotSettings,
    ChatMessage,
    ChatFolder,
    Conversation,
    ConversationMeta,
    CustomerProfile,
    IntroStep,
    ManagerInvite,
    ManagerDispatch,
    MessageLog,
    MessageTemplate,
    OutboxMessage,
    QuickReply,
    ServiceUser,
    Subscription,
    TenantAlert,
    UserSession,
    WebhookEvent,
    Workspace,
)
from app.ops import (
    apply_billing_hook,
    can_add_manager,
    collect_tenant_metrics,
    create_sqlite_backup,
    ensure_workspace_active_by_billing,
    ensure_workspace_limits_and_state,
    get_or_create_subscription,
    list_backups,
    refresh_tenant_alerts,
    restore_sqlite_backup,
)
from app.security import InMemoryRateLimiter, is_safe_image, is_same_origin, verify_hmac_signature, safe_json_dumps
from app.schemas import MaxWebhookEvent
from app.services import (
    DEFAULT_WORKSPACE_ID,
    create_service_user,
    create_workspace_with_owner,
    ensure_default_workspace,
    get_or_create_settings,
    get_workspace_by_tenant_code,
    list_workspace_managers,
)
from fastapi.templating import Jinja2Templates
from app.database import SessionLocal


_SUPERADMIN_TABS = (
    "dashboard",
    "workspaces",
    "users",
    "plans",
    "security",
    "monitoring",
    "backups",
    "audit",
    "system",
)


def _require_superadmin(user: ServiceUser) -> None:
    if user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для суперадмина")


def _to_iso(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value: int | str | None, default: int, min_value: int = 0) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(min_value, parsed)


def _superadmin_dashboard_snapshot(db: Session) -> dict:
    workspaces = db.query(Workspace).order_by(Workspace.id.asc()).all()
    users = db.query(ServiceUser).order_by(ServiceUser.id.asc()).all()
    sessions = db.query(UserSession).filter(UserSession.is_revoked.is_(False)).count()
    alerts_total = db.query(TenantAlert).filter(TenantAlert.is_resolved.is_(False)).count()
    suspended = sum(1 for ws in workspaces if ws.is_suspended)
    inactive = sum(1 for ws in workspaces if not ws.is_active)
    roles = {
        "superadmin": sum(1 for u in users if u.role == "superadmin"),
        "admin": sum(1 for u in users if u.role == "admin"),
        "manager": sum(1 for u in users if u.role == "manager"),
    }
    return {
        "workspaces_total": len(workspaces),
        "workspaces_active": len(workspaces) - suspended - inactive,
        "workspaces_suspended": suspended,
        "workspaces_inactive": inactive,
        "users_total": len(users),
        "sessions_active": sessions,
        "alerts_active": alerts_total,
        "alerts_open": alerts_total,
        "roles": roles,
    }


def _build_superadmin_context(
    *,
    request: Request,
    current_user: ServiceUser,
    db: Session,
    tab: str,
    message: str | None = None,
    error: str | None = None,
) -> dict:
    active_tab = tab if tab in _SUPERADMIN_TABS else "dashboard"
    dashboard = _superadmin_dashboard_snapshot(db)
    workspaces = db.query(Workspace).order_by(Workspace.id.asc()).all()
    users = db.query(ServiceUser).order_by(ServiceUser.id.asc()).all()
    subs = {
        row.workspace_id: row
        for row in db.query(Subscription).order_by(Subscription.id.asc()).all()
    }
    latest_alerts = (
        db.query(TenantAlert)
        .order_by(TenantAlert.id.desc())
        .limit(200)
        .all()
    )
    open_alerts_by_workspace: dict[int, int] = {}
    for row in latest_alerts:
        if row.is_resolved:
            continue
        open_alerts_by_workspace[row.workspace_id] = open_alerts_by_workspace.get(row.workspace_id, 0) + 1
    latest_audits = (
        db.query(AuditLog)
        .order_by(AuditLog.id.desc())
        .limit(200)
        .all()
    )
    latest_sessions = (
        db.query(UserSession)
        .order_by(UserSession.id.desc())
        .limit(200)
        .all()
    )
    backups = list_backups(limit=100)
    workspace_metrics: dict[int, dict[str, int]] = {}
    for ws in workspaces:
        workspace_metrics[ws.id] = collect_tenant_metrics(db, workspace_id=ws.id)

    workspace_rows: list[dict] = []
    for ws in workspaces:
        owner = next((u for u in users if u.workspace_id == ws.id and u.role == "admin"), None)
        sub = subs.get(ws.id) or get_or_create_subscription(db, workspace_id=ws.id)
        m = workspace_metrics.get(ws.id, {})
        status = "suspended" if ws.is_suspended else ("inactive" if not ws.is_active else "active")
        workspace_rows.append(
            {
                "workspace": ws,
                "subscription": sub,
                "metrics": m,
                "open_alerts_count": int(open_alerts_by_workspace.get(ws.id, 0)),
                "id": ws.id,
                "name": ws.name,
                "tenant_code": ws.tenant_code,
                "status": status,
                "plan_code": sub.plan_code,
                "sub_status": sub.status,
                "owner_username": (owner.username if owner else "—"),
                "managers_active": int(m.get("managers_active", 0)),
                "dialogs_total": int(m.get("dialogs_total", 0)),
                "messages_month": int(m.get("messages_month", 0)),
            }
        )

    user_rows: list[dict] = []
    user_by_id: dict[int, ServiceUser] = {u.id: u for u in users}
    for u in users:
        user_rows.append(
            {
                "id": u.id,
                "workspace_id": u.workspace_id,
                "username": u.username,
                "role": u.role,
                "is_active": bool(u.is_active),
                "is_blocked": bool(u.is_blocked),
                "email_verified": bool(getattr(u, "email_verified", False)),
                "last_login_at": _to_iso(u.last_login_at),
            }
        )

    audits_rows = [
        {
            "id": row.id,
            "workspace_id": row.workspace_id,
            "actor_user_id": row.actor_user_id,
            "actor_username": user_by_id[row.actor_user_id].username
            if row.actor_user_id in user_by_id
            else "—",
            "action": row.action,
            "object_type": row.object_type,
            "object_id": row.object_id,
            "created_at": _to_iso(row.created_at),
            "details_json": row.details_json,
        }
        for row in latest_audits
    ]

    alert_rows = [
        {
            "id": row.id,
            "workspace_id": row.workspace_id,
            "severity": row.severity,
            "alert_key": row.alert_key,
            "message": row.message,
            "metric_value": row.metric_value,
            "is_resolved": bool(row.is_resolved),
            "created_at": _to_iso(row.created_at),
        }
        for row in latest_alerts
    ]

    session_rows = [
        {
            "id": row.id,
            "user_id": row.user_id,
            "username": user_by_id[row.user_id].username if row.user_id in user_by_id else "—",
            "is_revoked": bool(row.is_revoked),
            "ip_address": row.ip_address,
            "user_agent": row.user_agent,
            "created_at": _to_iso(row.created_at),
            "last_seen_at": _to_iso(row.last_seen_at),
            "expires_at": _to_iso(row.expires_at),
        }
        for row in latest_sessions
    ]

    plan_rows: list[dict] = []
    for ws in workspaces:
        sub = subs.get(ws.id) or get_or_create_subscription(db, workspace_id=ws.id)
        plan_rows.append(
            {
                "workspace_id": ws.id,
                "workspace_name": ws.name,
                "plan_code": sub.plan_code,
                "status": sub.status,
                "manager_limit": sub.manager_limit,
                "dialogs_limit": sub.dialogs_limit,
                "messages_per_month_limit": sub.messages_per_month_limit,
                "folders_limit": getattr(sub, "folders_limit", 30),
                "quick_replies_limit": getattr(sub, "quick_replies_limit", 100),
                "grace_until": _to_iso(sub.grace_until),
            }
        )

    page_title_map = {
        "dashboard": "Панель суперадмина",
        "workspaces": "Клиенты (рабочие пространства)",
        "users": "Пользователи",
        "plans": "Тарифы и лимиты",
        "security": "Безопасность",
        "monitoring": "Мониторинг",
        "backups": "Резервные копии",
        "audit": "Аудит-лог",
        "system": "Системные настройки",
    }
    smtp_info = {
        "host": settings.smtp_host or "—",
        "port": settings.smtp_port,
        "sender": settings.smtp_sender or settings.smtp_username or "—",
        "tls": "включен" if settings.smtp_use_tls else "выключен",
        "ssl": "включен" if settings.smtp_use_ssl else "выключен",
        "email_verify_ttl": settings.email_verification_token_ttl_seconds,
        "email_verify_resend_cooldown": settings.email_verification_resend_cooldown_seconds,
    }
    security_info = {
        "rate_login": settings.rate_limit_login_per_minute,
        "rate_webhook": settings.rate_limit_webhook_per_minute,
        "rate_billing": settings.rate_limit_billing_per_minute,
        "force_https": "включен" if settings.force_https else "выключен",
    }

    return {
        "request": request,
        "current_user": current_user,
        "section": active_tab,
        "active_tab": active_tab,
        "page_title": page_title_map.get(active_tab, "Панель суперадмина"),
        "message": message,
        "error": error,
        "stats": dashboard,
        "dashboard": dashboard,
        "workspace_rows": workspace_rows,
        "users": user_rows,
        "user_rows": user_rows,
        "plan_rows": plan_rows,
        "alerts": alert_rows,
        "alert_rows": alert_rows,
        "audit_items": audits_rows,
        "audit_rows": audits_rows,
        "backups": backups,
        "session_rows": session_rows,
        "monitor_rows": [
            {
                "workspace_id": ws.id,
                "tenant_code": ws.tenant_code,
                "is_suspended": ws.is_suspended,
                "metrics": workspace_metrics.get(ws.id, {}),
            }
            for ws in workspaces
        ],
        "backup_rows": backups,
        "smtp": smtp_info,
        "security": security_info,
        "superadmin_static_2fa_code_enabled": bool((settings.superadmin_static_2fa_code or "").strip()),
        "system_flags": {
            "smtp_enabled": bool((settings.smtp_host or "").strip()),
            "webhook_secret_set": bool((settings.webhook_secret or "").strip()),
            "billing_secret_set": bool((settings.billing_hook_secret or "").strip()),
            "admin_totp_set": bool((settings.admin_totp_secret or "").strip()),
        },
    }


def _render_superadmin_page(
    *,
    request: Request,
    current_user: ServiceUser,
    db: Session,
    tab: str,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    _require_superadmin(current_user)
    context = _build_superadmin_context(
        request=request,
        current_user=current_user,
        db=db,
        tab=tab,
        message=message,
        error=error,
    )
    return templates.TemplateResponse(request, "superadmin.html", context)

app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory="app/templates")
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
webhook_path = settings.webhook_path if settings.webhook_path.startswith("/") else f"/{settings.webhook_path}"

Path("app/static/uploads").mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
_outbox_worker_task: asyncio.Task | None = None
_rate_limiter = InMemoryRateLimiter()
_MAX_UPLOAD_BYTES = int(settings.max_upload_bytes)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EMAIL_VERIFY_SALT = "email-verify-link"


def _apply_security_headers(response):
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if settings.force_https:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    csp = "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    response.headers.setdefault("Content-Security-Policy", csp)
    return response


@app.middleware("http")
async def add_security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    return _apply_security_headers(response)


def _enforce_same_origin(request: Request) -> None:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return
    if not request.url.path.startswith(("/app", "/admin")):
        return
    origin = (request.headers.get("origin") or "").strip()
    host = (request.headers.get("host") or "").strip()
    if not host:
        return
    expected = f"{request.url.scheme}://{host}"
    if origin:
        if not is_same_origin(origin=origin, host_url=expected):
            raise HTTPException(status_code=403, detail="csrf_origin_mismatch")
    else:
        referer = (request.headers.get("referer") or "").strip()
        if referer and not is_same_origin(origin=referer, host_url=expected):
            raise HTTPException(status_code=403, detail="csrf_referer_mismatch")


def _rate_limit_key(request: Request, scope: str) -> str:
    ip = request.client.host if request.client else "unknown"
    return f"{scope}:{ip}"


def _check_rate_limit_or_raise(request: Request, *, scope: str, limit: int, window_seconds: int = 60) -> None:
    if limit <= 0:
        return
    key = _rate_limit_key(request, scope)
    if not _rate_limiter.allow(key=key, limit=limit, window_seconds=window_seconds):
        raise HTTPException(status_code=429, detail=f"rate_limit_exceeded:{scope}")


async def _read_and_validate_upload(photo: UploadFile | None) -> tuple[str | None, bytes | None]:
    if not photo or not photo.filename:
        return None, None
    content = await photo.read()
    ok, reason = is_safe_image(
        filename=photo.filename,
        content=content,
        max_bytes=_MAX_UPLOAD_BYTES,
    )
    if not ok:
        if reason == "file_too_large":
            raise HTTPException(status_code=413, detail="Файл слишком большой")
        raise HTTPException(status_code=400, detail="Некорректный файл изображения")
    ext = Path(photo.filename).suffix.lower()
    return ext, content


def _workspace_id_for_user(user: ServiceUser | None) -> int:
    if user is None:
        return DEFAULT_WORKSPACE_ID
    return int(user.workspace_id or DEFAULT_WORKSPACE_ID)


def _sha256(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _json_escape(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)[1:-1]


def _email_verify_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=settings.secret_key, salt=_EMAIL_VERIFY_SALT)


def _create_email_verify_token(*, user_id: int, email: str) -> str:
    payload = {"uid": int(user_id), "email": (email or "").strip().lower()}
    return _email_verify_serializer().dumps(payload)


def _decode_email_verify_token(token: str, *, max_age_seconds: int | None = None) -> tuple[int, str] | None:
    ttl = max_age_seconds if max_age_seconds is not None else max(
        60,
        int(settings.email_verification_token_ttl_seconds),
    )
    try:
        payload = _email_verify_serializer().loads(token, max_age=ttl)
    except (BadData, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    user_id = payload.get("uid")
    email = str(payload.get("email", "")).strip().lower()
    if not isinstance(user_id, int) or not email:
        return None
    return user_id, email


def _build_email_verify_link(token: str) -> str:
    base = settings.public_base_url.rstrip("/")
    query = urlencode({"token": token})
    return f"{base}/app/verify-email?{query}"


def _send_email_verification_message(*, user: ServiceUser) -> bool:
    email = (user.username or "").strip().lower()
    if not email or not _EMAIL_RE.fullmatch(email):
        return False
    token = _create_email_verify_token(user_id=user.id, email=email)
    verify_link = _build_email_verify_link(token)
    ok, _ = send_email_verification(to_email=email, verify_link=verify_link)
    return ok


def _password_requirements(password: str) -> dict[str, bool]:
    value = password or ""
    return {
        "length": len(value) >= 8,
        "upper": any(ch.isupper() for ch in value),
        "lower": any(ch.islower() for ch in value),
        "digit": any(ch.isdigit() for ch in value),
        "special": any(not ch.isalnum() for ch in value),
    }


def _is_strong_password(password: str) -> bool:
    checks = _password_requirements(password)
    return all(checks.values())


def _password_missing_hint(password: str) -> str:
    checks = _password_requirements(password)
    missing: list[str] = []
    if not checks["length"]:
        missing.append("минимум 8 символов")
    if not checks["upper"]:
        missing.append("заглавную букву (A-Z)")
    if not checks["lower"]:
        missing.append("строчную букву (a-z)")
    if not checks["digit"]:
        missing.append("цифру")
    if not checks["special"]:
        missing.append("спецсимвол (!@#$...)")
    if not missing:
        return ""
    return "Добавьте: " + ", ".join(missing) + "."


def _generate_temp_password() -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    while True:
        candidate = "".join(secrets.choice(alphabet) for _ in range(14))
        if _is_strong_password(candidate):
            return candidate


def _render_app_settings_page(
    request: Request,
    *,
    db: Session,
    current_user: ServiceUser,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    ok, reason = ensure_workspace_limits_and_state(db, workspace_id=workspace_id)
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    quick_replies = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id)
        .order_by(QuickReply.command.asc())
        .all()
    )
    intro_steps = (
        db.query(IntroStep)
        .filter(IntroStep.workspace_id == workspace_id, IntroStep.is_active.is_(True))
        .order_by(IntroStep.step_order.asc(), IntroStep.id.asc())
        .all()
    )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    managers = list_workspace_managers(db, workspace_id=workspace_id)
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    refresh_tenant_alerts(db, workspace_id=workspace_id)
    active_alerts = (
        db.query(TenantAlert)
        .filter(
            TenantAlert.workspace_id == workspace_id,
            TenantAlert.is_resolved.is_(False),
        )
        .order_by(TenantAlert.id.desc())
        .all()
    )
    note = message or f"Workspace: {workspace_id}. Менеджеров: {len(managers)}"
    if not ok:
        note += f" · Ограничение: {reason}"
    email_status = "подтверждена" if bool(current_user.email_verified) else "не подтверждена"
    cooldown_seconds = max(int(settings.email_verification_resend_cooldown_seconds), 0)
    can_resend_verification = True
    if not current_user.email_verified and current_user.email_verification_sent_at:
        elapsed = (datetime.now(UTC).replace(tzinfo=None) - current_user.email_verification_sent_at).total_seconds()
        can_resend_verification = elapsed >= cooldown_seconds
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
            "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
            "template_after_phone": get_template_text(
                db,
                TEMPLATE_AFTER_PHONE,
                workspace_id=workspace_id,
            ),
            "quick_replies": quick_replies,
            "intro_steps": intro_steps,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "tenant_alerts": active_alerts,
            "message": note,
            "error": error,
            "ui_mode": "app",
            "current_user": current_user,
            "chats_href": "/app/chats",
            "logout_action": "/app/logout",
            "settings_action": "/app/settings",
            "quick_reply_create_action": "/app/quick-replies",
            "quick_reply_delete_action_prefix": "/app/quick-replies/",
            "intro_create_action": "/app/settings/intro-steps",
            "intro_delete_action_prefix": "/app/settings/intro-steps/",
            "email_status": email_status,
            "email_verified": bool(current_user.email_verified),
            "email_value": current_user.username,
            "email_resend_action": "/app/resend-email-verification",
            "can_resend_email_verification": can_resend_verification,
        },
    )


def _ensure_superadmin_credentials(db: Session) -> ServiceUser:
    """
    Keep superadmin credentials aligned with configured bootstrap values.
    This prevents prod/login drift when DB was created before config change.
    """
    target_username = (settings.superadmin_username or "admin").strip().lower() or "admin"
    target_password = (settings.superadmin_password or "").strip()
    super_user = db.query(ServiceUser).filter(ServiceUser.role == "superadmin").first()

    if super_user is None:
        initial_password = target_password or "Admin#Temp123!"
        super_user = ServiceUser(
            workspace_id=None,
            role="superadmin",
            username=target_username,
            password_hash=hash_password(initial_password),
            is_active=True,
            is_blocked=False,
        )
        db.add(super_user)
        db.commit()
        db.refresh(super_user)
        return super_user

    changed = False
    if super_user.username != target_username:
        super_user.username = target_username
        changed = True
    if target_password and not verify_password(target_password, super_user.password_hash):
        super_user.password_hash = hash_password(target_password)
        changed = True
    if not super_user.is_active:
        super_user.is_active = True
        changed = True
    if super_user.is_blocked:
        super_user.is_blocked = False
        changed = True
    if changed:
        db.add(super_user)
        db.commit()
        db.refresh(super_user)
    return super_user


async def _outbox_worker_loop() -> None:
    from app.database import SessionLocal

    while True:
        try:
            with SessionLocal() as db:
                await process_outbox_queue(db, limit=settings.outbox_worker_batch_size)
                workspace_ids = [row[0] for row in db.query(Workspace.id).all()]
                for workspace_id in workspace_ids:
                    refresh_tenant_alerts(db, workspace_id=int(workspace_id))
                    ensure_workspace_active_by_billing(db, workspace_id=int(workspace_id))
        except Exception:
            # Keep worker alive even if one cycle fails.
            pass
        await asyncio.sleep(max(settings.outbox_poll_interval_seconds, 1))


@app.on_event("startup")
async def startup() -> None:
    global _outbox_worker_task
    init_db()
    from app.database import SessionLocal

    with SessionLocal() as db:
        ensure_default_templates(db)
        workspace_ids = [row[0] for row in db.query(Workspace.id).all()]
        for workspace_id in workspace_ids:
            get_or_create_subscription(db, workspace_id=int(workspace_id))
        _ensure_superadmin_credentials(db)
    if settings.outbox_worker_enabled:
        _outbox_worker_task = asyncio.create_task(_outbox_worker_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global _outbox_worker_task
    if _outbox_worker_task is None:
        return
    _outbox_worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await _outbox_worker_task
    _outbox_worker_task = None


@app.get("/", response_class=RedirectResponse)
def index() -> RedirectResponse:
    return RedirectResponse(url="/app")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/admin/login", response_class=HTMLResponse)
def login_page() -> RedirectResponse:
    return RedirectResponse(url="/app", status_code=302)


@app.post("/admin/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
) -> RedirectResponse:
    if sign_in_admin(request, username=username, password=password):
        return RedirectResponse(url="/app", status_code=302)
    return RedirectResponse(url="/app", status_code=302)


@app.post("/admin/logout")
def logout() -> RedirectResponse:
    return RedirectResponse(url="/app", status_code=302)


@app.get("/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    replies = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id)
        .order_by(QuickReply.command.asc())
        .all()
    )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
            "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
            "template_after_phone": get_template_text(
                db,
                TEMPLATE_AFTER_PHONE,
                workspace_id=workspace_id,
            ),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/settings", response_class=HTMLResponse)
def update_settings(
    request: Request,
    prestart_message: str = Form(...),
    start_message: str = Form(...),
    after_phone_message: str = Form(...),
    manager_account_id: str = Form(...),
    admin_account_id: str = Form(""),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)
    workspace_id = DEFAULT_WORKSPACE_ID
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    bot_settings.manager_account_id = manager_account_id.strip()
    bot_settings.admin_account_id = admin_account_id.strip()
    db.add(bot_settings)
    set_template_text(db, TEMPLATE_PRESTART, prestart_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_START, start_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_AFTER_PHONE, after_phone_message, workspace_id=workspace_id)
    db.commit()

    replies = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id)
        .order_by(QuickReply.command.asc())
        .all()
    )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
            "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
            "template_after_phone": get_template_text(
                db,
                TEMPLATE_AFTER_PHONE,
                workspace_id=workspace_id,
            ),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": "Настройки сохранены",
            "error": None,
        },
    )


@app.post("/admin/quick-replies", response_class=HTMLResponse)
async def create_quick_reply(
    request: Request,
    command: str = Form(...),
    title: str = Form(...),
    text: str = Form(""),
    photo: UploadFile | None = File(default=None),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)
    workspace_id = DEFAULT_WORKSPACE_ID

    normalized = command.strip().lstrip("/")
    normalized = normalized.lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")

    if (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.command == normalized,
        )
        .first()
    ):
        bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
        replies = (
            db.query(QuickReply)
            .filter(QuickReply.workspace_id == workspace_id)
            .order_by(QuickReply.command.asc())
            .all()
        )
        chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
        delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
        return templates.TemplateResponse(
            request,
            "admin.html",
            {
                "request": request,
                "settings": bot_settings,
                "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
                "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
                "template_after_phone": get_template_text(
                    db,
                    TEMPLATE_AFTER_PHONE,
                    workspace_id=workspace_id,
                ),
                "quick_replies": replies,
                "webhook_path": webhook_path,
                "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
                "chat_metrics": chat_metrics,
                "delivery_metrics": delivery_metrics,
                "message": None,
                "error": f"Команда /{normalized} уже существует",
            },
            status_code=400,
        )

    image_path = None
    if photo and photo.filename:
        ext, content = await _read_and_validate_upload(photo)
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    reply = QuickReply(
        workspace_id=workspace_id,
        command=normalized,
        title=title.strip(),
        text=text.strip(),
        image_path=image_path,
    )
    db.add(reply)
    db.commit()

    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    replies = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id)
        .order_by(QuickReply.command.asc())
        .all()
    )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
            "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
            "template_after_phone": get_template_text(
                db,
                TEMPLATE_AFTER_PHONE,
                workspace_id=workspace_id,
            ),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": f"Быстрый ответ /{normalized} добавлен",
            "error": None,
        },
    )


@app.post("/admin/quick-replies/{reply_id}/delete", response_class=RedirectResponse)
def delete_quick_reply(
    reply_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    reply = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == DEFAULT_WORKSPACE_ID,
            QuickReply.id == reply_id,
        )
        .first()
    )
    if reply:
        if reply.image_path:
            relative_static_path = reply.image_path.removeprefix("/static/")
            img_path = Path("app/static") / relative_static_path
            if img_path.exists():
                img_path.unlink()
        db.delete(reply)
        db.commit()
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin/chats", response_class=HTMLResponse)
async def admin_chats_page(
    request: Request,
    conversation_id: int | None = None,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    return await _render_chat_workspace(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        ui=_admin_chats_ui(),
        include_removed=True,
        workspace_id=workspace_id,
    )


@app.post("/admin/chats/folders", response_class=RedirectResponse)
def admin_chat_create_folder(
    name: str = Form(""),
    conversation_id: int | None = Form(default=None),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    folder_name = name.strip()
    if folder_name:
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            assign_conversation_to_folder(
                db,
                conversation_id=conversation_id,
                folder_id=created.id,
                workspace_id=workspace_id,
            )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    conv_qs = f"&conversation_id={conversation_id}" if conversation_id is not None else ""
    view_qs = f"&view={view}" if view else ""
    return RedirectResponse(
        url=f"/admin/chats?q={q}{folder_qs}{conv_qs}{view_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def admin_chat_mark_unread(
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    ok = mark_thread_unread(
        db,
        conversation_id=conversation_id,
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&unread={suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def admin_chat_move_folder(
    conversation_id: int,
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    ok = assign_conversation_to_folder(
        db,
        conversation_id=conversation_id,
        folder_id=(folder_id if folder_id > 0 else None),
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={current_folder_id}" if current_folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&foldered={suffix}{folder_qs}",
        status_code=302,
    )


@app.get("/admin/chats/{conversation_id}/profile", response_class=HTMLResponse)
def admin_chat_customer_profile(
    request: Request,
    conversation_id: int,
    q: str = "",
    view: str = "",
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    conversation = db.query(Conversation).filter(Conversation.id == conversation_id).first()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Чат не найден")

    meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()
    profile = (
        db.query(CustomerProfile)
        .filter(CustomerProfile.customer_account_id == conversation.customer_account_id)
        .first()
    )

    sender_ids = [
        str(row[0])
        for row in (
            db.query(MessageLog.sender_account_id)
            .filter(MessageLog.conversation_id == conversation_id)
            .distinct()
            .limit(20)
            .all()
        )
        if row and row[0]
    ]
    message_mids = [
        {
            "direction": row.direction,
            "source": row.source,
            "max_mid": row.max_message_mid,
            "link_mid": row.link_mid,
        }
        for row in (
            db.query(ChatMessage)
            .filter(
                ChatMessage.conversation_id == conversation_id,
                (ChatMessage.max_message_mid.is_not(None)) | (ChatMessage.link_mid.is_not(None)),
            )
            .order_by(ChatMessage.id.desc())
            .limit(20)
            .all()
        )
    ]
    manager_dispatch_mids = [
        str(row.manager_message_mid)
        for row in (
            db.query(ManagerDispatch)
            .filter(ManagerDispatch.conversation_id == conversation_id)
            .order_by(ManagerDispatch.id.desc())
            .limit(20)
            .all()
        )
        if row.manager_message_mid
    ]
    outbox_targets = [
        {
            "operation": row.operation,
            "target_chat_id": row.target_chat_id,
            "target_user_id": row.target_user_id,
            "state": row.state,
            "external_message_mid": row.external_message_mid,
        }
        for row in (
            db.query(OutboxMessage)
            .filter(OutboxMessage.conversation_id == conversation_id)
            .order_by(OutboxMessage.id.desc())
            .limit(20)
            .all()
        )
    ]

    safe_q = q.strip()
    safe_view = view.strip().lower()
    back_to_chat_url = f"/admin/chats?conversation_id={conversation_id}&q={safe_q}"
    if view.strip().lower() == "chat":
        back_to_chat_url += "&view=chat"
    back_to_list_url = f"/admin/chats?q={safe_q}"

    basic_rows = [
        {"label": "Тикет", "value": f"T-{meta.ticket_no}" if meta else "—"},
        {
            "label": "Имя",
            "value": (profile.first_name if profile and profile.first_name else "Покупатель"),
        },
        {
            "label": "Username",
            "value": (f"@{profile.username}" if profile and profile.username else "—"),
        },
        {
            "label": "Телефон",
            "value": (
                (meta.phone_number if meta and meta.phone_number else "")
                or (profile.phone_number if profile and profile.phone_number else "")
                or "—"
            ),
        },
        {"label": "Статус", "value": (meta.status if meta else "—")},
        {"label": "Телефон подтвержден", "value": ("да" if meta and meta.phone_verified else "нет")},
    ]
    technical_rows = [
        {"label": "conversation_id", "value": str(conversation.id)},
        {"label": "chat_id", "value": conversation.chat_id or "—"},
        {"label": "customer_account_id", "value": conversation.customer_account_id or "—"},
        {
            "label": "source_chat_id",
            "value": (profile.source_chat_id if profile and profile.source_chat_id else "—"),
        },
        {"label": "manager_owner_id", "value": (meta.manager_owner_id if meta and meta.manager_owner_id else "—")},
        {"label": "sender_ids (последние)", "value": "\n".join(sender_ids) if sender_ids else "—"},
        {
            "label": "message mids (последние)",
            "value": (
                "\n".join(
                    [
                        f"{item['direction']}/{item['source']} max:{item['max_mid'] or '-'} link:{item['link_mid'] or '-'}"
                        for item in message_mids
                    ]
                )
                if message_mids
                else "—"
            ),
        },
        {
            "label": "manager dispatch mids",
            "value": "\n".join(manager_dispatch_mids) if manager_dispatch_mids else "—",
        },
        {
            "label": "outbox targets",
            "value": (
                "\n".join(
                    [
                        f"{item['operation']} chat:{item['target_chat_id'] or '-'} user:{item['target_user_id'] or '-'} [{item['state']}] mid:{item['external_message_mid'] or '-'}"
                        for item in outbox_targets
                    ]
                )
                if outbox_targets
                else "—"
            ),
        },
    ]

    return templates.TemplateResponse(
        request,
        "admin_customer_profile.html",
        {
            "request": request,
            "conversation": conversation,
            "meta": meta,
            "profile": profile,
            "basic_rows": basic_rows,
            "technical_rows": technical_rows,
            "conversation_id": conversation_id,
            "query": safe_q,
            "view": safe_view,
            "back_to_chat_url": back_to_chat_url,
            "back_to_list_url": back_to_list_url,
        },
    )


@app.post("/admin/chats/{conversation_id}/send", response_class=RedirectResponse)
async def admin_chats_send_message(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    edit_message_id: int | None = Form(default=None),
    photo: UploadFile | None = File(default=None),
    q: str = Form(""),
    view: str = Form(""),
    schedule_at: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="admin_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    workspace_id = DEFAULT_WORKSPACE_ID
    text_value = text.strip()
    view_value = view.strip().lower()
    image_path = None
    if photo and photo.filename:
        ext, content = await _read_and_validate_upload(photo)
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    if edit_message_id is not None:
        updated = None
        if text_value and not image_path:
            updated = await update_chat_message_text(
                db=db,
                chat_message_id=edit_message_id,
                new_text=text_value,
                workspace_id=workspace_id,
            )
        suffix = "1" if updated else "0"
        redirect_url = f"/admin/chats?conversation_id={conversation_id}&edited={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=redirect_url, status_code=302)

    if text_value.startswith("/") and not image_path:
        sent_ok = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
        )
        suffix = "1" if sent_ok else "0"
        redirect_url = f"/admin/chats?conversation_id={conversation_id}&quick={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=redirect_url, status_code=302)

    sent_ok = await send_admin_chat_message(
        db=db,
        conversation_id=conversation_id,
        text=text_value,
        image_path=image_path,
        workspace_id=workspace_id,
        schedule_at_iso=schedule_at,
    )
    scheduled_at_clean = schedule_at.strip()
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    redirect_url = f"/admin/chats?conversation_id={conversation_id}&{flag_name}={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view_value == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/admin/chats/{conversation_id}/quick-reply", response_class=RedirectResponse)
async def admin_chats_send_quick_reply(
    conversation_id: int,
    command: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    sent_ok = await send_admin_quick_reply(
        db=db,
        conversation_id=conversation_id,
        command_text=command,
        workspace_id=workspace_id,
    )
    suffix = "1" if sent_ok else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&quick={suffix}",
        status_code=302,
    )


@app.get("/admin/chats/{conversation_id}/quick-options")
def admin_chats_quick_options(
    conversation_id: int,
    q: str = "",
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    workspace_id = DEFAULT_WORKSPACE_ID
    conversation = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.is_active.is_(True),
        )
        .order_by(QuickReply.command.asc())
        .all()
    )
    query = q.strip().lstrip("/").lower()
    results = []
    for item in conversation:
        if query:
            hay = f"{item.command} {item.title} {item.text}".lower()
            if query not in hay:
                continue
        results.append(
            {
                "command": item.command,
                "title": item.title,
                "text": item.text,
                "has_image": bool(item.image_path),
            }
        )
        if len(results) >= 12:
            break
    return {"conversation_id": conversation_id, "items": results}


@app.post("/admin/chats/{conversation_id}/messages/{chat_message_id}/edit", response_class=RedirectResponse)
async def admin_chats_edit_message(
    conversation_id: int,
    chat_message_id: int,
    text: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    updated = await update_chat_message_text(
        db=db,
        chat_message_id=chat_message_id,
        new_text=text.strip(),
        workspace_id=workspace_id,
    )
    suffix = "1" if updated else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&edited={suffix}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/messages/{chat_message_id}/delete", response_class=RedirectResponse)
async def admin_chats_delete_message(
    conversation_id: int,
    chat_message_id: int,
    q: str = Form(""),
    view: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    removed = await remove_chat_message(
        db=db,
        chat_message_id=chat_message_id,
        workspace_id=workspace_id,
    )
    suffix = "1" if removed else "0"
    redirect_url = f"/admin/chats?conversation_id={conversation_id}&deleted={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view.strip().lower() == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post(
    "/admin/chats/{conversation_id}/messages/{chat_message_id}/retry",
    response_class=RedirectResponse,
)
async def admin_chats_retry_message(
    conversation_id: int,
    chat_message_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    retried = await retry_failed_outbox_message(
        db=db,
        chat_message_id=chat_message_id,
        workspace_id=workspace_id,
    )
    suffix = "1" if retried else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&retried={suffix}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/delete-user", response_class=RedirectResponse)
async def admin_chats_delete_conversation(
    conversation_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    deleted = delete_conversation(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )
    suffix = "1" if deleted else "0"
    return RedirectResponse(url=f"/admin/chats?removed={suffix}", status_code=302)


@app.post("/admin/chats/{conversation_id}/rename-user", response_class=RedirectResponse)
def admin_chats_rename_user(
    request: Request,
    conversation_id: int,
    customer_name: str = Form(""),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id = DEFAULT_WORKSPACE_ID
    updated = _rename_conversation_customer(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
        new_name=customer_name,
    )
    suffix = "1" if updated else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={quote_plus(q.strip())}&view={view}&renamed={suffix}{folder_qs}",
        status_code=302,
    )


def _require_manager_mini_access(token: str, db: Session) -> dict:
    claims = verify_manager_mini_claims(token)
    if not claims:
        raise HTTPException(status_code=403, detail="Недействительный токен mini-app")
    manager_id = str(claims.get("manager_id", "")).strip()
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    if not manager_id:
        raise HTTPException(status_code=403, detail="Недействительный токен mini-app")

    manager_user_id = claims.get("service_user_id")
    if isinstance(manager_user_id, int):
        manager_user = (
            db.query(ServiceUser)
            .filter(
                ServiceUser.id == manager_user_id,
                ServiceUser.workspace_id == workspace_id,
                ServiceUser.role == "manager",
                ServiceUser.is_active.is_(True),
                ServiceUser.is_blocked.is_(False),
            )
            .first()
        )
        if manager_user is None:
            raise HTTPException(status_code=403, detail="Доступ mini-app запрещен")
        expected_id = (manager_user.max_account_id or manager_user.username or "").strip()
        if expected_id and manager_id != expected_id:
            raise HTTPException(status_code=403, detail="Доступ mini-app запрещен")
        return claims

    settings_db = get_or_create_settings(db, workspace_id=workspace_id)
    expected_manager_id = (settings_db.manager_account_id or "").strip()
    if expected_manager_id and manager_id != expected_manager_id:
        raise HTTPException(status_code=403, detail="Доступ mini-app запрещен")
    return claims


def _manager_mini_url(
    *,
    token: str,
    conversation_id: int | None = None,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    extra: str = "",
) -> str:
    url = f"/mini/manager?token={quote_plus(token)}"
    if conversation_id is not None:
        url += f"&conversation_id={conversation_id}"
    if q.strip():
        url += f"&q={quote_plus(q.strip())}"
    if view.strip().lower() == "chat":
        url += "&view=chat"
    if folder_id is not None:
        url += f"&folder_id={folder_id}"
    if extra:
        if not extra.startswith("&"):
            url += "&"
        url += extra.lstrip("&")
    return url


def _admin_chats_ui() -> dict[str, str | bool]:
    return {
        "page_title": "Max Admin Chats",
        "page_path": "/admin/chats",
        "page_query_prefix": "/admin/chats?",
        "access_token": "",
        "create_folder_action": "/admin/chats/folders",
        "show_admin_nav": True,
        "settings_href": "/admin",
        "logout_action": "/admin/logout",
        "show_profile_links": True,
        "show_delete_user": True,
        "show_rename_user": True,
        "allow_message_edit_actions": True,
        "send_action_prefix": "/admin/chats/",
        "send_action_suffix": "",
        "delete_user_action_prefix": "/admin/chats/",
        "rename_user_action_prefix": "/admin/chats/",
        "profile_href_prefix": "/admin/chats/",
        "mark_unread_prefix": "/admin/chats/",
        "move_folder_prefix": "/admin/chats/",
        "create_folder_endpoint": "/admin/chats/folders",
        "delete_message_prefix": "/admin/chats/",
        "endpoint_query_suffix": "",
    }


def _manager_mini_ui(token: str) -> dict[str, str | bool]:
    token_value = quote_plus(token)
    query_suffix = f"?token={token_value}"
    return {
        "page_title": "Max Manager Mini App",
        "page_path": "/mini/manager",
        "page_query_prefix": f"/mini/manager?token={token_value}&",
        "access_token": token,
        "create_folder_action": f"/mini/manager/chats/folders{query_suffix}",
        "show_admin_nav": False,
        "settings_href": "",
        "logout_action": "",
        "show_profile_links": False,
        "show_delete_user": False,
        "show_rename_user": True,
        "allow_message_edit_actions": False,
        "send_action_prefix": "/mini/manager/chats/",
        "send_action_suffix": query_suffix,
        "delete_user_action_prefix": "",
        "rename_user_action_prefix": "/mini/manager/chats/",
        "profile_href_prefix": "",
        "mark_unread_prefix": "/mini/manager/chats/",
        "move_folder_prefix": "/mini/manager/chats/",
        "create_folder_endpoint": "/mini/manager/chats/folders",
        "delete_message_prefix": "/mini/manager/chats/",
        "endpoint_query_suffix": query_suffix,
    }


def _resolve_workspace_for_token_or_user(
    *,
    db: Session,
    request: Request | None = None,
    token: str | None = None,
) -> int:
    if token:
        claims = verify_manager_mini_claims(token)
        if claims and isinstance(claims.get("workspace_id"), int):
            return int(claims["workspace_id"])
    if request is not None:
        current_user = get_current_service_user(request=request, db=db)
        if current_user and current_user.workspace_id:
            return int(current_user.workspace_id)
    return DEFAULT_WORKSPACE_ID


def _resolve_app_workspace_scope(
    *,
    db: Session,
    current_user: ServiceUser,
    workspace_id: int | None,
) -> tuple[int, Workspace | None, bool]:
    """
    Resolve workspace scope for /app/chats.
    Superadmin must not access /app/chats: only /app/superadmin is allowed.
    """
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")

    current_workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    if workspace_id is not None and int(workspace_id) != int(current_workspace_id):
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    target = db.query(Workspace).filter(Workspace.id == int(current_workspace_id)).first()
    return int(current_workspace_id), target, False


def _workspace_scope_query_suffix(*, workspace_id: int, is_scoped: bool) -> str:
    if not is_scoped:
        return ""
    return f"&workspace_id={workspace_id}"


def _rename_conversation_customer(
    db: Session,
    *,
    conversation_id: int,
    workspace_id: int,
    new_name: str,
) -> bool:
    name_clean = new_name.strip()
    if not name_clean:
        return False
    conversation = (
        db.query(Conversation)
        .filter(
            Conversation.id == conversation_id,
            Conversation.workspace_id == workspace_id,
        )
        .first()
    )
    if conversation is None:
        return False
    profile = (
        db.query(CustomerProfile)
        .filter(
            CustomerProfile.workspace_id == workspace_id,
            CustomerProfile.customer_account_id == conversation.customer_account_id,
        )
        .first()
    )
    if profile is None:
        profile = CustomerProfile(
            workspace_id=workspace_id,
            customer_account_id=conversation.customer_account_id,
            first_name=name_clean,
            source_chat_id=conversation.chat_id or "",
        )
    else:
        profile.first_name = name_clean
        if not profile.source_chat_id:
            profile.source_chat_id = conversation.chat_id or ""
    db.add(profile)
    db.commit()
    return True


def _render_app_landing(
    request: Request,
    *,
    login_error: str | None = None,
    register_error: str | None = None,
    manager_error: str | None = None,
    manager_message: str | None = None,
    register_message: str | None = None,
    default_workspace_name: str = "",
    default_display_name: str = "",
    default_username: str = "",
    view: str = "home",
    invite_token: str = "",
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "app_landing.html",
        {
            "request": request,
            "login_error": login_error,
            "register_error": register_error,
            "manager_error": manager_error,
            "manager_message": manager_message,
            "register_message": register_message,
            "default_workspace_name": default_workspace_name,
            "default_display_name": default_display_name,
            "default_username": default_username,
            "view": view,
            "invite_token": invite_token,
        },
    )


def _build_manager_invite_links(
    *,
    db: Session,
    manager: ServiceUser,
    workspace_id: int,
    base_url: str,
) -> dict[str, str]:
    manager_identifier = (manager.max_account_id or manager.username or "").strip()
    if not manager_identifier:
        manager_identifier = manager.username
    raw_token = create_manager_invite_token(
        invite_id=manager.id,
        workspace_id=workspace_id,
        max_account_id=manager_identifier,
        manager_user_id=manager.id,
    )
    token_hash = _sha256(raw_token)
    invite_row = (
        db.query(ManagerInvite)
        .filter(
            ManagerInvite.workspace_id == workspace_id,
            ManagerInvite.used_by_user_id == manager.id,
            ManagerInvite.is_used.is_(False),
            ManagerInvite.expires_at > datetime.now(UTC).replace(tzinfo=None),
        )
        .first()
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    if invite_row is None:
        invite_row = ManagerInvite(
            workspace_id=workspace_id,
            max_account_id=manager_identifier,
            invite_token_hash=token_hash,
            display_name=manager.display_name or manager.username,
            expires_at=now + timedelta(days=7),
            is_used=False,
            used_by_user_id=manager.id,
        )
    else:
        invite_row.max_account_id = manager_identifier
        invite_row.invite_token_hash = token_hash
        invite_row.display_name = manager.display_name or manager.username
        # Keep a generated one-time link stable until it is consumed/expired.
        invite_row.expires_at = now + timedelta(days=7)
    db.add(invite_row)
    db.commit()

    encoded = quote_plus(raw_token)
    return {
        "invite_link": f"{base_url}/app/invite/{encoded}",
        "mini_link": f"{base_url}/mini/manager?token={quote_plus(create_manager_mini_token(manager_identifier, workspace_id=workspace_id, service_user_id=manager.id))}",
        "web_link": f"{base_url}/app/chats",
    }


def _manager_invite_context(
    *,
    db: Session,
    token: str,
) -> tuple[dict, ManagerInvite | None, ServiceUser | None, Workspace | None]:
    claims = verify_manager_invite_token(token)
    if not claims:
        raise HTTPException(status_code=400, detail="Недействительная или просроченная ссылка приглашения")
    invite_hash = _sha256(token)
    invite_row = (
        db.query(ManagerInvite)
        .filter(ManagerInvite.invite_token_hash == invite_hash)
        .first()
    )
    if invite_row is None:
        raise HTTPException(status_code=404, detail="Приглашение не найдено")
    now = datetime.now(UTC).replace(tzinfo=None)
    if invite_row.is_used:
        raise HTTPException(status_code=410, detail="Ссылка уже использована")
    if invite_row.expires_at and invite_row.expires_at < now:
        raise HTTPException(status_code=410, detail="Срок действия ссылки истек")
    manager_user_id = claims.get("manager_user_id")
    manager_user = None
    if isinstance(manager_user_id, int):
        manager_user = (
            db.query(ServiceUser)
            .filter(ServiceUser.id == manager_user_id)
            .first()
        )
    workspace = db.query(Workspace).filter(Workspace.id == int(claims["workspace_id"])).first()
    if workspace is None or not workspace.is_active or workspace.is_suspended:
        raise HTTPException(status_code=403, detail="Workspace недоступен")
    return claims, invite_row, manager_user, workspace


def _require_csrf(request: Request) -> None:
    cookie_token = (request.cookies.get(CSRF_COOKIE_NAME) or "").strip()
    header_token = (request.headers.get("x-csrf-token") or "").strip()
    if not cookie_token or not header_token or not secrets.compare_digest(cookie_token, header_token):
        raise HTTPException(status_code=403, detail="CSRF verification failed")


def _set_csrf_cookie_if_missing(request: Request, response: HTMLResponse | RedirectResponse) -> None:
    existing = (request.cookies.get(CSRF_COOKIE_NAME) or "").strip()
    value = existing or _new_csrf_token()
    response.set_cookie(
        CSRF_COOKIE_NAME,
        value,
        max_age=60 * 60 * 24 * 30,
        httponly=False,
        secure=bool(settings.secure_cookies),
        samesite="lax",
        path="/",
    )


def _sanitize_message_for_ui(value: str) -> str:
    return (value or "").strip()[:500]


def _is_password_complex(password: str) -> tuple[bool, list[str]]:
    value = (password or "").strip()
    missing: list[str] = []
    if len(value) < 8:
        missing.append("минимум 8 символов")
    if not any(ch.islower() for ch in value):
        missing.append("строчная буква")
    if not any(ch.isupper() for ch in value):
        missing.append("заглавная буква")
    if not any(ch.isdigit() for ch in value):
        missing.append("цифра")
    if not any(not ch.isalnum() for ch in value):
        missing.append("спецсимвол")
    return len(missing) == 0, missing


def _verify_superadmin_2fa_code(*, user: ServiceUser, code: str) -> bool:
    """
    Accept TOTP by default, with an optional explicit fallback code for bootstrap.
    """
    fallback_code = (settings.superadmin_static_2fa_code or "").strip()
    normalized_code = (code or "").strip()
    if fallback_code and secrets.compare_digest(normalized_code, fallback_code):
        return True
    return verify_totp_code(code=normalized_code, secret_b32=settings.admin_totp_secret.strip())


@app.get("/app", response_class=HTMLResponse)
def app_landing(
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    current_user = get_current_service_user(request=request, db=db)
    if current_user is not None:
        if current_user.role == "superadmin":
            return RedirectResponse(url="/app/superadmin", status_code=302)
        return RedirectResponse(url="/app/chats", status_code=302)
    return _render_app_landing(request)


@app.get("/app/login", response_class=HTMLResponse)
def app_login_page(
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    current_user = get_current_service_user(request=request, db=db)
    if current_user is not None:
        if current_user.role == "superadmin":
            return RedirectResponse(url="/app/superadmin", status_code=302)
        return RedirectResponse(url="/app/chats", status_code=302)
    return _render_app_landing(request, view="login")


@app.get("/sa/login", response_class=HTMLResponse)
def superadmin_login_page(
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    current_user = get_current_service_user(request=request, db=db)
    if current_user is not None:
        if current_user.role == "superadmin":
            return RedirectResponse(url="/app/superadmin", status_code=302)
        return RedirectResponse(url="/app/chats", status_code=302)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "error": None},
    )


@app.post("/sa/login", response_class=HTMLResponse)
def superadmin_login_submit(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    totp_code: str = Form(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_login",
        limit=max(int(settings.rate_limit_login_per_minute), 1),
    )
    user = sign_in_service_user(db, username=username, password=password)
    if user is None or user.role != "superadmin":
        return templates.TemplateResponse(
            request,
            "login.html",
            {"request": request, "error": "Неверный логин или пароль"},
            status_code=401,
        )
    if settings.admin_totp_secret.strip() or settings.superadmin_static_2fa_code.strip():
        if not _verify_superadmin_2fa_code(user=user, code=totp_code):
            return templates.TemplateResponse(
                request,
                "login.html",
                {"request": request, "error": "Неверный 2FA код"},
                status_code=401,
            )
    token = create_service_session(
        db,
        user_id=user.id,
        ip_address=request.client.host if request.client else "",
        user_agent=request.headers.get("user-agent", ""),
    )
    response = RedirectResponse(url="/app/superadmin", status_code=302)
    set_service_session_cookie(response, token)
    return response


@app.post("/app/register", response_class=HTMLResponse)
def app_register(
    request: Request,
    email: str = Form(""),
    password: str = Form(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="register",
        limit=max(int(settings.rate_limit_login_per_minute), 1),
    )
    email_clean = email.strip().lower()
    password_clean = password.strip()
    if not email_clean or not password_clean:
        return _render_app_landing(
            request,
            register_error="Введите email и пароль.",
            default_username=email_clean,
            view="register",
        )
    if "@" not in email_clean or "." not in email_clean.rsplit("@", 1)[-1]:
        return _render_app_landing(
            request,
            register_error="Введите корректный email.",
            default_username=email_clean,
            view="register",
        )
    password_ok, missing = _is_password_complex(password_clean)
    if not password_ok:
        return _render_app_landing(
            request,
            register_error="Пароль недостаточно сложный: " + ", ".join(missing),
            default_username=email_clean,
            view="register",
        )
    workspace_name_clean = f"Workspace {email_clean.split('@', 1)[0]}"
    try:
        workspace, owner = create_workspace_with_owner(
            db,
            workspace_name=workspace_name_clean,
            username=email_clean,
            password=password_clean,
            display_name=email_clean.split("@", 1)[0],
        )
    except ValueError as exc:
        code = str(exc)
        msg = "Не удалось зарегистрироваться."
        if code == "username_exists":
            msg = "Пользователь с таким email уже существует."
        elif code == "invalid_username":
            msg = "Email содержит недопустимые символы."
        elif code == "password_too_short":
            msg = "Пароль должен содержать минимум 8 символов."
        return _render_app_landing(
            request,
            register_error=msg,
            default_username=email_clean,
            view="register",
        )

    token = create_service_session(
        db,
        user_id=owner.id,
        ip_address=request.client.host if request.client else "",
        user_agent=request.headers.get("user-agent", ""),
    )
    response = RedirectResponse(url="/app/settings", status_code=302)
    set_service_session_cookie(response, token)
    sent_ok = _send_email_verification_message(user=owner)
    owner.email_verification_sent_at = datetime.now(UTC).replace(tzinfo=None)
    db.add(owner)
    db.add(
        AuditLog(
            workspace_id=workspace.id,
            actor_user_id=owner.id,
            action="workspace_registered",
            object_type="workspace",
            object_id=str(workspace.id),
            details_json='{"source":"landing"}',
        )
    )
    db.add(
        AuditLog(
            workspace_id=workspace.id,
            actor_user_id=owner.id,
            action="email_verification_sent",
            object_type="service_user",
            object_id=str(owner.id),
            details_json=f'{{"sent":{str(bool(sent_ok)).lower()}}}',
        )
    )
    db.commit()
    return response


@app.post("/app/login", response_class=HTMLResponse)
def app_login(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="login",
        limit=max(int(settings.rate_limit_login_per_minute), 1),
    )
    user = sign_in_service_user(db, username=username, password=password)
    if user is None:
        return _render_app_landing(
            request,
            login_error="Неверный логин или пароль.",
            default_username=username.strip().lower(),
            view="login",
        )
    if user.role == "superadmin":
        return _render_app_landing(
            request,
            login_error="Для superadmin используйте отдельный вход: /sa/login",
            default_username="",
            view="login",
        )
    token = create_service_session(
        db,
        user_id=user.id,
        ip_address=request.client.host if request.client else "",
        user_agent=request.headers.get("user-agent", ""),
    )
    target_url = "/app/superadmin" if user.role == "superadmin" else "/app/chats"
    response = RedirectResponse(url=target_url, status_code=302)
    set_service_session_cookie(response, token)
    return response


@app.post("/app/logout")
def app_logout(
    request: Request,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    current_user = get_current_service_user(request=request, db=db)
    if current_user is not None:
        revoke_workspace_sessions(db, workspace_id=current_user.workspace_id or DEFAULT_WORKSPACE_ID)
    response = RedirectResponse(url="/app", status_code=302)
    clear_service_session_cookie(response)
    return response


@app.post("/app/logout-all")
def app_logout_all(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    if current_user.role == "superadmin":
        revoke_all_service_sessions(db)
    else:
        revoke_workspace_sessions(db, workspace_id=current_user.workspace_id or DEFAULT_WORKSPACE_ID)
    response = RedirectResponse(url="/app", status_code=302)
    clear_service_session_cookie(response)
    return response


@app.get("/app/verify-email", response_class=HTMLResponse)
def app_verify_email(
    request: Request,
    token: str = "",
    db: Session = Depends(get_db),
) -> HTMLResponse:
    token_clean = (token or "").strip()
    decoded = _decode_email_verify_token(token_clean)
    if not token_clean or decoded is None:
        return _render_app_landing(
            request,
            register_error="Ссылка подтверждения недействительна или устарела.",
            view="login",
        )
    user_id, token_email = decoded
    user = db.query(ServiceUser).filter(ServiceUser.id == user_id).first()
    if user is None:
        return _render_app_landing(
            request,
            register_error="Пользователь для подтверждения не найден.",
            view="login",
        )
    current_email = (user.username or "").strip().lower()
    if current_email != token_email:
        return _render_app_landing(
            request,
            register_error="Эта ссылка больше не подходит для текущего email.",
            view="login",
        )
    if not user.email_verified:
        user.email_verified = True
        user.email_verified_at = datetime.now(UTC).replace(tzinfo=None)
        db.add(user)
        db.add(
            AuditLog(
                workspace_id=user.workspace_id or DEFAULT_WORKSPACE_ID,
                actor_user_id=user.id,
                action="email_verified",
                object_type="service_user",
                object_id=str(user.id),
                details_json='{"source":"verify_link"}',
            )
        )
        db.commit()
    return _render_app_landing(
        request,
        register_message="Email успешно подтверждён. Теперь можно войти.",
        default_username=current_email,
        view="login",
    )


@app.post("/app/resend-email-verification", response_class=HTMLResponse)
def app_resend_email_verification(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="email_verify_resend",
        limit=max(1, int(settings.rate_limit_login_per_minute)),
    )
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    if current_user.email_verified:
        return _render_app_settings_page(
            request,
            db=db,
            current_user=current_user,
            message="Email уже подтверждён.",
        )
    cooldown_seconds = max(int(settings.email_verification_resend_cooldown_seconds), 0)
    if current_user.email_verification_sent_at and cooldown_seconds > 0:
        elapsed = (
            datetime.now(UTC).replace(tzinfo=None) - current_user.email_verification_sent_at
        ).total_seconds()
        if elapsed < cooldown_seconds:
            remaining = int(cooldown_seconds - elapsed)
            return _render_app_settings_page(
                request,
                db=db,
                current_user=current_user,
                error=f"Повторная отправка будет доступна через {remaining} сек.",
            )
    sent_ok = _send_email_verification_message(user=current_user)
    now = datetime.now(UTC).replace(tzinfo=None)
    current_user.email_verification_sent_at = now
    db.add(current_user)
    db.add(
        AuditLog(
            workspace_id=current_user.workspace_id or DEFAULT_WORKSPACE_ID,
            actor_user_id=current_user.id,
            action="email_verification_resent",
            object_type="service_user",
            object_id=str(current_user.id),
            details_json=f'{{"sent":{str(bool(sent_ok)).lower()}}}',
        )
    )
    db.commit()
    if sent_ok:
        msg = "Письмо с подтверждением отправлено."
    else:
        msg = (
            "Письмо не отправлено: SMTP не настроен. "
            "Проверьте SMTP настройки на сервере."
        )
    return _render_app_settings_page(
        request,
        db=db,
        current_user=current_user,
        message=msg,
    )


@app.post("/app/quick-replies", response_class=HTMLResponse)
async def app_create_quick_reply(
    request: Request,
    command: str = Form(""),
    title: str = Form(""),
    text: str = Form(""),
    photo: UploadFile | None = File(default=None),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    normalized = command.strip().lstrip("/").lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")
    exists = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id, QuickReply.command == normalized)
        .first()
    )
    if exists:
        return RedirectResponse(url="/app/settings", status_code=302)
    image_path = None
    if photo and photo.filename:
        ext, content = await _read_and_validate_upload(photo)
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"
    db.add(
        QuickReply(
            workspace_id=workspace_id,
            command=normalized,
            title=title.strip(),
            text=text.strip(),
            image_path=image_path,
        )
    )
    db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/quick-replies/{reply_id}/delete", response_class=RedirectResponse)
def app_delete_quick_reply(
    request: Request,
    reply_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    reply = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id, QuickReply.id == reply_id)
        .first()
    )
    if reply:
        if reply.image_path:
            relative_static_path = reply.image_path.removeprefix("/static/")
            img_path = Path("app/static") / relative_static_path
            if img_path.exists():
                img_path.unlink()
        db.delete(reply)
        db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.get("/app/chats", response_class=HTMLResponse)
async def app_chats_page(
    request: Request,
    conversation_id: int | None = None,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    _check_rate_limit_or_raise(
        request,
        scope="app_view",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id, scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    query_scope_prefix = (
        f"/app/chats?workspace_id={workspace_id}&" if is_superadmin_scoped else "/app/chats?"
    )
    endpoint_scope_suffix = f"?workspace_id={workspace_id}" if is_superadmin_scoped else ""
    create_folder_action = f"/app/chats/folders{endpoint_scope_suffix}"
    page_title = f"Чаты клиента · {current_user.username}"
    settings_href = "/app/settings"
    if is_superadmin_scoped:
        page_title = f"Чаты клиента · {(scoped_workspace.name if scoped_workspace else workspace_id)}"
        settings_href = "/app/superadmin/workspaces"
    ui = _admin_chats_ui()
    ui.update(
        {
            "page_title": page_title,
            "page_path": "/app/chats",
            "page_query_prefix": query_scope_prefix,
            "create_folder_action": create_folder_action,
            "show_admin_nav": True,
            "settings_href": settings_href,
            "logout_action": "/app/logout",
            "show_rename_user": True,
            "send_action_prefix": "/app/chats/",
            "send_action_suffix": endpoint_scope_suffix,
            "delete_user_action_prefix": "/app/chats/",
            "rename_user_action_prefix": "/app/chats/",
            "profile_href_prefix": "/app/chats/",
            "mark_unread_prefix": "/app/chats/",
            "move_folder_prefix": "/app/chats/",
            "create_folder_endpoint": "/app/chats/folders",
            "delete_message_prefix": "/app/chats/",
            "endpoint_query_suffix": endpoint_scope_suffix,
        }
    )
    return await _render_chat_workspace(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        ui=ui,
        include_removed=True,
        workspace_id=workspace_id,
    )


@app.get("/app/settings", response_class=HTMLResponse)
def app_settings_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    return _render_app_settings_page(
        request,
        db=db,
        current_user=current_user,
    )


@app.get("/app/managers", response_class=HTMLResponse)
def app_managers_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    managers = list_workspace_managers(db, workspace_id=workspace_id)
    workspace = db.query(Workspace).filter(Workspace.id == workspace_id).first()
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")

    links: list[dict[str, str]] = []
    base_url = settings.public_base_url.rstrip("/")
    for manager in managers:
        built = _build_manager_invite_links(
            db=db,
            manager=manager,
            workspace_id=workspace_id,
            base_url=base_url,
        )
        links.append(
            {
                "username": manager.username,
                "display": manager.display_name or manager.username,
                "max_account_id": manager.max_account_id,
                "invite_link": built["invite_link"],
                "mini_link": built["mini_link"],
                "web_link": built["web_link"],
            }
        )

    return templates.TemplateResponse(
        request,
        "app_landing.html",
        {
            "request": request,
            "current_user": current_user,
            "current_workspace": workspace,
            "message": "Ссылки для менеджеров сформированы.",
            "error": None,
            "manager_links": links,
        },
    )


@app.post("/app/managers", response_class=HTMLResponse)
def app_create_manager(
    request: Request,
    username: str = Form(""),
    display_name: str = Form(""),
    max_account_id: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_manager_create",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    ok_ws, ws_err = ensure_workspace_limits_and_state(db, workspace_id=workspace_id)
    if not ok_ws:
        return templates.TemplateResponse(
            request,
            "app_landing.html",
            {
                "request": request,
                "current_user": current_user,
                "current_workspace": db.query(Workspace).filter(Workspace.id == workspace_id).first(),
                "message": None,
                "error": "Workspace приостановлен из-за биллинга.",
            },
            status_code=403,
        )
    can_add, add_err = can_add_manager(db, workspace_id=workspace_id)
    if not can_add:
        return templates.TemplateResponse(
            request,
            "app_landing.html",
            {
                "request": request,
                "current_user": current_user,
                "current_workspace": db.query(Workspace).filter(Workspace.id == workspace_id).first(),
                "message": None,
                "error": "Достигнут лимит менеджеров для текущего тарифа.",
            },
            status_code=400,
        )
    max_account_clean = max_account_id.strip()
    if not max_account_clean:
        return templates.TemplateResponse(
            request,
            "app_landing.html",
            {
                "request": request,
                "current_user": current_user,
                "current_workspace": db.query(Workspace).filter(Workspace.id == workspace_id).first(),
                "message": None,
                "error": "Укажите Max account ID для менеджера.",
            },
            status_code=400,
        )

    try:
        manager_user = create_service_user(
            db,
            username=username,
            password=f"InviteOnly#{uuid4().hex[:10]}",
            role="manager",
            workspace_id=workspace_id,
            display_name=display_name,
            max_account_id=max_account_clean,
        )
    except ValueError as exc:
        return templates.TemplateResponse(
            request,
            "app_landing.html",
            {
                "request": request,
                "current_user": current_user,
                "current_workspace": db.query(Workspace).filter(Workspace.id == workspace_id).first(),
                "message": None,
                "error": f"Не удалось добавить менеджера: {exc}",
            },
            status_code=400,
        )

    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="manager_created",
            object_type="service_user",
            object_id=str(manager_user.id),
            details_json=f'{{"username":"{manager_user.username}","max_account_id":"{max_account_clean}"}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/managers", status_code=302)


@app.post("/app/settings", response_class=RedirectResponse)
def app_update_settings(
    request: Request,
    prestart_message: str = Form(""),
    start_message: str = Form(""),
    after_phone_message: str = Form(""),
    manager_account_id: str = Form(""),
    admin_account_id: str = Form(""),
    routing_mode: str = Form("round_robin"),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    settings_row.manager_account_id = manager_account_id.strip()
    settings_row.admin_account_id = admin_account_id.strip()
    mode = (routing_mode or "round_robin").strip().lower()
    if mode not in {"round_robin", "random"}:
        mode = "round_robin"
    settings_row.routing_mode = mode
    db.add(settings_row)
    set_template_text(db, TEMPLATE_PRESTART, prestart_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_START, start_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_AFTER_PHONE, after_phone_message, workspace_id=workspace_id)
    db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/settings/intro-steps", response_class=RedirectResponse)
def app_add_intro_step(
    request: Request,
    text: str = Form(""),
    delay_seconds: int = Form(0),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    value = (text or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="intro_text_required")
    max_order = (
        db.query(func.max(IntroStep.step_order))
        .filter(IntroStep.workspace_id == workspace_id)
        .scalar()
    )
    step = IntroStep(
        workspace_id=workspace_id,
        step_order=int(max_order or 0) + 1,
        delay_seconds=max(0, int(delay_seconds)),
        text=value,
        is_active=True,
    )
    db.add(step)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="intro_step_added",
            object_type="intro_step",
            object_id="0",
            details_json="{}",
        )
    )
    db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/settings/intro-steps/{step_id}/delete", response_class=RedirectResponse)
def app_delete_intro_step(
    request: Request,
    step_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    if current_user.role not in {"owner", "admin"}:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    row = (
        db.query(IntroStep)
        .filter(IntroStep.id == step_id, IntroStep.workspace_id == workspace_id)
        .first()
    )
    if row is not None:
        db.delete(row)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="intro_step_deleted",
                object_type="intro_step",
                object_id=str(step_id),
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.get("/app/invite/{invite_token}", response_class=HTMLResponse)
def app_accept_manager_invite(
    invite_token: str,
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    try:
        claims, invite_row, manager_user, workspace = _manager_invite_context(
            db=db,
            token=invite_token,
        )
    except HTTPException as exc:
        return _render_app_landing(request, manager_error=str(exc.detail))

    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    max_account_id = str(claims.get("max_account_id", "")).strip()
    now = datetime.now(UTC).replace(tzinfo=None)

    if manager_user is None and max_account_id:
        manager_user = (
            db.query(ServiceUser)
            .filter(
                ServiceUser.workspace_id == workspace_id,
                ServiceUser.role == "manager",
                ServiceUser.max_account_id == max_account_id,
            )
            .first()
        )
    if manager_user is None or not manager_user.is_active or manager_user.is_blocked:
        return _render_app_landing(request, manager_error="Менеджер не активен или не найден.")
    if workspace is None or workspace.is_suspended or not workspace.is_active:
        return _render_app_landing(request, manager_error="Workspace недоступен.")
    if manager_user.password_hash:
        invite_row.is_used = True
        invite_row.used_by_user_id = manager_user.id
        invite_row.used_at = now
        db.add(invite_row)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=manager_user.id,
                action="manager_invite_consumed",
                object_type="manager_invite",
                object_id=str(invite_row.id),
                details_json=f'{{"manager_id":{manager_user.id},"mode":"auto_login"}}',
            )
        )
        db.commit()

        raw_session = create_service_session(
            db,
            user_id=manager_user.id,
            ip_address=request.client.host if request.client else "",
            user_agent=request.headers.get("user-agent", ""),
        )
        response = RedirectResponse(url="/app/chats", status_code=302)
        set_service_session_cookie(response, raw_session)
        return response

    return _render_app_landing(
        request,
        view="manager_invite",
        invite_token=invite_token,
        manager_message="Задайте пароль для аккаунта менеджера.",
    )


@app.post("/app/invite/{invite_token}", response_class=HTMLResponse)
def app_set_manager_password_from_invite(
    invite_token: str,
    request: Request,
    manager_password: str = Form(""),
    manager_password_confirm: str = Form(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="invite_password_set",
        limit=max(1, int(settings.rate_limit_login_per_minute)),
    )
    try:
        claims, invite_row, manager_user, workspace = _manager_invite_context(
            db=db,
            token=invite_token,
        )
    except HTTPException as exc:
        return _render_app_landing(request, manager_error=str(exc.detail))

    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    max_account_id = str(claims.get("max_account_id", "")).strip()
    now = datetime.now(UTC).replace(tzinfo=None)

    if manager_user is None and max_account_id:
        manager_user = (
            db.query(ServiceUser)
            .filter(
                ServiceUser.workspace_id == workspace_id,
                ServiceUser.role == "manager",
                ServiceUser.max_account_id == max_account_id,
            )
            .first()
        )
    if manager_user is None or not manager_user.is_active or manager_user.is_blocked:
        return _render_app_landing(request, manager_error="Менеджер не активен или не найден.")
    if workspace is None or workspace.is_suspended or not workspace.is_active:
        return _render_app_landing(request, manager_error="Workspace недоступен.")

    password_value = (manager_password or "").strip()
    confirm_value = (manager_password_confirm or "").strip()
    if not password_value or not confirm_value:
        return _render_app_landing(
            request,
            manager_error="Введите пароль и подтверждение.",
            view="manager_invite",
            invite_token=invite_token,
        )
    if password_value != confirm_value:
        return _render_app_landing(
            request,
            manager_error="Пароли не совпадают.",
            view="manager_invite",
            invite_token=invite_token,
        )
    ok, missing = _is_password_complex(password_value)
    if not ok:
        return _render_app_landing(
            request,
            manager_error="Слишком простой пароль: " + ", ".join(missing),
            view="manager_invite",
            invite_token=invite_token,
        )

    manager_user.password_hash = hash_password(password_value)
    invite_row.is_used = True
    invite_row.used_by_user_id = manager_user.id
    invite_row.used_at = now
    db.add(manager_user)
    db.add(invite_row)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=manager_user.id,
            action="manager_password_set_by_invite",
            object_type="service_user",
            object_id=str(manager_user.id),
            details_json='{"source":"invite"}',
        )
    )
    db.commit()

    raw_session = create_service_session(
        db,
        user_id=manager_user.id,
        ip_address=request.client.host if request.client else "",
        user_agent=request.headers.get("user-agent", ""),
    )
    response = RedirectResponse(url="/app/chats", status_code=302)
    set_service_session_cookie(response, raw_session)
    return response


@app.get("/app/superadmin", response_class=HTMLResponse)
def app_superadmin_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="dashboard",
        message="SaaS обзор загружен",
    )


@app.get("/app/superadmin/workspaces", response_class=HTMLResponse)
def app_superadmin_workspaces_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="workspaces",
    )


@app.get("/app/superadmin/workspaces/{workspace_id}/chats", response_class=RedirectResponse)
def app_superadmin_workspace_chats_redirect(
    workspace_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _require_superadmin(current_user)
    _ = (workspace_id, db)
    raise HTTPException(status_code=403, detail="Открытие чатов для superadmin отключено")


@app.get("/app/superadmin/users", response_class=HTMLResponse)
def app_superadmin_users_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="users",
    )


@app.get("/app/superadmin/plans", response_class=HTMLResponse)
def app_superadmin_plans_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="plans",
    )


@app.get("/app/superadmin/security", response_class=HTMLResponse)
def app_superadmin_security_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="security",
    )


@app.get("/app/superadmin/monitoring", response_class=HTMLResponse)
def app_superadmin_monitoring_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="monitoring",
    )


@app.get("/app/superadmin/backups/view", response_class=HTMLResponse)
def app_superadmin_backups_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="backups",
    )


@app.get("/app/superadmin/audit", response_class=HTMLResponse)
def app_superadmin_audit_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="audit",
    )


@app.get("/app/superadmin/system", response_class=HTMLResponse)
def app_superadmin_system_page(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="system",
    )


@app.post("/app/superadmin/users/{user_id}/role")
def app_superadmin_update_user_role(
    request: Request,
    user_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    # Role editing is disabled by business rules.
    _ = (request, user_id, db)
    return RedirectResponse(url="/app/superadmin/users", status_code=302)


@app.post("/app/superadmin/users/{user_id}/toggle-block")
def app_superadmin_toggle_user_block(
    request: Request,
    user_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    user = db.query(ServiceUser).filter(ServiceUser.id == user_id).first()
    if user is None:
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    if user.role == "superadmin":
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    user.is_blocked = not bool(user.is_blocked)
    if user.is_blocked:
        user.is_active = False
    db.add(user)
    db.add(
        AuditLog(
            workspace_id=user.workspace_id,
            actor_user_id=current_user.id,
            action="user_block_toggled",
            object_type="service_user",
            object_id=str(user.id),
            details_json='{"is_blocked":' + ("true" if user.is_blocked else "false") + "}",
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/users", status_code=302)


@app.post("/app/superadmin/users/{user_id}/revoke-sessions")
def app_superadmin_revoke_user_sessions(
    request: Request,
    user_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    user = db.query(ServiceUser).filter(ServiceUser.id == user_id).first()
    if user is None:
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    if user.role == "superadmin":
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    revoked = revoke_user_sessions(db, user_id=user_id)
    db.add(
        AuditLog(
            workspace_id=user.workspace_id,
            actor_user_id=current_user.id,
            action="user_sessions_revoked",
            object_type="service_user",
            object_id=str(user.id),
            details_json=f'{{"revoked":{revoked}}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/users", status_code=302)


@app.post("/app/superadmin/users/{user_id}/delete")
def app_superadmin_delete_user(
    request: Request,
    user_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    user = db.query(ServiceUser).filter(ServiceUser.id == user_id).first()
    if user is None:
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    if user.role == "superadmin":
        return RedirectResponse(url="/app/superadmin/users", status_code=302)
    workspace_id = user.workspace_id
    username = user.username
    db.query(UserSession).filter(UserSession.user_id == user_id).delete(synchronize_session=False)
    db.query(ManagerInvite).filter(ManagerInvite.used_by_user_id == user_id).update(
        {ManagerInvite.used_by_user_id: None},
        synchronize_session=False,
    )
    db.query(AuditLog).filter(AuditLog.actor_user_id == user_id).update(
        {AuditLog.actor_user_id: None},
        synchronize_session=False,
    )
    db.delete(user)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="user_deleted",
            object_type="service_user",
            object_id=str(user_id),
            details_json=f'{{"username":"{username}"}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/users", status_code=302)


@app.post("/app/billing/hook")
def app_billing_hook(
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
) -> dict:
    _check_rate_limit_or_raise(
        request,
        scope="billing",
        limit=max(int(settings.rate_limit_billing_per_minute), 1),
    )
    expected = settings.billing_hook_secret.strip()
    if expected:
        signature = request.headers.get("X-Billing-Signature", "") or request.headers.get("X-Billing-Secret", "")
        body_bytes = safe_json_dumps(payload).encode("utf-8")
        if not verify_hmac_signature(body=body_bytes, secret=expected, provided_signature=signature):
            raise HTTPException(status_code=403, detail="invalid_billing_signature")
    workspace_id = int(payload.get("workspace_id") or 0)
    if workspace_id <= 0:
        raise HTTPException(status_code=400, detail="workspace_id_required")
    event_type = str(payload.get("event_type") or "").strip()
    if not event_type:
        raise HTTPException(status_code=400, detail="event_type_required")
    external_id = str(payload.get("external_id") or "").strip()
    sub = apply_billing_hook(
        db,
        workspace_id=workspace_id,
        event_type=event_type,
        external_id=external_id,
        payload_json=str(payload),
    )
    return {"ok": True, "workspace_id": workspace_id, "status": sub.status}


@app.post("/app/superadmin/workspaces/{workspace_id}/suspend")
def app_superadmin_suspend_workspace(
    request: Request,
    workspace_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    ws = db.query(Workspace).filter(Workspace.id == workspace_id).first()
    if ws:
        ws.manual_suspended = True
        ws.is_suspended = True
        db.add(ws)
        revoke_workspace_sessions(db, workspace_id=workspace_id)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="workspace_suspended",
                object_type="workspace",
                object_id=str(workspace_id),
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)


@app.post("/app/superadmin/workspaces/{workspace_id}/resume")
def app_superadmin_resume_workspace(
    request: Request,
    workspace_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    ws = db.query(Workspace).filter(Workspace.id == workspace_id).first()
    if ws:
        ws.manual_suspended = False
        ws.is_suspended = False
        db.add(ws)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="workspace_resumed",
                object_type="workspace",
                object_id=str(workspace_id),
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)


@app.post("/app/superadmin/workspaces/{workspace_id}/revoke-sessions")
def app_superadmin_revoke_workspace_sessions(
    request: Request,
    workspace_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    revoke_workspace_sessions(db, workspace_id=workspace_id)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="workspace_sessions_revoked",
            object_type="workspace",
            object_id=str(workspace_id),
            details_json="{}",
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)


@app.post("/app/superadmin/workspaces/{workspace_id}/delete")
def app_superadmin_delete_workspace(
    request: Request,
    workspace_id: int,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    if workspace_id == DEFAULT_WORKSPACE_ID:
        return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)
    ws = db.query(Workspace).filter(Workspace.id == workspace_id).first()
    if ws is None:
        return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)
    ws_name = ws.name
    user_ids = [
        row[0]
        for row in db.query(ServiceUser.id)
        .filter(ServiceUser.workspace_id == workspace_id, ServiceUser.role != "superadmin")
        .all()
    ]
    if user_ids:
        db.query(UserSession).filter(UserSession.user_id.in_(user_ids)).delete(synchronize_session=False)
        db.query(AuditLog).filter(AuditLog.actor_user_id.in_(user_ids)).update(
            {AuditLog.actor_user_id: None},
            synchronize_session=False,
        )
    db.query(ManagerInvite).filter(ManagerInvite.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(OutboxMessage).filter(OutboxMessage.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ManagerDispatch).filter(ManagerDispatch.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ConversationMeta).filter(ConversationMeta.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(MessageLog).filter(MessageLog.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ChatMessage).filter(ChatMessage.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(Conversation).filter(Conversation.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(CustomerProfile).filter(CustomerProfile.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ChatFolder).filter(ChatFolder.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(IntroStep).filter(IntroStep.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(QuickReply).filter(QuickReply.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(MessageTemplate).filter(MessageTemplate.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(BotSettings).filter(BotSettings.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(Subscription).filter(Subscription.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(TenantAlert).filter(TenantAlert.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(BillingEvent).filter(BillingEvent.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ServiceUser).filter(ServiceUser.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(AuditLog).filter(AuditLog.workspace_id == workspace_id).delete(synchronize_session=False)
    db.delete(ws)
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="workspace_deleted",
            object_type="workspace",
            object_id=str(workspace_id),
            details_json=f'{{"name":"{ws_name}"}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)


@app.post("/app/superadmin/workspaces/{workspace_id}/plan")
def app_superadmin_update_workspace_plan(
    request: Request,
    workspace_id: int,
    manager_limit: int = Form(3),
    dialogs_limit: int = Form(500),
    messages_per_month_limit: int = Form(5000),
    plan_code: str = Form("trial"),
    status: str = Form("active"),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    sub.plan_code = (plan_code or "trial").strip().lower()[:64] or "trial"
    sub.manager_limit = max(1, int(manager_limit))
    sub.dialogs_limit = max(1, int(dialogs_limit))
    sub.messages_per_month_limit = max(1, int(messages_per_month_limit))
    sub.status = (status or "active").strip().lower()
    db.add(sub)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="workspace_plan_updated",
            object_type="subscription",
            object_id=str(sub.id),
            details_json=(
                f'{{"plan_code":"{sub.plan_code}","manager_limit":{sub.manager_limit},"dialogs_limit":{sub.dialogs_limit},'
                f'"messages_per_month_limit":{sub.messages_per_month_limit},"status":"{sub.status}"}}'
            ),
        )
    )
    db.commit()
    ensure_workspace_active_by_billing(db, workspace_id=workspace_id)
    return RedirectResponse(url="/app/superadmin/plans", status_code=302)


@app.post("/app/superadmin/backup")
def app_superadmin_create_backup(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    with SessionLocal() as db:
        backup_path = create_sqlite_backup()
        db.add(
            AuditLog(
                workspace_id=None,
                actor_user_id=current_user.id,
                action="backup_created",
                object_type="backup",
                object_id=backup_path.name,
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/superadmin/backups/view", status_code=302)


@app.post("/app/superadmin/restore")
def app_superadmin_restore_backup(
    request: Request,
    backup_name: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    target_name = backup_name.strip()
    restore_sqlite_backup(target_name)
    with SessionLocal() as db:
        db.add(
            AuditLog(
                workspace_id=None,
                actor_user_id=current_user.id,
                action="backup_restored",
                object_type="backup",
                object_id=target_name,
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/superadmin/backups/view", status_code=302)


@app.get("/app/superadmin/backups")
def app_superadmin_backups_redirect(
    current_user: ServiceUser = Depends(require_service_user),
) -> RedirectResponse:
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для суперадмина")
    return RedirectResponse(url="/app/superadmin/backups/view", status_code=302)


@app.get("/app/superadmin/backups/list")
def app_superadmin_list_backups(
    current_user: ServiceUser = Depends(require_service_user),
) -> dict:
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для суперадмина")
    return {"ok": True, "items": list_backups(limit=50)}


@app.get("/app/superadmin/backup-check")
def app_superadmin_backup_check(
    current_user: ServiceUser = Depends(require_service_user),
) -> dict:
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    items = list_backups(limit=1)
    return {"ok": bool(items), "latest_backup": (items[0] if items else None)}


@app.get("/app/superadmin/metrics")
def app_superadmin_metrics(
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> dict:
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    results: list[dict] = []
    for ws in db.query(Workspace).order_by(Workspace.id.asc()).all():
        metrics = collect_tenant_metrics(db, workspace_id=ws.id)
        created_alerts = refresh_tenant_alerts(db, workspace_id=ws.id)
        results.append(
            {
                "workspace_id": ws.id,
                "tenant_code": ws.tenant_code,
                "is_suspended": ws.is_suspended,
                "metrics": metrics,
                "new_alerts": len(created_alerts),
            }
        )
    return {"ok": True, "tenants": results}


@app.get("/app/superadmin/alerts")
def app_superadmin_alerts(
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> dict:
    if current_user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для superadmin")
    alerts = (
        db.query(TenantAlert)
        .order_by(TenantAlert.id.desc())
        .limit(200)
        .all()
    )
    return {
        "ok": True,
        "items": [
            {
                "id": a.id,
                "workspace_id": a.workspace_id,
                "alert_key": a.alert_key,
                "severity": a.severity,
                "message": a.message,
                "metric_value": a.metric_value,
                "is_resolved": a.is_resolved,
                "created_at": str(a.created_at),
                "resolved_at": (str(a.resolved_at) if a.resolved_at else None),
            }
            for a in alerts
        ],
    }


@app.post("/app/superadmin/2fa", response_class=RedirectResponse)
def app_superadmin_set_2fa_secret(
    request: Request,
    secret_b32: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="superadmin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    _require_superadmin(current_user)
    normalized = (secret_b32 or "").strip().replace(" ", "").upper()
    if normalized and (len(normalized) < 16 or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567=" for ch in normalized)):
        raise HTTPException(status_code=400, detail="invalid_totp_secret")
    settings.admin_totp_secret = normalized
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="superadmin_2fa_updated",
            object_type="security",
            object_id="admin_totp_secret",
            details_json='{"enabled":' + ("true" if bool(normalized) else "false") + "}",
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/security", status_code=302)


@app.post("/app/chats/folders", response_class=RedirectResponse)
def app_chat_create_folder(
    name: str = Form(""),
    conversation_id: int | None = Form(default=None),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    folder_name = name.strip()
    if folder_name:
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            assign_conversation_to_folder(
                db,
                conversation_id=conversation_id,
                folder_id=created.id,
                workspace_id=workspace_id,
            )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    conv_qs = f"&conversation_id={conversation_id}" if conversation_id is not None else ""
    view_qs = f"&view={view}" if view else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?q={q}{folder_qs}{conv_qs}{view_qs}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def app_chat_mark_unread(
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    ok = mark_thread_unread(db, conversation_id=conversation_id, workspace_id=workspace_id)
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&q={q}&view={view}&unread={suffix}{folder_qs}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def app_chat_move_folder(
    conversation_id: int,
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    ok = assign_conversation_to_folder(
        db,
        conversation_id=conversation_id,
        folder_id=(folder_id if folder_id > 0 else None),
        workspace_id=workspace_id,
    )
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={current_folder_id}" if current_folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&q={q}&view={view}&foldered={suffix}{folder_qs}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/send", response_class=RedirectResponse)
async def app_chats_send_message(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    edit_message_id: int | None = Form(default=None),
    photo: UploadFile | None = File(default=None),
    q: str = Form(""),
    view: str = Form(""),
    schedule_at: str = Form(""),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="app_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    text_value = text.strip()
    view_value = view.strip().lower()
    image_path = None
    if photo and photo.filename:
        ext, content = await _read_and_validate_upload(photo)
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    if edit_message_id is not None:
        updated = None
        if text_value and not image_path:
            updated = await update_chat_message_text(
                db=db,
                chat_message_id=edit_message_id,
                new_text=text_value,
                workspace_id=workspace_id,
            )
        suffix = "1" if updated else "0"
        redirect_url = f"/app/chats?conversation_id={conversation_id}&edited={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=f"{redirect_url}{workspace_qs}", status_code=302)

    if text_value.startswith("/") and not image_path:
        sent_ok = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
        )
        suffix = "1" if sent_ok else "0"
        redirect_url = f"/app/chats?conversation_id={conversation_id}&quick={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=f"{redirect_url}{workspace_qs}", status_code=302)

    sent_ok = await send_admin_chat_message(
        db=db,
        conversation_id=conversation_id,
        text=text_value,
        image_path=image_path,
        workspace_id=workspace_id,
        schedule_at_iso=schedule_at,
    )
    scheduled_at_clean = schedule_at.strip()
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    redirect_url = f"/app/chats?conversation_id={conversation_id}&{flag_name}={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view_value == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=f"{redirect_url}{workspace_qs}", status_code=302)


@app.post("/app/chats/{conversation_id}/quick-reply", response_class=RedirectResponse)
async def app_chats_send_quick_reply(
    conversation_id: int,
    command: str = Form(""),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    sent_ok = await send_admin_quick_reply(
        db=db,
        conversation_id=conversation_id,
        command_text=command,
        workspace_id=workspace_id,
    )
    suffix = "1" if sent_ok else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&quick={suffix}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/messages/{chat_message_id}/edit", response_class=RedirectResponse)
async def app_chats_edit_message(
    conversation_id: int,
    chat_message_id: int,
    text: str = Form(""),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    updated = await update_chat_message_text(
        db=db,
        chat_message_id=chat_message_id,
        new_text=text.strip(),
        workspace_id=workspace_id,
    )
    suffix = "1" if updated else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&edited={suffix}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/messages/{chat_message_id}/delete", response_class=RedirectResponse)
async def app_chats_delete_message(
    conversation_id: int,
    chat_message_id: int,
    q: str = Form(""),
    view: str = Form(""),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    removed = await remove_chat_message(db=db, chat_message_id=chat_message_id, workspace_id=workspace_id)
    suffix = "1" if removed else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    redirect_url = f"/app/chats?conversation_id={conversation_id}&deleted={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view.strip().lower() == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=f"{redirect_url}{workspace_qs}", status_code=302)


@app.post(
    "/app/chats/{conversation_id}/messages/{chat_message_id}/retry",
    response_class=RedirectResponse,
)
async def app_chats_retry_message(
    conversation_id: int,
    chat_message_id: int,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    retried = await retry_failed_outbox_message(
        db=db,
        chat_message_id=chat_message_id,
        workspace_id=workspace_id,
    )
    suffix = "1" if retried else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&retried={suffix}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/delete-user", response_class=RedirectResponse)
async def app_chats_delete_conversation(
    conversation_id: int,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    deleted = delete_conversation(db, conversation_id=conversation_id, workspace_id=workspace_id)
    suffix = "1" if deleted else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(url=f"/app/chats?removed={suffix}{workspace_qs}", status_code=302)


@app.post("/app/chats/{conversation_id}/rename-user", response_class=RedirectResponse)
def app_chats_rename_user(
    request: Request,
    conversation_id: int,
    customer_name: str = Form(""),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    updated = _rename_conversation_customer(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
        new_name=customer_name,
    )
    suffix = "1" if updated else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={conversation_id}&q={quote_plus(q.strip())}&view={view}&renamed={suffix}{folder_qs}{workspace_qs}",
        status_code=302,
    )


def _chat_op_messages(request: Request) -> tuple[str | None, str | None]:
    sent_flag = request.query_params.get("sent")
    scheduled_flag = request.query_params.get("scheduled")
    quick_flag = request.query_params.get("quick")
    edited_flag = request.query_params.get("edited")
    deleted_flag = request.query_params.get("deleted")
    retried_flag = request.query_params.get("retried")
    renamed_flag = request.query_params.get("renamed")
    op_message = None
    op_error = None
    if sent_flag == "1":
        op_message = "Сообщение отправлено"
    if sent_flag == "0":
        op_error = "Не удалось отправить сообщение"
    if scheduled_flag == "1":
        op_message = "Сообщение запланировано"
    if scheduled_flag == "0":
        op_error = "Не удалось запланировать сообщение"
    if quick_flag == "1":
        op_message = "Быстрый ответ отправлен"
    if quick_flag == "0":
        op_error = "Не удалось отправить быстрый ответ"
    if edited_flag == "1":
        op_message = "Сообщение изменено"
    if edited_flag == "0":
        op_error = "Не удалось изменить сообщение"
    if deleted_flag == "1":
        op_message = "Сообщение удалено"
    if deleted_flag == "0":
        op_error = "Не удалось удалить сообщение"
    if retried_flag == "1":
        op_message = "Повторная отправка выполнена"
    if retried_flag == "0":
        op_error = "Повторная отправка не удалась"
    if renamed_flag == "1":
        op_message = "Пользователь переименован"
    if renamed_flag == "0":
        op_error = "Не удалось переименовать пользователя"
    if request.query_params.get("unread") == "1":
        op_message = "Чат отмечен непрочитанным"
    if request.query_params.get("unread") == "0":
        op_error = "Не удалось отметить чат непрочитанным"
    if request.query_params.get("foldered") == "1":
        op_message = "Чат перемещен в папку"
    if request.query_params.get("foldered") == "0":
        op_error = "Не удалось переместить чат в папку"
    return op_message, op_error


async def _render_chat_workspace(
    *,
    request: Request,
    db: Session,
    conversation_id: int | None,
    q: str,
    view: str,
    folder_id: int | None,
    ui: dict[str, str | bool],
    include_removed: bool,
    workspace_id: int,
) -> HTMLResponse:
    # Opportunistically drain due queue items on every workspace open.
    await process_outbox_queue(db, limit=30)
    op_message, op_error = _chat_op_messages(request)

    threads = load_chat_threads(db, query=q, workspace_id=workspace_id)
    if folder_id is not None:
        if folder_id > 0:
            threads = [item for item in threads if item.folder_id == folder_id]
        else:
            threads = [item for item in threads if item.folder_id is None]

    has_explicit_conversation = conversation_id is not None
    active_thread = None
    if conversation_id is not None:
        for item in threads:
            if item.conversation_id == conversation_id:
                active_thread = item
                break
    if active_thread is None and threads and not has_explicit_conversation:
        active_thread = threads[0]

    messages = []
    if active_thread:
        mark_thread_read(db, active_thread.conversation_id, workspace_id=workspace_id)
        messages = load_chat_messages(db, active_thread.conversation_id, workspace_id=workspace_id)
    mobile_chat_view = view.strip().lower() == "chat"

    context: dict = {
        "request": request,
        "threads": threads,
        "active_thread": active_thread,
        "messages": messages,
        "query": q,
        "folder_filter": folder_id,
        "message": op_message,
        "error": op_error,
        "mobile_chat_view": mobile_chat_view,
        "admin_quick_options": [
            {"command": item.command, "title": item.title}
            for item in list_active_quick_replies(db, workspace_id=workspace_id)
        ],
        "chat_folders": [
            {"id": folder.id, "name": folder.name}
            for folder in list_chat_folders(db, workspace_id=workspace_id)
        ],
        "ui": ui,
    }
    if include_removed:
        context["removed"] = request.query_params.get("removed")
    return templates.TemplateResponse(request, "admin_chats.html", context)


@app.get("/mini/manager", response_class=HTMLResponse)
async def manager_mini_page(
    request: Request,
    token: str,
    conversation_id: int | None = None,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    return await _render_chat_workspace(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        ui=_manager_mini_ui(token),
        include_removed=False,
        workspace_id=workspace_id,
    )


@app.post("/mini/manager/chats/folders", response_class=RedirectResponse)
def manager_mini_create_folder(
    token: str,
    name: str = Form(""),
    conversation_id: int | None = Form(default=None),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    folder_name = name.strip()
    if folder_name:
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            assign_conversation_to_folder(
                db,
                conversation_id=conversation_id,
                folder_id=created.id,
                workspace_id=workspace_id,
            )
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=folder_id,
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def manager_mini_mark_unread(
    request: Request,
    conversation_id: int,
    token: str,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    ok = mark_thread_unread(db, conversation_id=conversation_id, workspace_id=workspace_id)
    suffix = "1" if ok else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"unread={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def manager_mini_move_folder(
    request: Request,
    conversation_id: int,
    token: str,
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    ok = assign_conversation_to_folder(
        db,
        conversation_id=conversation_id,
        folder_id=(folder_id if folder_id > 0 else None),
        workspace_id=workspace_id,
    )
    suffix = "1" if ok else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=current_folder_id,
            extra=f"foldered={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/rename-user", response_class=RedirectResponse)
def manager_mini_rename_user(
    request: Request,
    conversation_id: int,
    token: str,
    customer_name: str = Form(""),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    updated = _rename_conversation_customer(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
        new_name=customer_name,
    )
    suffix = "1" if updated else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"renamed={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/send", response_class=RedirectResponse)
async def manager_mini_send_message(
    request: Request,
    conversation_id: int,
    token: str,
    text: str = Form(""),
    photo: UploadFile | None = File(default=None),
    q: str = Form(""),
    view: str = Form(""),
    schedule_at: str = Form(""),
    folder_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    text_value = text.strip()
    view_value = view.strip().lower()
    image_path = None
    if photo and photo.filename:
        ext, content = await _read_and_validate_upload(photo)
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    if text_value.startswith("/") and not image_path:
        sent_ok = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
        )
        suffix = "1" if sent_ok else "0"
        return RedirectResponse(
            url=_manager_mini_url(
                token=token,
                conversation_id=conversation_id,
                q=q,
                view=view_value,
                folder_id=folder_id,
                extra=f"quick={suffix}",
            ),
            status_code=302,
        )

    sent_ok = await send_admin_chat_message(
        db=db,
        conversation_id=conversation_id,
        text=text_value,
        image_path=image_path,
        workspace_id=workspace_id,
        schedule_at_iso=schedule_at,
    )
    scheduled_at_clean = schedule_at.strip()
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view_value,
            folder_id=folder_id,
            extra=f"{flag_name}={suffix}",
        ),
        status_code=302,
    )


@app.post(webhook_path)
@app.post("/webhook/max")
@app.post("/max-webhook")
@app.post("/max-webhok")
@app.post("/webhok/max")
async def max_webhook(
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
) -> dict:
    _check_rate_limit_or_raise(
        request,
        scope="webhook",
        limit=max(int(settings.rate_limit_webhook_per_minute), 1),
    )
    webhook_secret = (settings.webhook_secret or "").strip()
    if webhook_secret:
        signature = (
            request.headers.get("X-Webhook-Signature")
            or request.headers.get("X-Hub-Signature-256")
            or ""
        )
        raw_body = await request.body()
        if not verify_hmac_signature(
            body=raw_body,
            secret=webhook_secret,
            provided_signature=signature,
        ):
            raise HTTPException(status_code=403, detail="invalid_webhook_signature")
    event = MaxWebhookEvent.from_payload(payload)
    if event is None:
        # Ignore non-message updates or malformed events without failing webhook delivery.
        return {"ok": True, "ignored": "unsupported_payload"}

    # Webhook dedup by stable event UID.
    event_uid = event.event_uid_value()
    if event_uid:
        seen = db.query(WebhookEvent).filter(WebhookEvent.event_uid == event_uid).first()
        if seen:
            return {"ok": True, "ignored": "duplicate_event"}
        db.add(WebhookEvent(event_uid=event_uid, update_type=event.update_type))
        db.commit()

    accepted_update_types = {
        "message_created",
        "message_callback",
        "new_message",
        "bot_started",
        "bot_start",
    }
    if event.update_type and event.update_type not in accepted_update_types:
        return {"ok": True, "ignored": event.update_type}

    settings_db = get_or_create_settings(db)
    max_client = MaxClient()

    if event.sender_id == settings.max_bot_account_id:
        return {"ok": True}

    if settings_db.admin_account_id and event.sender_id == settings_db.admin_account_id:
        return {"ok": True, "ignored": "admin"}

    if event.sender_id == settings_db.manager_account_id:
        return await handle_manager_message(
            db=db,
            client=max_client,
            settings=settings_db,
            event=event,
        )

    return await handle_customer_event(
        db=db,
        client=max_client,
        settings=settings_db,
        event=event,
    )
