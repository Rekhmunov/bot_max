from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import secrets
import string
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import urlencode
from urllib.parse import urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jinja2 import TemplateNotFound
from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy import func, text
from sqlalchemy.exc import OperationalError
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
    sign_in_service_user_with_reason,
    verify_manager_invite_token,
    verify_manager_mini_claims,
    verify_manager_mini_token,
    _sha256 as auth_sha256,
)
from app.config import settings
from app.database import get_db, init_db
from app.manager_bridge import (
    TEMPLATE_AFTER_PHONE,
    TEMPLATE_PRESTART,
    TEMPLATE_START,
    block_conversation_customer,
    unblock_conversation_customer,
    create_chat_folder,
    delete_chat_folders,
    delete_conversation,
    ensure_default_templates,
    get_conversation_folder_ids,
    get_chat_metrics,
    get_delivery_metrics,
    get_media_diagnostics_metrics,
    list_chat_message_media_urls_map,
    get_template_text,
    list_active_quick_replies,
    list_chat_folders,
    load_chat_messages,
    load_chat_threads,
    is_conversation_customer_blocked,
    pin_conversation_for_user,
    reorder_pins_for_user,
    mark_thread_unread,
    mark_thread_read,
    mark_conversation_messages_read_by_customer,
    _try_delete_unreferenced_media_file,
    handle_customer_event,
    handle_manager_message,
    backfill_chat_message_media_assets,
    cleanup_orphan_chat_message_media_links,
    backfill_quick_reply_media_assets,
    process_outbox_queue,
    run_storage_cleanup_for_all_workspaces,
    get_message_media_urls,
    get_quick_reply_media_paths,
    ensure_media_asset_for_path,
    sync_quick_reply_media_asset_links,
    remove_quick_reply_media_asset_link,
    remove_chat_message,
    retry_failed_outbox_message,
    replace_conversation_folder_links,
    set_template_text,
    send_admin_chat_message,
    send_admin_quick_reply,
    update_chat_message_text,
    queue_only_send_text,
    queue_only_send_media_group,
    get_conversation_by_id,
    unpin_conversation_for_user,
    DEFAULT_BUSINESS_TIMEZONE,
    DEFAULT_OFFHOURS_COOLDOWN_SECONDS,
    DEFAULT_OFFHOURS_MESSAGE,
    evaluate_workspace_business_hours,
    get_workspace_business_hours_payload,
    replace_workspace_business_hours,
)
from app.max_client import MaxClient
from app.email_utils import send_email_verification
from app.models import (
    AuditLog,
    BillingEvent,
    BotSettings,
    ChatMessage,
    ChatMessageMedia,
    ChatFolder,
    Conversation,
    ConversationFolderLink,
    ConversationPin,
    ConversationMeta,
    CustomerProfile,
    IntroStep,
    ManagerInvite,
    ManagerDispatch,
    MediaAsset,
    MessageLog,
    MessageTemplate,
    OutboxMessage,
    PlatformSettings,
    QuickReplyMediaAssetLink,
    QuickReply,
    QuickReplyMedia,
    ServiceUser,
    Subscription,
    TariffPlan,
    TenantAlert,
    UserSession,
    WebhookEvent,
    Workspace,
    WorkspaceBusinessException,
    WorkspaceBusinessHours,
    WorkspaceBusinessSlot,
    WorkspaceRetentionPolicy,
)
from app.ops import (
    apply_billing_hook,
    can_add_manager,
    collect_tenant_metrics,
    create_sqlite_backup,
    ensure_workspace_active_by_billing,
    ensure_workspace_limits_and_state,
    get_or_create_subscription,
    is_subscription_unlimited,
    list_backups,
    refresh_tenant_alerts,
    resolve_subscription_limits,
    delete_sqlite_backup,
    restore_sqlite_backup,
)
from app.security import InMemoryRateLimiter, is_safe_image, is_same_origin, verify_hmac_signature, safe_json_dumps
from app.schemas import MaxWebhookEvent
from app.storage import (
    ensure_storage_ready,
    save_upload_bytes,
    delete_by_public_url,
    get_storage_health,
    get_storage_health_snapshot,
)
from app.services import (
    DEFAULT_WORKSPACE_ID,
    create_service_user,
    create_workspace_with_owner,
    ensure_default_workspace,
    get_or_create_settings,
    list_workspace_managers,
)
from app.realtime import chat_realtime_hub
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
_SUPERADMIN_AUDIT_PAGE_SIZE = 50
_SUBSCRIPTION_STATUS_OPTIONS = {"active", "basic", "trial", "past_due", "paused", "cancelled"}
_WORKSPACE_USER_ROLES = {"user", "owner", "admin"}
_WORKSPACE_QUICK_REPLY_ROLES = _WORKSPACE_USER_ROLES | {"manager"}


def _require_superadmin(user: ServiceUser) -> None:
    if user.role != "superadmin":
        raise HTTPException(status_code=403, detail="Только для суперадмина")


def _to_iso(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _to_local(dt: datetime | None, fmt: str = "%d.%m.%Y %H:%M") -> str:
    if dt is None:
        return ""
    value = dt
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    if not isinstance(value, datetime):
        return ""
    local_value = value.replace(tzinfo=UTC).astimezone()
    try:
        return local_value.strftime(fmt)
    except Exception:
        return local_value.strftime("%d.%m.%Y %H:%M")


def _to_moscow_chat_label(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    value = dt
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    try:
        msk_tz = ZoneInfo("Europe/Moscow")
        msk_value = value.astimezone(msk_tz)
    except Exception:
        msk_value = value
    return msk_value.strftime("%H:%M | %d-%m-%Y")


def _safe_int(value: int | str | None, default: int, min_value: int = 0) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(min_value, parsed)


def _normalize_tariff_code(value: object) -> str:
    return _normalize_tariff_plan_code(value)


def _normalize_subscription_status(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"active", "trial", "past_due", "paused", "cancelled"}:
        return raw
    if raw == "basic":
        return "active"
    return "active"


def _tariff_plan_limits_dict(plan: TariffPlan | None) -> dict[str, int]:
    if plan is None:
        return {
            "manager_limit": 0,
            "dialogs_limit": 0,
            "messages_per_month_limit": 0,
            "quick_replies_limit": 0,
            "folders_limit": 0,
            "pinned_chats_limit": 0,
        }
    return {
        "manager_limit": max(1, int(getattr(plan, "manager_limit", 0) or 0)),
        "dialogs_limit": max(1, int(getattr(plan, "dialogs_limit", 0) or 0)),
        "messages_per_month_limit": max(1, int(getattr(plan, "messages_per_month_limit", 0) or 0)),
        "quick_replies_limit": max(1, int(getattr(plan, "quick_replies_limit", 0) or 0)),
        "folders_limit": max(1, int(getattr(plan, "folders_limit", 0) or 0)),
        "pinned_chats_limit": max(1, int(getattr(plan, "pinned_chats_limit", 0) or 0)),
    }


def _tariff_plan_display_name(plan: TariffPlan | None, *, fallback_code: str = "") -> str:
    if plan is not None and str(getattr(plan, "name", "") or "").strip():
        return str(getattr(plan, "name", "") or "").strip()
    code_value = str(getattr(plan, "code", "") or fallback_code or "").strip().lower()
    if code_value in {"basic", "trial"}:
        return "Начальный"
    if code_value == "unlimited":
        return "Безлимит"
    return code_value or "Тариф"


def _ensure_superadmin_tariff_baseline(db: Session) -> None:
    """Guarantee baseline plans/default and normalize legacy subscription statuses."""
    changed = False
    basic = db.query(TariffPlan).filter(TariffPlan.code == "basic").first()
    if basic is None:
        basic = TariffPlan(
            code="basic",
            name="Начальный",
            description="Базовый тариф по умолчанию",
            manager_limit=3,
            dialogs_limit=500,
            messages_per_month_limit=5000,
            quick_replies_limit=10,
            folders_limit=10,
            pinned_chats_limit=5,
            is_default=True,
            is_active=True,
            billing_product_code="",
            billing_price_code="",
        )
        db.add(basic)
        changed = True
    unlimited = db.query(TariffPlan).filter(TariffPlan.code == "unlimited").first()
    if unlimited is None:
        unlimited = TariffPlan(
            code="unlimited",
            name="Безлимит",
            description="Тариф без ограничений",
            manager_limit=1_000_000_000,
            dialogs_limit=1_000_000_000,
            messages_per_month_limit=1_000_000_000,
            quick_replies_limit=1_000_000_000,
            folders_limit=1_000_000_000,
            pinned_chats_limit=1_000_000_000,
            is_default=False,
            is_active=True,
            billing_product_code="",
            billing_price_code="",
        )
        db.add(unlimited)
        changed = True
    plans = db.query(TariffPlan).order_by(TariffPlan.id.asc()).all()
    defaults = [plan for plan in plans if bool(getattr(plan, "is_default", False))]
    if not defaults and basic is not None:
        basic.is_default = True
        db.add(basic)
        changed = True
    elif len(defaults) > 1:
        keep_id = int(defaults[0].id)
        for plan in defaults[1:]:
            if int(plan.id) == keep_id:
                continue
            if bool(plan.is_default):
                plan.is_default = False
                db.add(plan)
                changed = True
    subs = db.query(Subscription).all()
    for sub in subs:
        normalized_status = _normalize_subscription_status(getattr(sub, "status", "active"))
        if normalized_status != str(getattr(sub, "status", "") or "").strip().lower():
            sub.status = normalized_status
            db.add(sub)
            changed = True
        normalized_code = _normalize_tariff_code(getattr(sub, "plan_code", "basic"))
        if normalized_code != str(getattr(sub, "plan_code", "") or "").strip().lower():
            sub.plan_code = normalized_code
            db.add(sub)
            changed = True
    if changed:
        db.commit()


def _parse_folder_ids_form(raw: str) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for part in re.split(r"[,\s;]+", str(raw or "")):
        token = (part or "").strip()
        if not token:
            continue
        try:
            folder_id = int(token)
        except (TypeError, ValueError):
            continue
        if folder_id <= 0 or folder_id in seen:
            continue
        seen.add(folder_id)
        values.append(folder_id)
    return values


def _resolve_folder_ids_from_form(*, folder_ids_csv: str, folder_id: int) -> list[int]:
    parsed = _parse_folder_ids_form(folder_ids_csv)
    if parsed:
        return parsed
    fallback = int(folder_id or 0)
    return [fallback] if fallback > 0 else []


_BUSINESS_WEEKDAY_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_COMMON_TIMEZONES = [
    "UTC",
    "Europe/Moscow",
    "Europe/Kaliningrad",
    "Asia/Yekaterinburg",
    "Asia/Omsk",
    "Asia/Novosibirsk",
    "Asia/Krasnoyarsk",
    "Asia/Irkutsk",
    "Asia/Yakutsk",
    "Asia/Vladivostok",
    "Asia/Magadan",
    "Asia/Kamchatka",
    "Europe/Minsk",
    "Europe/Berlin",
    "Asia/Almaty",
    "Asia/Tashkent",
    "America/New_York",
]


def _truthy_form_value(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _parse_time_to_minute(value: object, *, default_minute: int) -> int:
    raw = str(value or "").strip()
    if not raw:
        return int(default_minute)
    parts = raw.split(":", 1)
    if len(parts) != 2:
        return int(default_minute)
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return int(default_minute)
    if hour < 0 or hour > 23:
        return int(default_minute)
    if minute < 0 or minute > 59:
        return int(default_minute)
    return hour * 60 + minute


def _minute_to_hhmm(value: int) -> str:
    minute = max(0, min(24 * 60, int(value)))
    if minute >= 24 * 60:
        return "24:00"
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _normalize_timezone_for_form(value: str) -> str:
    tz_name = (value or "").strip() or DEFAULT_BUSINESS_TIMEZONE
    try:
        ZoneInfo(tz_name)
        return tz_name
    except ZoneInfoNotFoundError:
        return DEFAULT_BUSINESS_TIMEZONE


def _business_hours_view_model(
    db: Session,
    *,
    workspace_id: int,
) -> dict[str, object]:
    payload = get_workspace_business_hours_payload(db, workspace_id=workspace_id)
    timezone_name = _normalize_timezone_for_form(str(payload.get("timezone") or ""))
    slot_rows = payload.get("slots", []) or []
    day_slots: dict[int, list[tuple[int, int]]] = {}
    for item in slot_rows:
        if not isinstance(item, dict):
            continue
        try:
            weekday = int(item.get("weekday", 0))
            start_minute = int(item.get("start_minute", 0))
            end_minute = int(item.get("end_minute", 0))
        except (TypeError, ValueError):
            continue
        if weekday < 0 or weekday > 6:
            continue
        if start_minute < 0 or end_minute > 1440 or end_minute <= start_minute:
            continue
        day_slots.setdefault(weekday, []).append((start_minute, end_minute))
    weekday_rows: list[dict[str, object]] = []
    for weekday in range(7):
        intervals = sorted(day_slots.get(weekday, []), key=lambda item: (item[0], item[1]))
        if intervals:
            start_minute = int(intervals[0][0])
            end_minute = int(intervals[-1][1])
            enabled = True
        else:
            start_minute = 9 * 60
            end_minute = 18 * 60
            enabled = False
        weekday_rows.append(
            {
                "weekday": weekday,
                "label": _BUSINESS_WEEKDAY_LABELS[weekday],
                "enabled": enabled,
                "start_hhmm": _minute_to_hhmm(start_minute),
                "end_hhmm": _minute_to_hhmm(end_minute),
            }
        )
    timezone_options = list(dict.fromkeys([timezone_name, *_COMMON_TIMEZONES]))
    return {
        "enabled": bool(payload.get("enabled", False)),
        "timezone": timezone_name,
        "offhours_message": str(payload.get("offhours_message") or DEFAULT_OFFHOURS_MESSAGE),
        "cooldown_minutes": max(1, int(int(payload.get("cooldown_seconds") or DEFAULT_OFFHOURS_COOLDOWN_SECONDS) / 60)),
        "weekdays": weekday_rows,
        "timezone_options": timezone_options,
    }


def _extract_business_hours_form(form_data: dict | object) -> dict[str, object]:
    enabled = _truthy_form_value(
        form_data.get("business_hours_enabled", "")
        if hasattr(form_data, "get")
        else ""
    )
    timezone_name = _normalize_timezone_for_form(
        str(form_data.get("business_timezone", "") if hasattr(form_data, "get") else "")
    )
    offhours_message = str(
        form_data.get("offhours_message", "") if hasattr(form_data, "get") else ""
    ).strip()[:2000]
    cooldown_minutes_raw = str(
        form_data.get("offhours_cooldown_minutes", "") if hasattr(form_data, "get") else ""
    ).strip()
    try:
        cooldown_minutes = int(cooldown_minutes_raw or "360")
    except (TypeError, ValueError):
        cooldown_minutes = 360
    cooldown_minutes = max(1, min(10080, cooldown_minutes))
    slots: list[dict[str, int]] = []
    for weekday in range(7):
        prefix = f"business_day_{weekday}"
        day_enabled = _truthy_form_value(
            form_data.get(f"{prefix}_enabled", "") if hasattr(form_data, "get") else ""
        )
        if not day_enabled:
            continue
        start_minute = _parse_time_to_minute(
            form_data.get(f"{prefix}_start", "09:00") if hasattr(form_data, "get") else "09:00",
            default_minute=9 * 60,
        )
        end_minute = _parse_time_to_minute(
            form_data.get(f"{prefix}_end", "18:00") if hasattr(form_data, "get") else "18:00",
            default_minute=18 * 60,
        )
        if end_minute <= start_minute:
            end_minute = min(24 * 60, start_minute + 60)
        slots.append(
            {
                "weekday": weekday,
                "start_minute": int(start_minute),
                "end_minute": int(end_minute),
            }
        )
    return {
        "enabled": enabled,
        "timezone": timezone_name,
        "offhours_message": offhours_message,
        "cooldown_seconds": int(cooldown_minutes * 60),
        "slots": slots,
    }


def _filter_threads_by_folder(threads: list[object], folder_id: int | None) -> list[object]:
    if folder_id is None:
        return threads
    if int(folder_id) > 0:
        return [
            item
            for item in threads
            if int(folder_id) in [int(v) for v in (getattr(item, "folder_ids", []) or [])]
        ]
    return [item for item in threads if not [int(v) for v in (getattr(item, "folder_ids", []) or [])]]


def _folder_ids_csv_for_conversation(
    db: Session,
    *,
    conversation_id: int,
    workspace_id: int,
) -> str:
    return ",".join(
        str(item)
        for item in get_conversation_folder_ids(
            db,
            conversation_id=conversation_id,
            workspace_id=workspace_id,
        )
    )


def _assign_new_folder_to_conversation(
    db: Session,
    *,
    conversation_id: int,
    new_folder_id: int,
    workspace_id: int,
) -> bool:
    current_ids = get_conversation_folder_ids(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )
    next_ids = list(current_ids)
    if int(new_folder_id) not in next_ids:
        next_ids.append(int(new_folder_id))
    return replace_conversation_folder_links(
        db=db,
        conversation_id=conversation_id,
        folder_ids=next_ids,
        workspace_id=workspace_id,
    )


def _replace_conversation_folders_and_redirect(
    *,
    db: Session,
    conversation_id: int,
    workspace_id: int,
    folder_ids_csv: str,
    folder_id: int,
) -> tuple[bool, str]:
    next_folder_ids = _resolve_folder_ids_from_form(
        folder_ids_csv=folder_ids_csv,
        folder_id=folder_id,
    )
    ok = replace_conversation_folder_links(
        db=db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=workspace_id,
    )
    multi_suffix = "1" if ok and len(next_folder_ids) > 1 else "0"
    return ok, multi_suffix


def _normalize_manager_ids(raw: str) -> str:
    tokens: list[str] = []
    seen: set[str] = set()
    for item in re.split(r"[,\n;]+", raw or ""):
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        tokens.append(value)
    return ",".join(tokens)


def _parse_manager_ids(raw: str) -> list[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _merge_manager_ids(existing_csv: str, candidate_id: str) -> list[str]:
    ids = _parse_manager_ids(existing_csv)
    target = (candidate_id or "").strip()
    if not target:
        return ids
    if target not in ids:
        ids.append(target)
    return ids


def _purge_default_workspace_manager_data(db: Session) -> int:
    """
    Default workspace belongs to the platform/system scope.
    It must not keep tenant managers or manager onboarding artifacts.
    """
    manager_ids = [
        row[0]
        for row in db.query(ServiceUser.id)
        .filter(
            ServiceUser.workspace_id == DEFAULT_WORKSPACE_ID,
            ServiceUser.role == "manager",
        )
        .all()
    ]
    changed = False
    removed = len(manager_ids)

    if manager_ids:
        db.query(UserSession).filter(UserSession.user_id.in_(manager_ids)).delete(synchronize_session=False)
        db.query(ManagerInvite).filter(ManagerInvite.used_by_user_id.in_(manager_ids)).update(
            {ManagerInvite.used_by_user_id: None},
            synchronize_session=False,
        )
        db.query(AuditLog).filter(AuditLog.actor_user_id.in_(manager_ids)).update(
            {AuditLog.actor_user_id: None},
            synchronize_session=False,
        )
        db.query(ServiceUser).filter(ServiceUser.id.in_(manager_ids)).delete(synchronize_session=False)
        changed = True

    deleted_invites = (
        db.query(ManagerInvite)
        .filter(ManagerInvite.workspace_id == DEFAULT_WORKSPACE_ID)
        .delete(synchronize_session=False)
    )
    deleted_dispatches = (
        db.query(ManagerDispatch)
        .filter(ManagerDispatch.workspace_id == DEFAULT_WORKSPACE_ID)
        .delete(synchronize_session=False)
    )
    deleted_manager_audits = (
        db.query(AuditLog)
        .filter(
            AuditLog.workspace_id == DEFAULT_WORKSPACE_ID,
            AuditLog.action.like("manager_%"),
        )
        .delete(synchronize_session=False)
    )
    if deleted_invites or deleted_dispatches or deleted_manager_audits:
        changed = True

    default_settings = (
        db.query(BotSettings)
        .filter(BotSettings.workspace_id == DEFAULT_WORKSPACE_ID)
        .first()
    )
    if default_settings is not None and (default_settings.manager_account_id or "").strip():
        default_settings.manager_account_id = ""
        db.add(default_settings)
        changed = True

    if changed:
        db.commit()
    return removed


def _safe_json_dict(raw: str) -> dict:
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _collect_manager_status_rows(
    db: Session,
    *,
    workspace_id: int,
    manager_ids: list[str],
) -> tuple[list[dict[str, str | bool]], dict[str, int]]:
    normalized_ids: list[str] = []
    seen_ids: set[str] = set()
    for raw_id in manager_ids:
        value = (raw_id or "").strip()
        if not value or value in seen_ids:
            continue
        seen_ids.add(value)
        normalized_ids.append(value)

    summary = {
        "connected": 0,
        "pending": 0,
        "failed": 0,
        "not_sent": 0,
        "deactivated": 0,
        "total": len(normalized_ids),
    }
    if not normalized_ids:
        return [], summary

    manager_rows = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.max_account_id.in_(normalized_ids),
        )
        .order_by(ServiceUser.id.desc())
        .all()
    )
    manager_by_max_id: dict[str, ServiceUser] = {}
    for row in manager_rows:
        max_id = (row.max_account_id or "").strip()
        if not max_id or max_id in manager_by_max_id:
            continue
        manager_by_max_id[max_id] = row

    invite_rows = (
        db.query(ManagerInvite)
        .filter(
            ManagerInvite.workspace_id == workspace_id,
            ManagerInvite.max_account_id.in_(normalized_ids),
        )
        .order_by(ManagerInvite.created_at.desc(), ManagerInvite.id.desc())
        .all()
    )
    latest_invite_by_max_id: dict[str, ManagerInvite] = {}
    connected_at_by_max_id: dict[str, datetime] = {}
    for invite in invite_rows:
        max_id = (invite.max_account_id or "").strip()
        if not max_id:
            continue
        if max_id not in latest_invite_by_max_id:
            latest_invite_by_max_id[max_id] = invite
        if invite.used_at:
            existing_connected_at = connected_at_by_max_id.get(max_id)
            if existing_connected_at is None or invite.used_at > existing_connected_at:
                connected_at_by_max_id[max_id] = invite.used_at

    audit_rows = (
        db.query(AuditLog)
        .filter(
            AuditLog.workspace_id == workspace_id,
            AuditLog.action.in_(["manager_invite_sent", "manager_invite_send_failed"]),
        )
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(5000)
        .all()
    )
    last_sent_at_by_max_id: dict[str, datetime] = {}
    last_failed_at_by_max_id: dict[str, datetime] = {}
    for log_row in audit_rows:
        details = _safe_json_dict(log_row.details_json)
        max_id = str(details.get("max_account_id", "")).strip()
        if not max_id or max_id not in seen_ids:
            continue
        if log_row.action == "manager_invite_sent" and max_id not in last_sent_at_by_max_id:
            last_sent_at_by_max_id[max_id] = log_row.created_at
        if log_row.action == "manager_invite_send_failed" and max_id not in last_failed_at_by_max_id:
            last_failed_at_by_max_id[max_id] = log_row.created_at

    status_labels = {
        "connected": "Подключен",
        "pending": "Ожидает подключения",
        "failed": "Ошибка отправки",
        "not_sent": "Ссылка не отправлялась",
        "deactivated": "Отключен",
    }
    status_rows: list[dict[str, str | bool]] = []
    for max_id in normalized_ids:
        manager_user = manager_by_max_id.get(max_id)
        invite_row = latest_invite_by_max_id.get(max_id)
        last_login_at = manager_user.last_login_at if manager_user else None
        connected_at = connected_at_by_max_id.get(max_id)
        if last_login_at and (connected_at is None or last_login_at > connected_at):
            connected_at = last_login_at
        last_sent_at = last_sent_at_by_max_id.get(max_id)
        if last_sent_at is None and invite_row is not None:
            last_sent_at = invite_row.created_at
        last_failed_at = last_failed_at_by_max_id.get(max_id)

        status_key = "not_sent"
        if manager_user is not None and (not manager_user.is_active or manager_user.is_blocked):
            status_key = "deactivated"
        elif connected_at is not None:
            status_key = "connected"
        elif last_failed_at is not None and (last_sent_at is None or last_failed_at >= last_sent_at):
            status_key = "failed"
        elif last_sent_at is not None:
            status_key = "pending"

        summary[status_key] += 1
        status_rows.append(
            {
                "max_account_id": max_id,
                "status_key": status_key,
                "status_label": status_labels.get(status_key, "—"),
                "link_sent_at": _to_iso(last_sent_at),
                "connected_at": _to_iso(connected_at),
                "last_login_at": _to_iso(last_login_at),
                "can_delete_chats": bool(getattr(manager_user, "can_delete_chats", False)),
                "has_service_user": manager_user is not None,
            }
        )

    return status_rows, summary


def _build_usage_context(
    db: Session,
    *,
    subscription: Subscription | None,
    tenant_metrics: dict[str, int] | None,
    manager_status_summary: dict[str, int] | None,
) -> tuple[dict[str, int], dict[str, int], dict[str, str]]:
    sub = subscription
    limits = resolve_subscription_limits(db, subscription=sub) if sub is not None else {}
    metrics = tenant_metrics or {}
    manager_summary = manager_status_summary or {}
    limit_managers = max(0, int(limits.get("manager_limit", 0) or 0))
    limit_dialogs = max(0, int(limits.get("dialogs_limit", 0) or 0))
    limit_messages = max(0, int(limits.get("messages_per_month_limit", 0) or 0))
    limit_quick = max(0, int(limits.get("quick_replies_limit", 0) or 0))
    limit_folders = max(0, int(limits.get("folders_limit", 0) or 0))
    limit_pins = max(0, int(limits.get("pinned_chats_limit", 0) or 0))
    usage_limits = {
        "managers": limit_managers,
        "dialogs": limit_dialogs,
        "messages_month": limit_messages,
        "quick_replies": limit_quick,
        "folders": limit_folders,
        "pinned_chats": limit_pins,
    }
    usage_used = {
        "managers": max(0, int(manager_summary.get("connected", metrics.get("managers_active", 0)) or 0)),
        "dialogs": max(0, int(metrics.get("dialogs_total", 0) or 0)),
        "messages_month": max(0, int(metrics.get("messages_month", 0) or 0)),
        "quick_replies": max(0, int(metrics.get("quick_replies_total", 0) or 0)),
        "folders": max(0, int(metrics.get("folders_total", 0) or 0)),
        "pinned_chats": max(0, int(metrics.get("pins_total", 0) or 0)),
    }
    usage_remaining: dict[str, str] = {}
    for key, limit_value in usage_limits.items():
        used_value = usage_used.get(key, 0)
        if limit_value >= 1_000_000_000:
            usage_remaining[key] = "∞"
        else:
            usage_remaining[key] = str(max(0, int(limit_value) - int(used_value)))
    return usage_limits, usage_used, usage_remaining


def _user_can_delete_chats(user: ServiceUser | None) -> bool:
    if user is None:
        return False
    role_value = str(getattr(user, "role", "") or "").strip().lower()
    if role_value in _WORKSPACE_USER_ROLES or role_value == "superadmin":
        return True
    return bool(getattr(user, "can_delete_chats", False))


def _ensure_user_can_delete_chats(user: ServiceUser | None) -> None:
    if _user_can_delete_chats(user):
        return
    raise HTTPException(status_code=403, detail="Удаление чатов и сообщений запрещено")


def _ensure_manager_can_delete_chats(
    db: Session,
    *,
    workspace_id: int,
    manager_user_id: int,
) -> ServiceUser:
    manager = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.id == int(manager_user_id),
            ServiceUser.workspace_id == int(workspace_id),
            ServiceUser.role == "manager",
            ServiceUser.is_active.is_(True),
            ServiceUser.is_blocked.is_(False),
        )
        .first()
    )
    if manager is None:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    _ensure_user_can_delete_chats(manager)
    return manager


def _parse_conversation_ids_from_payload(payload: object) -> list[int]:
    ids_raw = payload.get("conversation_ids") if isinstance(payload, dict) else None
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ordered_ids: list[int] = []
    seen: set[int] = set()
    for item in ids_raw:
        try:
            conv_id = int(item)
        except (TypeError, ValueError):
            continue
        if conv_id <= 0 or conv_id in seen:
            continue
        seen.add(conv_id)
        ordered_ids.append(conv_id)
    if not ordered_ids:
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    return ordered_ids


def _parse_folder_ids_from_payload(payload: object) -> list[int]:
    if not isinstance(payload, dict):
        return []
    normalized_ids: list[int] = []
    seen: set[int] = set()
    raw_ids = payload.get("folder_ids")
    if isinstance(raw_ids, list):
        iterable: list[object] = raw_ids
    elif isinstance(raw_ids, str):
        iterable = [item.strip() for item in re.split(r"[,\s;]+", raw_ids) if str(item).strip()]
    else:
        iterable = []
    for item in iterable:
        try:
            folder_id = int(item)
        except (TypeError, ValueError):
            continue
        if folder_id <= 0 or folder_id in seen:
            continue
        seen.add(folder_id)
        normalized_ids.append(folder_id)
    if normalized_ids:
        return normalized_ids
    try:
        fallback_folder_id = int(payload.get("folder_id") or 0)
    except (TypeError, ValueError):
        fallback_folder_id = 0
    return [fallback_folder_id] if fallback_folder_id > 0 else []


def _parse_optional_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


async def _parse_bulk_action_request_payload(request: Request) -> dict[str, object]:
    payload: dict[str, object] = {}
    content_type = str(request.headers.get("content-type") or "").strip().lower()
    if "application/json" in content_type:
        with suppress(Exception):
            parsed = await request.json()
            if isinstance(parsed, dict):
                payload = dict(parsed)
    if payload:
        return payload

    form = None
    with suppress(Exception):
        form = await request.form()
    if not form:
        return {}

    conversation_ids: list[object] = []
    for raw in form.getlist("conversation_ids"):
        text = str(raw or "").strip()
        if not text:
            continue
        if any(char in text for char in [",", ";", " "]):
            conversation_ids.extend([item for item in re.split(r"[,\s;]+", text) if item])
        else:
            conversation_ids.append(text)
    if not conversation_ids:
        csv_fallback = str(form.get("conversation_ids_csv") or "").strip()
        if csv_fallback:
            conversation_ids = [item for item in re.split(r"[,\s;]+", csv_fallback) if item]

    return {
        "action": str(form.get("action") or "").strip(),
        "conversation_ids": conversation_ids,
        "folder_ids": form.get("folder_ids"),
        "folder_id": form.get("folder_id"),
        "current_folder_id": form.get("current_folder_id"),
        "q": str(form.get("q") or "").strip(),
        "view": str(form.get("view") or "").strip(),
    }


def _bulk_operation_redirect_url(
    *,
    base_path: str,
    q: str,
    view: str,
    folder_id: int | None = None,
    workspace_qs: str = "",
    deleted_total: int = 0,
    moved_total: int = 0,
    moved_multi: bool = False,
) -> str:
    url = f"{base_path}?q={quote_plus(q.strip())}&view={quote_plus(view.strip())}"
    if folder_id is not None:
        url += f"&folder_id={int(folder_id)}"
    if moved_total > 0:
        url += "&foldered=1"
        if moved_multi:
            url += "&foldered_multi=1"
        url += f"&bulk_moved={int(moved_total)}"
    if deleted_total > 0:
        url += "&removed=1"
        url += f"&bulk_removed={int(deleted_total)}"
    elif moved_total <= 0:
        url += "&removed=0"
    if workspace_qs:
        url += workspace_qs
    return url


def _manager_routing_mode_label(mode: str) -> str:
    normalized = (mode or "round_robin").strip().lower()
    if normalized == "random":
        return "Случайно"
    return "По очереди"


def _normalize_bot_link(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("max.ru/"):
        return f"https://{raw}"
    return f"https://max.ru/{raw.lstrip('/')}"


def _is_valid_bot_link(value: str) -> bool:
    link = (value or "").strip().lower()
    return bool(re.fullmatch(r"https://max\.ru/id\d+_bot", link))


def _ensure_workspace_webhook_key(settings_row: BotSettings) -> str:
    current = (settings_row.webhook_key or "").strip()
    if current:
        return current
    settings_row.webhook_key = secrets.token_urlsafe(18).replace("-", "").replace("_", "")[:32]
    return settings_row.webhook_key


def _workspace_webhook_url(settings_row: BotSettings) -> str:
    key = _ensure_workspace_webhook_key(settings_row)
    base = settings.public_base_url.rstrip("/")
    return f"{base}{webhook_path}/{key}"


def _workspace_webhook_url_by_key(webhook_key: str) -> str:
    key = (webhook_key or "").strip()
    if not key:
        return "—"
    base = settings.public_base_url.rstrip("/")
    return f"{base}{webhook_path}/{key}"


def _read_flow_log(stage: str, **kwargs: object) -> None:
    try:
        details = ", ".join(f"{key}={repr(value)}" for key, value in kwargs.items())
    except Exception:
        details = ""
    logger = logging.getLogger(__name__)
    if details:
        logger.info("[READ_FLOW] %s: %s", stage, details)
    else:
        logger.info("[READ_FLOW] %s", stage)


def _ws_hint_log(stage: str, **kwargs: object) -> None:
    try:
        details = ", ".join(f"{key}={repr(value)}" for key, value in kwargs.items())
    except Exception:
        details = ""
    logger = logging.getLogger(__name__)
    if details:
        logger.info("[WS_HINT] %s: %s", stage, details)
    else:
        logger.info("[WS_HINT] %s", stage)


async def _try_mark_chat_seen(
    *,
    client: MaxClient,
    chat_id: str,
    workspace_id: int,
    sender_id: str = "",
    update_type: str = "",
    message_mid: str | None = None,
    event_uid: str | None = None,
) -> None:
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        _read_flow_log(
            "mark_seen_skipped",
            reason="empty_chat_id",
            workspace_id=workspace_id,
            sender_id=sender_id,
            update_type=update_type,
        )
        return
    result = await client.send_chat_action(chat_id=chat_value, action="mark_seen")
    ok = bool(
        result.get("success", True)
        or result.get("ok")
        or result.get("result")
        or result.get("mock")
    )
    if ok:
        _read_flow_log(
            "mark_seen_sent",
            workspace_id=workspace_id,
            chat_id=chat_value,
            sender_id=sender_id,
            update_type=update_type,
            message_mid=str(message_mid or ""),
            event_uid=str(event_uid or ""),
        )
        return
    _read_flow_log(
        "mark_seen_failed",
        workspace_id=workspace_id,
        chat_id=chat_value,
        sender_id=sender_id,
        update_type=update_type,
        message_mid=str(message_mid or ""),
        event_uid=str(event_uid or ""),
        status_code=result.get("status_code"),
        error=_extract_max_error_message(result) or result.get("error") or result.get("message"),
    )


async def _try_mark_chat_seen_from_manager_read(
    *,
    db: Session,
    workspace_id: int,
    conversation_id: int,
    chat_id: str,
    sender_id: str = "",
) -> None:
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        _read_flow_log(
            "mark_seen_skipped",
            reason="chat_id_missing_on_manager_read",
            workspace_id=workspace_id,
            conversation_id=conversation_id,
        )
        return
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    client, client_error = _workspace_client_or_error(settings_row)
    if client is None:
        _read_flow_log(
            "mark_seen_skipped",
            reason="client_unavailable",
            workspace_id=workspace_id,
            conversation_id=conversation_id,
            error=client_error or "",
        )
        return
    await _try_mark_chat_seen(
        client=client,
        chat_id=chat_value,
        workspace_id=workspace_id,
        sender_id=sender_id,
        update_type="manager_mark_read",
        message_mid=None,
        event_uid=None,
    )


def _user_friendly_bot_connection_error() -> str:
    return "Подключение бота требует проверки, обратитесь в поддержку"


def _workspace_client_or_error(settings_row: BotSettings) -> tuple[MaxClient | None, str | None]:
    token = (settings_row.bot_token or "").strip()
    if not token:
        return None, "Укажите токен вашего бота Max в настройках."
    return MaxClient(token=token), None


def _extract_max_error_message(result: dict) -> str:
    response_payload = result.get("response")
    if isinstance(response_payload, dict):
        for key in ("message", "error", "detail", "description"):
            value = response_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    details = result.get("details")
    if isinstance(details, str) and details.strip():
        return details.strip()
    return ""


async def _auto_subscribe_workspace_webhook(settings_row: BotSettings) -> tuple[bool, str | None]:
    client, client_error = _workspace_client_or_error(settings_row)
    if client is None:
        return False, client_error or "Укажите токен вашего бота Max в настройках."

    webhook_workspace_url = _workspace_webhook_url(settings_row)
    public_url = urlsplit(webhook_workspace_url)
    host = (public_url.hostname or "").lower()
    if public_url.scheme != "https" or host in {"localhost", "127.0.0.1", "::1"}:
        # In local/dev environments we skip remote subscribe and keep save flow intact.
        return True, None
    webhook_secret_value = (settings.webhook_secret or "").strip() or None
    subscribe_result = await client.subscribe_webhook(
        url=webhook_workspace_url,
        update_types=[
            "message_created",
            "message_callback",
            "bot_started",
        ],
        secret=webhook_secret_value,
    )
    success = bool(
        subscribe_result.get("success", True)
        or subscribe_result.get("ok")
        or subscribe_result.get("result")
        or subscribe_result.get("mock")
    )
    if success:
        return True, None

    status_code = subscribe_result.get("status_code")
    error_details = _extract_max_error_message(subscribe_result)
    if status_code == 401:
        return False, "Неверный токен бота Max. Проверьте токен и сохраните снова."
    if status_code == 403:
        return False, "Нет доступа к API Max по указанному токену."
    if status_code in {400, 404, 422}:
        suffix = f" ({error_details})" if error_details else ""
        return False, f"Не удалось автоматически подключить webhook в Max{suffix}"
    suffix = f" ({error_details})" if error_details else ""
    return False, f"Не удалось автоматически подключить webhook в Max. Код: {status_code or 'unknown'}{suffix}"


async def _auto_unsubscribe_workspace_webhook(settings_row: BotSettings) -> tuple[bool, str | None]:
    client, client_error = _workspace_client_or_error(settings_row)
    if client is None:
        return True, None

    webhook_workspace_url = _workspace_webhook_url(settings_row)
    public_url = urlsplit(webhook_workspace_url)
    host = (public_url.hostname or "").lower()
    if public_url.scheme != "https" or host in {"localhost", "127.0.0.1", "::1"}:
        # In local/dev environments we skip remote unsubscribe and keep delete flow intact.
        return True, None

    unsubscribe_result = await client.unsubscribe_webhook(url=webhook_workspace_url)
    success = bool(
        unsubscribe_result.get("success", True)
        or unsubscribe_result.get("ok")
        or unsubscribe_result.get("result")
        or unsubscribe_result.get("mock")
    )
    if success:
        return True, None

    status_code = unsubscribe_result.get("status_code")
    # 404 means there is no active subscription for this URL. Treat as success.
    if status_code == 404:
        return True, None
    error_details = _extract_max_error_message(unsubscribe_result)
    suffix = f" ({error_details})" if error_details else ""
    return False, f"Не удалось отключить webhook в Max. Код: {status_code or 'unknown'}{suffix}"


def _localized_alert_entry(alert: TenantAlert) -> dict[str, str]:
    severity_labels = {
        "critical": "Критично",
        "warning": "Предупреждение",
        "info": "Информация",
    }
    key_labels = {
        "managers_limit": "Лимит менеджеров",
        "dialogs_limit": "Лимит диалогов",
        "messages_month_limit": "Лимит сообщений в месяц",
        "quick_replies_limit": "Лимит быстрых ответов",
        "folders_limit": "Лимит папок",
        "pinned_chats_limit": "Лимит закрепленных чатов",
    }
    alert_key = (alert.alert_key or "").strip()
    key_label = key_labels.get(alert_key, alert_key or "Алерт")
    raw_message = (alert.message or "").strip()
    suffix = raw_message
    prefix = f"{alert_key}:"
    if alert_key and raw_message.startswith(prefix):
        suffix = raw_message[len(prefix) :].strip()
    message_ru = f"{key_label}: {suffix}" if suffix else key_label
    return {
        "severity_label": severity_labels.get((alert.severity or "").strip().lower(), "Алерт"),
        "message": message_ru,
    }


_SUPERADMIN_AUDIT_PAGE_SIZE = 50


def _normalize_tariff_plan_code(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw == "trial":
        return "basic"
    normalized = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-._")
    return normalized or "basic"


def _default_tariff_limits() -> dict[str, int]:
    return {
        "manager_limit": 3,
        "dialogs_limit": 500,
        "messages_per_month_limit": 5000,
        "quick_replies_limit": 10,
        "folders_limit": 10,
        "pinned_chats_limit": 5,
    }


def _tariff_plan_limits_dict(plan: TariffPlan | None) -> dict[str, int]:
    defaults = _default_tariff_limits()
    if plan is None:
        return defaults
    return {
        "manager_limit": max(1, int(getattr(plan, "manager_limit", defaults["manager_limit"]) or defaults["manager_limit"])),
        "dialogs_limit": max(1, int(getattr(plan, "dialogs_limit", defaults["dialogs_limit"]) or defaults["dialogs_limit"])),
        "messages_per_month_limit": max(
            1,
            int(
                getattr(plan, "messages_per_month_limit", defaults["messages_per_month_limit"])
                or defaults["messages_per_month_limit"]
            ),
        ),
        "quick_replies_limit": max(
            1,
            int(getattr(plan, "quick_replies_limit", defaults["quick_replies_limit"]) or defaults["quick_replies_limit"]),
        ),
        "folders_limit": max(1, int(getattr(plan, "folders_limit", defaults["folders_limit"]) or defaults["folders_limit"])),
        "pinned_chats_limit": max(
            1,
            int(getattr(plan, "pinned_chats_limit", defaults["pinned_chats_limit"]) or defaults["pinned_chats_limit"]),
        ),
    }


def _get_tariff_plan_by_code(db: Session, *, plan_code: object) -> TariffPlan | None:
    code = _normalize_tariff_plan_code(plan_code)
    return db.query(TariffPlan).filter(TariffPlan.code == code).first()


def _get_or_create_default_tariff_plan(db: Session) -> TariffPlan:
    default_plan = (
        db.query(TariffPlan)
        .filter(TariffPlan.is_default.is_(True), TariffPlan.is_active.is_(True))
        .order_by(TariffPlan.id.asc())
        .first()
    )
    if default_plan is not None:
        return default_plan
    basic_plan = _get_tariff_plan_by_code(db, plan_code="basic")
    if basic_plan is not None:
        basic_plan.is_default = True
        basic_plan.is_active = True
        db.add(basic_plan)
        db.commit()
        db.refresh(basic_plan)
        return basic_plan
    limits = _default_tariff_limits()
    created = TariffPlan(
        code="basic",
        name="Начальный",
        description="Базовый тариф по умолчанию",
        manager_limit=limits["manager_limit"],
        dialogs_limit=limits["dialogs_limit"],
        messages_per_month_limit=limits["messages_per_month_limit"],
        quick_replies_limit=limits["quick_replies_limit"],
        folders_limit=limits["folders_limit"],
        pinned_chats_limit=limits["pinned_chats_limit"],
        is_default=True,
        is_active=True,
        billing_product_code="",
        billing_price_code="",
    )
    db.add(created)
    db.commit()
    db.refresh(created)
    return created


def _sanitize_tariff_plan_form(
    *,
    code: object,
    name: object,
    description: object,
    manager_limit: object,
    dialogs_limit: object,
    messages_per_month_limit: object,
    quick_replies_limit: object,
    folders_limit: object,
    pinned_chats_limit: object,
    billing_product_code: object,
    billing_price_code: object,
    fallback_code: str = "basic",
) -> dict[str, object]:
    normalized_code = _normalize_tariff_plan_code(code or fallback_code)
    normalized_name = str(name or "").strip()[:255]
    if not normalized_name:
        normalized_name = normalized_code
    payload = {
        "code": normalized_code,
        "name": normalized_name,
        "description": str(description or "").strip()[:3000],
        "manager_limit": max(1, _safe_int(manager_limit, 3)),
        "dialogs_limit": max(1, _safe_int(dialogs_limit, 500)),
        "messages_per_month_limit": max(1, _safe_int(messages_per_month_limit, 5000)),
        "quick_replies_limit": max(1, _safe_int(quick_replies_limit, 10)),
        "folders_limit": max(1, _safe_int(folders_limit, 10)),
        "pinned_chats_limit": max(1, _safe_int(pinned_chats_limit, 5)),
        "billing_product_code": str(billing_product_code or "").strip()[:128],
        "billing_price_code": str(billing_price_code or "").strip()[:128],
    }
    return payload


def _sync_subscription_cached_limits_from_plan(db: Session, *, subscription: Subscription) -> None:
    limits = resolve_subscription_limits(db, subscription=subscription)
    subscription.manager_limit = int(limits.get("manager_limit", 0) or 0)
    subscription.dialogs_limit = int(limits.get("dialogs_limit", 0) or 0)
    subscription.messages_per_month_limit = int(limits.get("messages_per_month_limit", 0) or 0)
    subscription.quick_replies_limit = int(limits.get("quick_replies_limit", 0) or 0)
    subscription.folders_limit = int(limits.get("folders_limit", 0) or 0)
    subscription.pinned_chats_limit = int(limits.get("pinned_chats_limit", 0) or 0)


def _build_audit_pagination(
    *,
    total_count: int,
    requested_page: object,
    page_size: int = _SUPERADMIN_AUDIT_PAGE_SIZE,
) -> dict[str, int | bool]:
    safe_total = max(0, int(total_count or 0))
    safe_size = max(1, int(page_size or _SUPERADMIN_AUDIT_PAGE_SIZE))
    pages_total = max(1, int((safe_total + safe_size - 1) / safe_size))
    page = _safe_int(requested_page if requested_page is not None else 1, 1, min_value=1)
    page = min(page, pages_total)
    offset = (page - 1) * safe_size
    return {
        "page": page,
        "page_size": safe_size,
        "pages_total": pages_total,
        "offset": offset,
        "has_prev": page > 1,
        "has_next": page < pages_total,
        "prev_page": (page - 1) if page > 1 else 1,
        "next_page": (page + 1) if page < pages_total else pages_total,
        "total_count": safe_total,
    }


def _deactivate_workspace_managers_by_max_ids(
    db: Session,
    *,
    workspace_id: int,
    manager_max_ids: list[str],
    actor_user_id: int | None,
) -> list[int]:
    ids_to_remove = [item.strip() for item in manager_max_ids if item and item.strip()]
    if not ids_to_remove:
        return []
    unique_ids = list(dict.fromkeys(ids_to_remove))
    manager_rows = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.max_account_id.in_(unique_ids),
        )
        .all()
    )
    revoked_user_ids: list[int] = []
    for row in manager_rows:
        row.is_active = False
        row.is_blocked = True
        db.add(row)
        revoked_user_ids.append(int(row.id))
    db.query(ManagerInvite).filter(
        ManagerInvite.workspace_id == workspace_id,
        ManagerInvite.max_account_id.in_(unique_ids),
    ).delete(synchronize_session=False)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action="manager_removed",
            object_type="service_user",
            object_id=",".join(str(item) for item in revoked_user_ids) if revoked_user_ids else ",".join(unique_ids),
            details_json=safe_json_dumps(
                {
                    "max_account_ids": unique_ids,
                    "revoked_user_ids": revoked_user_ids,
                    "source": "settings_sync",
                }
            ),
        )
    )
    db.commit()
    for user_id in revoked_user_ids:
        revoke_user_sessions(db, user_id=user_id)
    return revoked_user_ids


def _manager_limit_for_workspace(db: Session, *, workspace_id: int) -> int:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    return max(1, int(limits.get("manager_limit", 0) or 0))


def _quick_reply_limit_for_workspace(db: Session, *, workspace_id: int) -> int:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    return max(1, int(limits.get("quick_replies_limit", 0) or 0))


def _folder_limit_for_workspace(db: Session, *, workspace_id: int) -> int:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    return max(1, int(limits.get("folders_limit", 0) or 0))


def _is_unlimited_plan(db: Session, *, workspace_id: int) -> bool:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    return bool(is_subscription_unlimited(db, subscription=sub))


def _pinned_chats_limit_for_workspace(db: Session, *, workspace_id: int) -> int:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    return max(1, int(limits.get("pinned_chats_limit", 0) or 0))


def _chat_scope_service_user_id(
    *,
    current_user: ServiceUser | None = None,
    manager_claims: dict | None = None,
) -> int:
    if current_user is not None and int(current_user.id or 0) > 0:
        return int(current_user.id)
    if manager_claims:
        manager_user_id = manager_claims.get("service_user_id")
        if isinstance(manager_user_id, int) and manager_user_id > 0:
            return int(manager_user_id)
    return 0


def _chat_redirect_with_scope(
    *,
    base_path: str,
    conversation_id: int,
    q: str,
    view: str,
    folder_id: int | None = None,
    params: dict[str, str] | None = None,
    workspace_suffix: str = "",
) -> str:
    query_parts = [
        f"conversation_id={conversation_id}",
        f"q={q}",
        f"view={view}",
    ]
    if params:
        for key, value in params.items():
            query_parts.append(f"{key}={value}")
    if folder_id is not None:
        query_parts.append(f"folder_id={folder_id}")
    query = "&".join(query_parts)
    return f"{base_path}?{query}{workspace_suffix}"


def _limit_input_value(limit_value: int, *, is_unlimited: bool) -> str:
    if is_unlimited:
        return "∞"
    return str(int(limit_value))


def _extract_manager_ids_from_form(form_data: object) -> list[str]:
    get = getattr(form_data, "get", None)
    getlist = getattr(form_data, "getlist", None)
    values: list[str] = []
    if callable(getlist):
        for value in getlist("manager_account_ids"):
            values.append(str(value or ""))
    if callable(get):
        # Backward compatibility for old form fields.
        values.append(str(get("manager_account_id", "") or ""))
        values.append(str(get("manager_account_id_2", "") or ""))
    return _parse_manager_ids(_normalize_manager_ids(",".join(values)))


def _manager_rows_limit(db: Session, *, workspace_id: int) -> int:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    return max(1, int(limits.get("manager_limit", 0) or 0))


def _safe_media_send_diagnostics(
    db: Session,
    *,
    workspace_id: int,
) -> dict[str, int | float]:
    """
    Keep settings/dashboard pages resilient if a partially migrated DB
    misses optional idempotency columns used by diagnostics queries.
    """
    try:
        return get_media_diagnostics_metrics(db, workspace_id=workspace_id).__dict__
    except Exception:
        return {
            "window_minutes": 30,
            "total_outbox": 0,
            "sent_outbox": 0,
            "failed_outbox": 0,
            "deduped_outbox": 0,
            "send_message_total": 0,
            "send_message_sent": 0,
            "send_message_failed": 0,
            "media_send_total": 0,
            "media_send_sent": 0,
            "media_send_failed": 0,
            "fallback_markers": 0,
            "proto_payload_errors": 0,
            "upload_token_missing_errors": 0,
            "send_success_rate": 0.0,
        }


def _is_system_workspace(workspace_id: int) -> bool:
    return int(workspace_id) == int(DEFAULT_WORKSPACE_ID)


def _purge_workspace_manager_data(db: Session, *, workspace_id: int) -> None:
    manager_rows = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
        )
        .all()
    )
    manager_user_ids = [int(row.id) for row in manager_rows]
    if manager_user_ids:
        db.query(UserSession).filter(UserSession.user_id.in_(manager_user_ids)).delete(synchronize_session=False)
    db.query(ManagerInvite).filter(ManagerInvite.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ServiceUser).filter(
        ServiceUser.workspace_id == workspace_id,
        ServiceUser.role == "manager",
    ).delete(synchronize_session=False)
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    if (settings_row.manager_account_id or "").strip():
        settings_row.manager_account_id = ""
        db.add(settings_row)
    db.commit()


def _build_manager_username(db: Session, *, workspace_id: int, manager_id: str) -> str:
    seed = re.sub(r"[^a-z0-9_.-]+", "_", (manager_id or "").strip().lower()).strip("._-")
    if not seed:
        seed = "manager"
    base = f"mgr_{workspace_id}_{seed}"
    base = base[:64]
    candidate = base
    suffix = 2
    while db.query(ServiceUser.id).filter(ServiceUser.username == candidate).first():
        tail = f"_{suffix}"
        candidate = f"{base[: max(1, 64 - len(tail))]}{tail}"
        suffix += 1
    return candidate


def _get_or_create_manager_by_max_id(
    db: Session,
    *,
    workspace_id: int,
    manager_id: str,
    actor_user_id: int | None,
) -> ServiceUser:
    if workspace_id == DEFAULT_WORKSPACE_ID:
        raise ValueError("managers_disabled_for_system_workspace")
    manager = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.max_account_id == manager_id,
        )
        .first()
    )
    if manager is not None:
        # Manager removed from settings should be addable again by the same Max ID.
        if not manager.is_active or manager.is_blocked:
            manager.is_active = True
            manager.is_blocked = False
            db.add(manager)
            db.add(
                AuditLog(
                    workspace_id=workspace_id,
                    actor_user_id=actor_user_id,
                    action="manager_reactivated",
                    object_type="service_user",
                    object_id=str(manager.id),
                    details_json=safe_json_dumps(
                        {
                            "max_account_id": manager_id,
                            "source": "settings_invite_reuse",
                        }
                    ),
                )
            )
            db.commit()
            db.refresh(manager)
        return manager

    can_add, _ = can_add_manager(db, workspace_id=workspace_id)
    if not can_add:
        raise ValueError("manager_limit_exceeded")

    username = _build_manager_username(db, workspace_id=workspace_id, manager_id=manager_id)
    manager = create_service_user(
        db,
        username=username,
        password=f"InviteOnly#{uuid4().hex[:10]}",
        role="manager",
        workspace_id=workspace_id,
        display_name=f"Менеджер {manager_id}",
        max_account_id=manager_id,
    )
    # Manager invite flow must require explicit password setup from invite page.
    # Keep password empty here so opening the link never auto-consumes it.
    manager.password_hash = ""
    db.add(manager)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action="manager_created",
            object_type="service_user",
            object_id=str(manager.id),
            details_json=f'{{"username":"{manager.username}","max_account_id":"{manager_id}","source":"settings_send_link"}}',
        )
    )
    db.commit()
    return manager


async def _send_manager_invite_link_to_max(
    db: Session,
    *,
    workspace_id: int,
    manager_id: str,
    actor_user_id: int | None,
) -> tuple[bool, str]:
    manager_id_clean = (manager_id or "").strip()
    if not manager_id_clean:
        return False, "Укажите ID менеджера."
    if not manager_id_clean.isdigit():
        return False, "Указан некорректный ID менеджера в Max."

    ok_ws, _ = ensure_workspace_limits_and_state(db, workspace_id=workspace_id)
    if not ok_ws:
        return False, "Workspace приостановлен из-за биллинга."

    try:
        manager_user = _get_or_create_manager_by_max_id(
            db,
            workspace_id=workspace_id,
            manager_id=manager_id_clean,
            actor_user_id=actor_user_id,
        )
    except ValueError as exc:
        if str(exc) == "manager_limit_exceeded":
            return False, "Ваш тарифный план не позволяет добавлять больше менеджеров."
        if str(exc) == "managers_disabled_for_system_workspace":
            return False, "Для системного workspace создание менеджеров отключено."
        return False, "Менеджер не активен или заблокирован."

    links = _build_manager_invite_links(
        db=db,
        manager=manager_user,
        workspace_id=workspace_id,
        base_url=settings.public_base_url.rstrip("/"),
    )
    message_text = (
        "Вас пригласили в FeedPilot.\n"
        f"Ссылка для входа: {links['invite_link']}\n"
        f"Mini-app: {links['mini_link']}"
    )
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    client, client_error = _workspace_client_or_error(settings_row)
    if client is None:
        return False, client_error or "Укажите токен вашего бота Max в настройках."
    send_result = await client.send_text_to_user(user_id=manager_id_clean, text=message_text)
    sent_ok = bool(send_result.get("success", True) or send_result.get("message") or send_result.get("mock"))
    if not sent_ok:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=actor_user_id,
                action="manager_invite_send_failed",
                object_type="service_user",
                object_id=str(manager_user.id),
                details_json=f'{{"max_account_id":"{manager_id_clean}"}}',
            )
        )
        db.commit()
        status_code = send_result.get("status_code")
        if status_code == 404:
            return False, "Указан неверный ID менеджера в Max."
        if status_code == 400:
            return False, "Не удалось отправить ссылку: проверьте корректность ID менеджера."
        return False, "Не удалось отправить ссылку в Max."

    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action="manager_invite_sent",
            object_type="service_user",
            object_id=str(manager_user.id),
            details_json=f'{{"max_account_id":"{manager_id_clean}"}}',
        )
    )
    db.commit()
    return True, f"Ссылка отправлена менеджеру {manager_id_clean}."


def _build_manager_invite_link_for_copy(
    db: Session,
    *,
    workspace_id: int,
    manager_id: str,
    actor_user_id: int | None,
) -> tuple[bool, str, str]:
    manager_id_clean = (manager_id or "").strip()
    if not manager_id_clean:
        return False, "Укажите ID менеджера.", ""
    if not manager_id_clean.isdigit():
        return False, "Указан некорректный ID менеджера в Max.", ""

    ok_ws, _ = ensure_workspace_limits_and_state(db, workspace_id=workspace_id)
    if not ok_ws:
        return False, "Workspace приостановлен из-за биллинга.", ""

    try:
        manager_user = _get_or_create_manager_by_max_id(
            db,
            workspace_id=workspace_id,
            manager_id=manager_id_clean,
            actor_user_id=actor_user_id,
        )
    except ValueError as exc:
        if str(exc) == "manager_limit_exceeded":
            return False, "Ваш тарифный план не позволяет добавлять больше менеджеров.", ""
        if str(exc) == "managers_disabled_for_system_workspace":
            return False, "Для системного workspace создание менеджеров отключено.", ""
        return False, "Менеджер не активен или заблокирован.", ""

    links = _build_manager_invite_links(
        db=db,
        manager=manager_user,
        workspace_id=workspace_id,
        base_url=settings.public_base_url.rstrip("/"),
    )
    invite_link = (links.get("invite_link") or "").strip()
    if not invite_link:
        return False, "Не удалось сформировать ссылку приглашения.", ""
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action="manager_invite_link_generated",
            object_type="service_user",
            object_id=str(manager_user.id),
            details_json=f'{{"max_account_id":"{manager_id_clean}"}}',
        )
    )
    db.commit()
    return True, "Ссылка сформирована.", invite_link


def _upsert_template_values_single_commit(
    db: Session,
    *,
    workspace_id: int,
    prestart_message: str,
    start_message: str,
    after_phone_message: str,
) -> None:
    # Some legacy SQLite snapshots still have unique(template_key) without workspace scope.
    # Update existing rows first and insert only missing rows to avoid insertmany conflicts.
    values = {
        TEMPLATE_PRESTART: (prestart_message or "").strip(),
        TEMPLATE_START: (start_message or "").strip(),
        TEMPLATE_AFTER_PHONE: (after_phone_message or "").strip(),
    }
    existing_rows = (
        db.query(MessageTemplate)
        .filter(
            MessageTemplate.workspace_id == workspace_id,
            MessageTemplate.template_key.in_(list(values.keys())),
        )
        .all()
    )
    existing_by_key = {row.template_key: row for row in existing_rows}
    for key, text_value in values.items():
        row = existing_by_key.get(key)
        if row is not None:
            row.template_text = text_value
            db.add(row)
            continue
        db.add(
            MessageTemplate(
                workspace_id=workspace_id,
                template_key=key,
                template_text=text_value,
            )
        )


def _drop_legacy_template_unique_index(db: Session) -> None:
    # Old single-tenant snapshots may keep global unique(template_key) index.
    # Drop it in runtime to avoid 500 during /app/settings save for new workspaces.
    db.execute(text("DROP INDEX IF EXISTS ix_message_templates_template_key"))


def _superadmin_dashboard_snapshot(db: Session) -> dict:
    normalized_role = func.lower(func.trim(ServiceUser.role))
    registered_workspace_ids = {
        int(row.workspace_id)
        for row in db.query(ServiceUser.workspace_id)
        .filter(
            ServiceUser.workspace_id.isnot(None),
            normalized_role.in_(list(_WORKSPACE_USER_ROLES)),
        )
        .all()
        if row.workspace_id is not None
        and int(row.workspace_id) > 0
        and int(row.workspace_id) != int(DEFAULT_WORKSPACE_ID)
    }
    workspaces = (
        db.query(Workspace)
        .filter(Workspace.id.in_(sorted(registered_workspace_ids)))
        .order_by(Workspace.id.asc())
        .all()
        if registered_workspace_ids
        else []
    )
    users = (
        db.query(ServiceUser)
        .filter(
            normalized_role.in_(list(_WORKSPACE_USER_ROLES)),
            ServiceUser.workspace_id.isnot(None),
            ServiceUser.workspace_id != int(DEFAULT_WORKSPACE_ID),
        )
        .order_by(ServiceUser.id.asc())
        .all()
    )
    sessions = db.query(UserSession).filter(UserSession.is_revoked.is_(False)).count()
    alerts_total = db.query(TenantAlert).filter(TenantAlert.is_resolved.is_(False)).count()
    suspended = sum(1 for ws in workspaces if ws.is_suspended)
    inactive = sum(1 for ws in workspaces if not ws.is_active)
    roles = {
        "superadmin": 0,
        "user": sum(1 for u in users if str(u.role or "").strip().lower() in _WORKSPACE_USER_ROLES),
        "manager": 0,
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
    audit_page: int = 1,
    message: str | None = None,
    error: str | None = None,
) -> dict:
    active_tab = tab if tab in _SUPERADMIN_TABS else "dashboard"
    dashboard = _superadmin_dashboard_snapshot(db)
    normalized_role = func.lower(func.trim(ServiceUser.role))
    workspace_rows: list[dict] = []
    tariff_rows: list[dict[str, object]] = []
    if active_tab in {"workspaces", "plans", "monitoring"}:
        registered_workspace_ids = {
            int(row.workspace_id)
            for row in db.query(ServiceUser.workspace_id)
            .filter(
                ServiceUser.workspace_id.isnot(None),
                normalized_role.in_(list(_WORKSPACE_USER_ROLES)),
            )
            .all()
            if row.workspace_id is not None
            and int(row.workspace_id) > 0
            and int(row.workspace_id) != int(DEFAULT_WORKSPACE_ID)
        }
        workspaces = (
            db.query(Workspace)
            .filter(Workspace.id.in_(sorted(registered_workspace_ids)))
            .order_by(Workspace.id.asc())
            .all()
            if registered_workspace_ids
            else []
        )
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

        workspace_metrics: dict[int, dict[str, int]] = {}
        for ws in workspaces:
            workspace_metrics[ws.id] = collect_tenant_metrics(db, workspace_id=ws.id)

        settings_rows = db.query(BotSettings).filter(
            BotSettings.workspace_id.in_([ws.id for ws in workspaces])
        ).all()
        settings_by_workspace: dict[int, BotSettings] = {
            int(row.workspace_id): row for row in settings_rows
        }
        owner_rows = (
            db.query(ServiceUser)
            .filter(
                ServiceUser.workspace_id.in_([ws.id for ws in workspaces]),
                normalized_role.in_(["user", "admin", "owner"]),
            )
            .order_by(ServiceUser.workspace_id.asc(), ServiceUser.id.asc())
            .all()
        )
        owner_login_by_workspace: dict[int, str] = {}
        for owner in owner_rows:
            workspace_key = int(owner.workspace_id or 0)
            if workspace_key <= 0 or workspace_key in owner_login_by_workspace:
                continue
            owner_login_by_workspace[workspace_key] = str(owner.username or "").strip() or "—"
        plans_by_code: dict[str, TariffPlan] = {
            _normalize_tariff_plan_code(row.code): row
            for row in db.query(TariffPlan).order_by(TariffPlan.id.asc()).all()
        }
        default_plan = _get_or_create_default_tariff_plan(db)
        if active_tab == "plans":
            for plan in plans_by_code.values():
                limits = _tariff_plan_limits_dict(plan)
                tariff_rows.append(
                    {
                        "id": int(plan.id),
                        "code": str(plan.code),
                        "name": str(plan.name or plan.code),
                        "description": str(plan.description or ""),
                        "limits": limits,
                        "is_default": bool(plan.is_default),
                        "is_active": bool(plan.is_active),
                        "billing_product_code": str(plan.billing_product_code or ""),
                        "billing_price_code": str(plan.billing_price_code or ""),
                        "is_unlimited": int(limits.get("manager_limit", 0) or 0) >= 1_000_000_000,
                    }
                )
            tariff_rows.sort(key=lambda item: (0 if item.get("is_default") else 1, str(item.get("code") or "")))

        for ws in workspaces:
            sub = subs.get(ws.id) or get_or_create_subscription(db, workspace_id=ws.id)
            normalized_sub_plan_code = _normalize_tariff_plan_code(sub.plan_code)
            sub.plan_code = normalized_sub_plan_code
            selected_plan = plans_by_code.get(normalized_sub_plan_code) or default_plan
            if selected_plan is None:
                selected_plan = _get_or_create_default_tariff_plan(db)
            if normalized_sub_plan_code != str(selected_plan.code):
                sub.plan_code = str(selected_plan.code)
            _sync_subscription_cached_limits_from_plan(db, subscription=sub)
            db.add(sub)
            m = workspace_metrics.get(ws.id, {})
            ws_settings = settings_by_workspace.get(int(ws.id))
            ws_webhook_key = (ws_settings.webhook_key or "").strip() if ws_settings else ""
            ws_token_set = bool((ws_settings.bot_token or "").strip()) if ws_settings else False
            status = "suspended" if ws.is_suspended else ("inactive" if not ws.is_active else "active")
            workspace_rows.append(
                {
                    "workspace": ws,
                    "subscription": sub,
                    "metrics": m,
                    "open_alerts_count": int(open_alerts_by_workspace.get(ws.id, 0)),
                    "bot_token_set": ws_token_set,
                    "webhook_key": ws_webhook_key,
                    "webhook_url": _workspace_webhook_url_by_key(ws_webhook_key),
                    "bot_link": ((ws_settings.bot_link or "").strip() if ws_settings else ""),
                    "id": ws.id,
                    "name": ws.name,
                    "tenant_code": ws.tenant_code,
                    "status": status,
                    "plan_code": sub.plan_code,
                    "plan_name": str(getattr(selected_plan, "name", "") or sub.plan_code),
                    "available_plan_options": [
                        {
                            "code": str(plan.code),
                            "name": str(plan.name or plan.code),
                            "is_active": bool(plan.is_active),
                        }
                        for plan in plans_by_code.values()
                        if bool(plan.is_active)
                    ],
                    "sub_status": sub.status,
                    "owner_username": owner_login_by_workspace.get(int(ws.id), "—"),
                    "managers_active": int(m.get("managers_active", 0)),
                    "dialogs_total": int(m.get("dialogs_total", 0)),
                    "messages_month": int(m.get("messages_month", 0)),
                }
            )
        db.commit()

    user_rows: list[dict] = []
    if active_tab == "users":
        # Superadmin users tab should list workspace managers too.
        users_roles_for_superadmin = set(_WORKSPACE_USER_ROLES) | {"manager"}
        users = (
            db.query(ServiceUser)
            .filter(
                normalized_role.in_(list(users_roles_for_superadmin)),
                ServiceUser.workspace_id.isnot(None),
                ServiceUser.workspace_id != int(DEFAULT_WORKSPACE_ID),
            )
            .order_by(ServiceUser.id.asc())
            .all()
        )
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

    query_error = str(request.query_params.get("error") or "").strip().lower()
    query_message = ""
    if query_error:
        error_map = {
            "tariff_exists": "Тариф с таким кодом уже существует.",
            "tariff_missing": "Тариф не найден.",
            "tariff_in_use": "Нельзя удалить тариф: он назначен пользователям.",
            "tariff_default_delete": "Нельзя удалить тариф по умолчанию.",
            "workspace_missing": "Клиент не найден.",
            "backup_missing": "Резервная копия не найдена.",
            "superadmin_protected": "Удаление пользователя с ролью superadmin запрещено.",
        }
        mapped = error_map.get(query_error)
        if mapped and not error:
            error = mapped
    if request.query_params.get("tariff_created") == "1":
        query_message = "Тариф создан."
    elif request.query_params.get("tariff_updated") == "1":
        query_message = "Тариф обновлен."
    elif request.query_params.get("tariff_deleted") == "1":
        query_message = "Тариф удален."
    elif request.query_params.get("tariff_default") == "1":
        query_message = "Тариф по умолчанию обновлен."
    elif request.query_params.get("workspace_tariff_updated") == "1":
        query_message = "Тариф клиента обновлен."
    elif request.query_params.get("backup_deleted") == "1":
        query_message = "Резервная копия удалена."
    if query_message and not message:
        message = query_message

    audits_rows: list[dict] = []
    audit_pagination: dict[str, int | bool] = {
        "page": 1,
        "page_size": _SUPERADMIN_AUDIT_PAGE_SIZE,
        "pages_total": 1,
        "offset": 0,
        "has_prev": False,
        "has_next": False,
        "prev_page": 1,
        "next_page": 1,
        "total_count": 0,
    }
    if active_tab in {"audit"}:
        audit_total_count = (
            db.query(AuditLog)
            .filter(
                (AuditLog.workspace_id != int(DEFAULT_WORKSPACE_ID))
                | (AuditLog.workspace_id.is_(None))
            )
            .count()
        )
        audit_pagination = _build_audit_pagination(
            total_count=audit_total_count,
            requested_page=request.query_params.get("page"),
            page_size=_SUPERADMIN_AUDIT_PAGE_SIZE,
        )
        latest_audits = (
            db.query(AuditLog)
            .filter(
                (AuditLog.workspace_id != int(DEFAULT_WORKSPACE_ID))
                | (AuditLog.workspace_id.is_(None))
            )
            .order_by(AuditLog.id.desc())
            .offset(int(audit_pagination["offset"]))
            .limit(int(audit_pagination["page_size"]))
            .all()
        )
        audits_rows = [
            {
                "id": row.id,
                "workspace_id": row.workspace_id,
                "actor_user_id": row.actor_user_id,
                "action": row.action,
                "object_type": row.object_type,
                "object_id": row.object_id,
                "created_at": _to_iso(row.created_at),
                "details_json": row.details_json,
            }
            for row in latest_audits
        ]

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
    backups = list_backups(limit=100) if active_tab == "backups" else []
    support_row = (
        db.query(PlatformSettings).order_by(PlatformSettings.id.asc()).first()
        if active_tab == "system"
        else None
    )
    support_contacts = {
        "tech_email": (
            (support_row.technical_support_email if support_row else "")
            or (settings.support_tech_email or "").strip()
        ),
        "billing_email": (
            (support_row.billing_support_email if support_row else "")
            or (settings.support_finance_email or "").strip()
        ),
    }

    return {
        "request": request,
        "current_user": current_user,
        "section": active_tab,
        "page_title": page_title_map.get(active_tab, "Панель суперадмина"),
        "message": message,
        "error": error,
        "stats": dashboard,
        "workspace_rows": workspace_rows,
        "tariff_rows": tariff_rows,
        "users": user_rows,
        "audit_items": audits_rows,
        "audit_pagination": audit_pagination,
        "backups": backups,
        "smtp": smtp_info,
        "security": security_info,
        "support_contacts": support_contacts,
        "superadmin_static_2fa_code_enabled": bool((settings.superadmin_static_2fa_code or "").strip()),
    }


def _render_superadmin_page(
    *,
    request: Request,
    current_user: ServiceUser,
    db: Session,
    tab: str,
    message: str | None = None,
    error: str | None = None,
    audit_page: int | None = None,
) -> HTMLResponse:
    _require_superadmin(current_user)
    context = _build_superadmin_context(
        request=request,
        current_user=current_user,
        db=db,
        tab=tab,
        message=message,
        error=error,
        audit_page=audit_page,
    )
    return templates.TemplateResponse(request, "superadmin.html", context)

app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["to_local"] = _to_local
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
webhook_path = settings.webhook_path if settings.webhook_path.startswith("/") else f"/{settings.webhook_path}"

ensure_storage_ready()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
_outbox_worker_task: asyncio.Task | None = None
_storage_cleanup_last_run_at: datetime | None = None
_rate_limiter = InMemoryRateLimiter()
_MAX_UPLOAD_BYTES = int(settings.max_upload_bytes)
_CHAT_UPDATES_MESSAGES_LIMIT = 220
_QUICK_REPLY_PHOTO_MAX_BYTES = 1 * 1024 * 1024
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EMAIL_VERIFY_SALT = "email-verify-link"


def _apply_security_headers(response):
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if settings.force_https:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    # Chat/quick-reply previews use URL.createObjectURL(file) => blob: URLs.
    # Allow blob: in img-src so client-side previews render before upload.
    csp = "default-src 'self'; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
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


async def _read_and_validate_uploads(photos: list[UploadFile] | None) -> list[tuple[str, bytes, str]]:
    validated: list[tuple[str, bytes, str]] = []
    for upload in photos or []:
        if not upload or not upload.filename:
            continue
        ext, content = await _read_and_validate_upload(upload)
        if ext is None or content is None:
            continue
        validated.append((ext, content, str(upload.filename)))
    return validated


async def _merge_upload_inputs(
    request: Request,
    *,
    files_field: str = "photos",
    legacy_file_field: str = "photo",
    provided_files: list[UploadFile] | None = None,
) -> list[UploadFile]:
    merged: list[UploadFile] = [item for item in (provided_files or []) if item and getattr(item, "filename", "")]
    if merged:
        return merged
    form_data = await request.form()
    all_items_getter = getattr(form_data, "getlist", None)
    if callable(all_items_getter):
        candidate_items = all_items_getter(files_field) or []
    else:
        candidate_items = []
    for item in candidate_items:
        if isinstance(item, UploadFile) and item.filename:
            merged.append(item)
    if merged:
        return merged
    legacy_item = form_data.get(legacy_file_field)
    if isinstance(legacy_item, UploadFile) and legacy_item.filename:
        merged.append(legacy_item)
    return merged


def _store_uploaded_images(validated_files: list[tuple[str, bytes, str]]) -> list[str]:
    stored_paths: list[str] = []
    for ext, content, _name in validated_files:
        safe_name = f"{uuid4().hex}{ext}"
        stored_paths.append(save_upload_bytes(file_name=safe_name, content=content))
    return stored_paths


def _cleanup_uploaded_images(paths: list[str]) -> None:
    for item in paths:
        path_value = str(item or "").strip()
        if not path_value:
            continue
        # Avoid removing files that are still referenced by chat/quick-reply rows.
        with SessionLocal() as cleanup_db:
            _try_delete_unreferenced_media_file(cleanup_db, media_path=path_value)


async def _resolve_schedule_at_value(request: Request, schedule_at: str) -> str:
    """Read schedule value with backward-compatible legacy key fallback."""
    value = (schedule_at or "").strip()
    if value:
        return value
    form_data = await request.form()
    legacy_value = str(form_data.get("scheduled_at") or "").strip()
    return legacy_value


async def _resolve_text_value(request: Request, text: str) -> str:
    """Read message text with form fallback for non-multipart clients."""
    value = (text or "").strip()
    if value:
        return value
    form_data = await request.form()
    return str(form_data.get("text") or "").strip()


async def _read_and_validate_quick_reply_photo(photo: UploadFile | None) -> tuple[str | None, bytes | None]:
    if not photo or not photo.filename:
        return None, None
    content = await photo.read()
    ok, reason = is_safe_image(
        filename=photo.filename,
        content=content,
        max_bytes=_QUICK_REPLY_PHOTO_MAX_BYTES,
    )
    if not ok:
        if reason == "file_too_large":
            raise HTTPException(status_code=413, detail="Фото для быстрого ответа должно быть не больше 1 МБ")
        raise HTTPException(status_code=400, detail="Некорректный файл изображения")
    ext = Path(photo.filename).suffix.lower()
    return ext, content


def _workspace_id_for_user(user: ServiceUser | None) -> int:
    if user is None:
        return DEFAULT_WORKSPACE_ID
    return int(user.workspace_id or DEFAULT_WORKSPACE_ID)


def _quick_reply_media_rows(db: Session, reply_id: int) -> list[QuickReplyMedia]:
    return (
        db.query(QuickReplyMedia)
        .filter(QuickReplyMedia.quick_reply_id == reply_id)
        .order_by(QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
        .all()
    )


def _attach_media_to_quick_replies(db: Session, replies: list[QuickReply]) -> list[QuickReply]:
    if not replies:
        return replies
    reply_ids = [int(item.id) for item in replies]
    media_rows = (
        db.query(QuickReplyMedia)
        .filter(QuickReplyMedia.quick_reply_id.in_(reply_ids))
        .order_by(QuickReplyMedia.quick_reply_id.asc(), QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
        .all()
    )
    by_reply_id: dict[int, list[QuickReplyMedia]] = {}
    for row in media_rows:
        by_reply_id.setdefault(int(row.quick_reply_id), []).append(row)
    for reply in replies:
        setattr(reply, "media_items", by_reply_id.get(int(reply.id), []))
    return replies


async def _save_quick_reply_media_files(
    *,
    db: Session,
    reply: QuickReply,
    files: list[UploadFile],
    sort_order_tokens: list[str] | None = None,
) -> None:
    if not files:
        return
    existing = _quick_reply_media_rows(db, reply.id)
    next_order = len(existing)
    order_by_name: dict[str, int] = {}
    for idx, raw in enumerate(sort_order_tokens or []):
        token = str(raw or "").strip()
        if not token:
            continue
        filename = token
        if token.startswith("new:"):
            filename = token[4:]
        elif "|" in token:
            parts = token.split("|")
            if len(parts) >= 2:
                filename = parts[1].strip()
        if filename and filename not in order_by_name:
            order_by_name[filename] = idx
    for upload in files:
        if not upload or not upload.filename:
            continue
        # Explicit product rule for quick-reply photos: max 1 MB.
        if upload.size is not None and int(upload.size) > _QUICK_REPLY_PHOTO_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Фото в быстром ответе не должно превышать 1 МБ.")
        ext, content = await _read_and_validate_quick_reply_photo(upload)
        if len(content) > _QUICK_REPLY_PHOTO_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Фото в быстром ответе не должно превышать 1 МБ.")
        safe_name = f"{uuid4().hex}{ext}"
        stored_public_url = save_upload_bytes(file_name=safe_name, content=content)
        workspace_id = int(reply.workspace_id or DEFAULT_WORKSPACE_ID)
        media = QuickReplyMedia(
            workspace_id=workspace_id,
            quick_reply_id=reply.id,
            media_path=stored_public_url,
            sort_order=order_by_name.get(upload.filename, next_order),
        )
        ensure_media_asset_for_path(
            db,
            workspace_id=workspace_id,
            media_path=media.media_path,
        )
        next_order += 1
        db.add(media)
    db.commit()


def _delete_quick_reply_media_files(db: Session, *, reply_id: int) -> None:
    media_rows = _quick_reply_media_rows(db, reply_id)
    for media in media_rows:
        media_path_value = str(media.media_path or "").strip()
        remove_quick_reply_media_asset_link(
            db,
            quick_reply_id=int(reply_id),
            media_path=media_path_value,
        )
        _try_delete_unreferenced_media_file(db, media_path=media_path_value)
    db.query(QuickReplyMedia).filter(QuickReplyMedia.quick_reply_id == reply_id).delete(synchronize_session=False)
    # Keep normalized quick-reply media links consistent after hard delete.
    sync_quick_reply_media_asset_links(
        db,
        quick_reply_id=int(reply_id),
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    db.commit()


def _normalize_quick_reply_media_order(db: Session, reply_id: int) -> None:
    ordered = _quick_reply_media_rows(db, reply_id)
    changed = False
    for idx, media in enumerate(ordered):
        if media.sort_order != idx:
            media.sort_order = idx
            db.add(media)
            changed = True
    if changed:
        db.commit()


def _quick_reply_owner_user_id(current_user: ServiceUser | None) -> int:
    if current_user is None:
        return 0
    if (current_user.role or "").strip().lower() == "manager":
        return int(current_user.id or 0)
    return 0


def _settings_ui_mode(current_user: ServiceUser) -> str:
    return "manager" if (current_user.role or "").strip().lower() == "manager" else "app"


def _load_quick_replies_with_media(
    db: Session,
    *,
    workspace_id: int,
    owner_user_id: int = 0,
) -> list[QuickReply]:
    replies = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == int(owner_user_id or 0),
        )
        .order_by(QuickReply.command.asc())
        .all()
    )
    return _attach_media_to_quick_replies(db, replies)


def _chat_scope_quick_reply_owner_id(
    *,
    current_user: ServiceUser | None = None,
    manager_claims: dict | None = None,
) -> int:
    if current_user is not None:
        return _quick_reply_owner_user_id(current_user)
    if manager_claims:
        manager_user_id = manager_claims.get("service_user_id")
        if isinstance(manager_user_id, int) and manager_user_id > 0:
            return int(manager_user_id)
    return 0


def _workspace_quick_replies_count(db: Session, *, workspace_id: int) -> int:
    return db.query(QuickReply).filter(QuickReply.workspace_id == workspace_id).count()


def _build_quick_options_for_compose(
    db: Session,
    *,
    workspace_id: int,
    owner_user_id: int,
) -> list[dict]:
    """Return quick reply data for the slash-menu compose fill feature.

    Each item includes ``text`` and ``media_urls`` so the frontend can populate
    the message input and media preview without a separate API round-trip.
    """
    replies = _attach_media_to_quick_replies(
        db,
        list_active_quick_replies(db, workspace_id=workspace_id, owner_user_id=owner_user_id),
    )
    result: list[dict] = []
    for item in replies:
        media_items: list = getattr(item, "media_items", None) or []
        media_urls: list[str] = []
        if media_items:
            for row in media_items:
                path = str(row.media_path or "").strip()
                if path:
                    media_urls.append(path)
        elif item.image_path:
            path = str(item.image_path or "").strip()
            if path:
                media_urls.append(path)
        result.append({
            "id": int(item.id),
            "command": item.command,
            "title": item.title,
            "text": item.text or "",
            "media_urls": media_urls,
        })
    return result


def _manager_ids_from_settings_row(settings_row: BotSettings | None) -> set[str]:
    if settings_row is None:
        return set()
    return {item.strip() for item in _parse_manager_ids(settings_row.manager_account_id) if item.strip()}


def _resolve_workspace_by_webhook_key(db: Session, key: str) -> int | None:
    normalized = (key or "").strip()
    if not normalized:
        return None
    row = (
        db.query(BotSettings.workspace_id)
        .filter(BotSettings.webhook_key == normalized)
        .first()
    )
    if row and row[0]:
        return int(row[0])
    return None


def _resolve_workspace_id_from_event(db: Session, event: MaxWebhookEvent) -> int:
    sender_id = (event.sender_id or "").strip()
    chat_id = (event.chat_id or "").strip()
    if sender_id:
        manager_row = (
            db.query(ServiceUser.workspace_id)
            .filter(
                ServiceUser.role == "manager",
                ServiceUser.max_account_id == sender_id,
            )
            .first()
        )
        if manager_row and manager_row[0]:
            return int(manager_row[0])
        for settings_row in db.query(BotSettings).order_by(BotSettings.id.asc()).all():
            if sender_id in _manager_ids_from_settings_row(settings_row):
                return int(settings_row.workspace_id or DEFAULT_WORKSPACE_ID)
    if chat_id:
        conversation_row = (
            db.query(Conversation.workspace_id)
            .filter(Conversation.chat_id == chat_id)
            .first()
        )
        if conversation_row and conversation_row[0]:
            return int(conversation_row[0])
        profile_by_chat = (
            db.query(CustomerProfile.workspace_id)
            .filter(CustomerProfile.source_chat_id == chat_id)
            .order_by(CustomerProfile.id.desc())
            .first()
        )
        if profile_by_chat and profile_by_chat[0]:
            return int(profile_by_chat[0])
    if sender_id:
        profile_row = (
            db.query(CustomerProfile.workspace_id)
            .filter(CustomerProfile.customer_account_id == sender_id)
            .order_by(CustomerProfile.id.desc())
            .first()
        )
        if profile_row and profile_row[0]:
            return int(profile_row[0])
    return DEFAULT_WORKSPACE_ID


def _sha256(value: str) -> str:
    return auth_sha256(value)


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


def _safe_media_diagnostics(
    db: Session,
    *,
    workspace_id: int,
    window_minutes: int = 30,
) -> object:
    """
    Keep settings page resilient if diagnostics schema is partially migrated.
    """
    try:
        return get_media_diagnostics_metrics(
            db,
            workspace_id=workspace_id,
            window_minutes=window_minutes,
        )
    except Exception:
        db.rollback()
        return None


def _estimate_media_upload_risk_for_ten_photos(*, per_file_bytes: int) -> dict[str, object]:
    per_file = max(0, int(per_file_bytes or 0))
    estimated_total = per_file * 10
    safe_limit = max(0, int(_MAX_UPLOAD_BYTES or 0))
    risk = bool(safe_limit > 0 and estimated_total > safe_limit)
    return {
        "per_file_bytes": per_file,
        "safe_limit_bytes": safe_limit,
        "estimated_total_for_10_bytes": estimated_total,
        "risk_for_10_photos": risk,
        "note": (
            "Ожидается риск 413 по суммарному размеру запроса (client_max_body_size)."
            if risk
            else "Локальный лимит не указывает риск 413 для 10 фото."
        ),
    }


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
    edit_quick_reply_id: int | None = None,
) -> HTMLResponse:
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    is_manager_view = (current_user.role or "").strip().lower() == "manager"
    ok = True
    reason = ""
    try:
        ok, reason = ensure_workspace_limits_and_state(db, workspace_id=workspace_id)
    except OperationalError:
        # Keep settings page available on partially migrated SQLite snapshots.
        db.rollback()
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    quick_replies = _load_quick_replies_with_media(
        db,
        workspace_id=workspace_id,
        owner_user_id=quick_reply_owner_id,
    )
    quick_reply_edit_target: QuickReply | None = None
    if edit_quick_reply_id is not None:
        quick_reply_edit_target = (
            db.query(QuickReply)
            .filter(
                QuickReply.workspace_id == workspace_id,
                QuickReply.owner_user_id == quick_reply_owner_id,
                QuickReply.id == int(edit_quick_reply_id),
            )
            .first()
        )
    intro_steps = (
        db.query(IntroStep)
        .filter(IntroStep.workspace_id == workspace_id, IntroStep.is_active.is_(True))
        .order_by(IntroStep.step_order.asc(), IntroStep.id.asc())
        .all()
    )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    media_send_diagnostics = get_media_diagnostics_metrics(db, workspace_id=workspace_id)
    sub = None
    tenant_metrics = None
    active_alerts: list[TenantAlert] = []
    try:
        sub = get_or_create_subscription(db, workspace_id=workspace_id)
        tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
        refresh_tenant_alerts(db, workspace_id=workspace_id, metrics=tenant_metrics)
        active_alerts = (
            db.query(TenantAlert)
            .filter(
                TenantAlert.workspace_id == workspace_id,
                TenantAlert.is_resolved.is_(False),
            )
            .order_by(TenantAlert.id.desc())
            .all()
        )
    except OperationalError:
        # Keep settings page available on partially migrated SQLite snapshots.
        db.rollback()
    manager_rows_limit = _manager_rows_limit(db, workspace_id=workspace_id)
    manager_id_rows = _parse_manager_ids(bot_settings.manager_account_id)
    manager_status_rows, manager_status_summary = _collect_manager_status_rows(
        db,
        workspace_id=workspace_id,
        manager_ids=manager_id_rows,
    )
    note = message or ""
    if not ok:
        note += f" · Ограничение: {reason}"
    email_status = "подтверждена" if bool(current_user.email_verified) else "не подтверждена"
    cooldown_seconds = max(int(settings.email_verification_resend_cooldown_seconds), 0)
    can_resend_verification = True
    if not current_user.email_verified and current_user.email_verification_sent_at:
        elapsed = (datetime.now(UTC).replace(tzinfo=None) - current_user.email_verification_sent_at).total_seconds()
        can_resend_verification = elapsed >= cooldown_seconds
    usage_limits, usage_used, usage_remaining = _build_usage_context(
        db,
        subscription=sub,
        tenant_metrics=tenant_metrics,
        manager_status_summary=manager_status_summary,
    )
    _ensure_workspace_webhook_key(bot_settings)
    db.add(bot_settings)
    db.flush()
    bot_link_value = (bot_settings.bot_link or "").strip()
    bot_token_value = (bot_settings.bot_token or "").strip()
    masked_token = ""
    if bot_token_value:
        if len(bot_token_value) <= 4:
            masked_token = "*" * len(bot_token_value)
        else:
            masked_token = f"{bot_token_value[:2]}{'*' * max(len(bot_token_value) - 4, 4)}{bot_token_value[-2:]}"
    bot_connection_ok = bool(bot_token_value)
    bot_connection_note = ""
    webhook_workspace_url = _workspace_webhook_url(bot_settings)
    business_hours_vm = _business_hours_view_model(db, workspace_id=workspace_id)
    storage_health = get_storage_health_snapshot()

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
            "webhook_url": webhook_workspace_url,
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "media_send_diagnostics": media_send_diagnostics,
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "tenant_alerts": active_alerts,
            "message": note,
            "error": error,
            "ui_mode": _settings_ui_mode(current_user),
            "current_user": current_user,
            "chats_href": "/app/chats",
            "logout_action": "/app/logout",
            "settings_action": "/app/settings",
            "token_delete_action": "/app/settings/delete-bot-token",
            "manager_id_rows": manager_id_rows,
            "manager_ids_limit": _manager_limit_for_workspace(db, workspace_id=workspace_id),
            "manager_invite_copy_action": "/app/settings/copy-manager-link",
            "manager_remove_action": "/app/settings/remove-manager",
            "quick_reply_create_action": "/app/quick-replies",
            "quick_reply_delete_action_prefix": "/app/quick-replies/",
            "quick_reply_edit_target": quick_reply_edit_target,
            "intro_create_action": "/app/settings/intro-steps",
            "intro_delete_action_prefix": "/app/settings/intro-steps/",
            "email_status": email_status,
            "email_verified": bool(current_user.email_verified),
            "email_value": current_user.username,
            "email_resend_action": "/app/resend-email-verification",
            "can_resend_email_verification": can_resend_verification,
            "manager_id_rows": manager_id_rows,
            "manager_ids_limit": manager_rows_limit,
            "manager_invite_copy_action": "/app/settings/copy-manager-link",
            "manager_remove_action": "/app/settings/remove-manager",
            "manager_status_rows": manager_status_rows,
            "manager_status_summary": manager_status_summary,
            "usage_limits": usage_limits,
            "usage_used": usage_used,
            "usage_remaining": usage_remaining,
            "workspace_bot_link": bot_link_value,
            "workspace_bot_token_masked": masked_token,
            "workspace_bot_connection_ok": bot_connection_ok,
            "workspace_bot_connection_note": bot_connection_note,
            "storage_health": storage_health,
            "workspace_webhook_url": webhook_workspace_url,
            "routing_mode_label": _manager_routing_mode_label(bot_settings.routing_mode or "round_robin"),
            "business_hours": business_hours_vm,
            "business_hours_preview_action": "/app/settings/business-hours/preview",
            "show_settings_card": not is_manager_view,
            "show_app_token_card": False,
            # Hide technical webhook block in user settings.
            "show_webhook_card": False,
            "show_delivery_card": not is_manager_view,
            "show_managers_card": not is_manager_view,
            "show_tariff_card": not is_manager_view,
            "show_intro_steps_card": not is_manager_view,
            "show_intro_form": False,
            "show_business_hours_card": not is_manager_view,
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
    _purge_default_workspace_manager_data(db)
    return super_user


async def _kick_outbox_once() -> None:
    """Process outbox immediately after a new message is queued (no wait for next poll cycle)."""
    from app.database import SessionLocal
    try:
        with SessionLocal() as db:
            await process_outbox_queue(db, limit=settings.outbox_worker_batch_size)
    except Exception:
        pass


async def _outbox_worker_loop() -> None:
    from app.database import SessionLocal

    global _storage_cleanup_last_run_at
    while True:
        try:
            with SessionLocal() as db:
                await process_outbox_queue(db, limit=settings.outbox_worker_batch_size)
                workspace_ids = [row[0] for row in db.query(Workspace.id).all()]
                for workspace_id in workspace_ids:
                    refresh_tenant_alerts(db, workspace_id=int(workspace_id))
                    ensure_workspace_active_by_billing(db, workspace_id=int(workspace_id))
                now_utc = datetime.now(UTC)
                should_run_cleanup = (
                    _storage_cleanup_last_run_at is None
                    or (now_utc - _storage_cleanup_last_run_at) >= timedelta(hours=24)
                )
                if should_run_cleanup:
                    run_storage_cleanup_for_all_workspaces(db)
                    _storage_cleanup_last_run_at = now_utc
                # P1 backfill runs in small batches during normal worker cycles
                # to avoid downtime and reduce migration risk.
                cleanup_orphan_chat_message_media_links(db, limit=500)
                backfill_chat_message_media_assets(db, limit=250)
                backfill_quick_reply_media_assets(db, limit=250)
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
    sign_in_admin(request, username=username, password=password)
    return RedirectResponse(url="/app", status_code=302)


@app.post("/admin/logout")
def logout() -> RedirectResponse:
    return RedirectResponse(url="/app", status_code=302)


@app.get("/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    edit_quick_reply_id: int | None = None,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    storage_health = get_storage_health()
    workspace_id = DEFAULT_WORKSPACE_ID
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    business_hours_vm = _business_hours_view_model(db, workspace_id=workspace_id)
    webhook_workspace_url = _workspace_webhook_url(bot_settings)
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    usage_limits, usage_used, usage_remaining = _build_usage_context(
        db,
        subscription=sub,
        tenant_metrics=tenant_metrics,
        manager_status_summary=None,
    )
    replies = _load_quick_replies_with_media(db, workspace_id=workspace_id)
    quick_reply_edit_target: QuickReply | None = None
    if edit_quick_reply_id is not None:
        quick_reply_edit_target = (
            db.query(QuickReply)
            .filter(
                QuickReply.workspace_id == workspace_id,
                QuickReply.id == int(edit_quick_reply_id),
            )
            .first()
        )
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    media_send_diagnostics = get_media_diagnostics_metrics(db, workspace_id=workspace_id)
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
            "webhook_url": webhook_workspace_url,
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "media_send_diagnostics": media_send_diagnostics,
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "usage_limits": usage_limits,
            "usage_used": usage_used,
            "usage_remaining": usage_remaining,
            "manager_id_rows": _parse_manager_ids(bot_settings.manager_account_id),
            "manager_ids_limit": _manager_limit_for_workspace(db, workspace_id=workspace_id),
            "manager_invite_copy_action": "/admin/settings/copy-manager-link",
            "manager_status_rows": [],
            "manager_status_summary": {
                "connected": 0,
                "pending": 0,
                "failed": 0,
                "not_sent": 0,
                "deactivated": 0,
                "total": 0,
            },
            "message": None,
            "error": None,
            "quick_reply_edit_target": quick_reply_edit_target,
            "business_hours": business_hours_vm,
            "business_hours_preview_action": "/app/settings/business-hours/preview",
            "show_business_hours_card": True,
            "storage_health": storage_health,
        },
    )


@app.post("/admin/settings", response_class=HTMLResponse)
async def update_settings(
    request: Request,
    prestart_message: str = Form(...),
    start_message: str = Form(...),
    after_phone_message: str = Form(...),
    manager_account_id: str = Form(""),
    manager_account_id_2: str = Form(""),
    admin_account_id: str = Form(""),
    routing_mode: str = Form("round_robin"),
    request_customer_phone: str = Form(""),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)
    form_data = await request.form()
    settings_section = str(form_data.get("settings_section", "general") or "general").strip().lower()
    if settings_section == "business_hours":
        workspace_id = DEFAULT_WORKSPACE_ID
        business_payload = _extract_business_hours_form(form_data)
        replace_workspace_business_hours(
            db,
            workspace_id=workspace_id,
            enabled=bool(business_payload.get("enabled", False)),
            timezone_name=str(business_payload.get("timezone") or DEFAULT_BUSINESS_TIMEZONE),
            offhours_message=str(business_payload.get("offhours_message") or DEFAULT_OFFHOURS_MESSAGE),
            cooldown_seconds=int(
                business_payload.get("cooldown_seconds") or DEFAULT_OFFHOURS_COOLDOWN_SECONDS
            ),
            slots=list(business_payload.get("slots") or []),
            exceptions=[],
            commit=False,
        )
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=None,
                action="business_hours_updated",
                object_type="workspace_business_hours",
                object_id=str(workspace_id),
                details_json=safe_json_dumps(
                    {
                        "enabled": bool(business_payload.get("enabled", False)),
                        "timezone": str(business_payload.get("timezone") or ""),
                        "slots_count": len(list(business_payload.get("slots") or [])),
                        "cooldown_seconds": int(
                            business_payload.get("cooldown_seconds")
                            or DEFAULT_OFFHOURS_COOLDOWN_SECONDS
                        ),
                    }
                ),
            )
        )
        db.commit()
        return RedirectResponse(url="/admin", status_code=302)
    manager_ids = _extract_manager_ids_from_form(form_data)
    if not manager_ids:
        manager_ids = _parse_manager_ids(_normalize_manager_ids(f"{manager_account_id},{manager_account_id_2}"))
    manager_ids_csv = _normalize_manager_ids(",".join(manager_ids))
    workspace_id = DEFAULT_WORKSPACE_ID
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    business_hours_vm = _business_hours_view_model(db, workspace_id=workspace_id)
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    manager_limit_value = _manager_limit_for_workspace(db, workspace_id=workspace_id)
    webhook_workspace_url = _workspace_webhook_url(bot_settings)
    media_send_diagnostics = get_media_diagnostics_metrics(db, workspace_id=workspace_id)
    if len(manager_ids) > manager_limit_value:
        replies = _load_quick_replies_with_media(db, workspace_id=workspace_id)
        chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
        delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
        bot_settings.manager_account_id = manager_ids_csv
        return templates.TemplateResponse(
            request,
            "admin.html",
            {
                "request": request,
                "settings": bot_settings,
                "template_prestart": prestart_message,
                "template_start": start_message,
                "template_after_phone": after_phone_message,
                "quick_replies": replies,
                "webhook_path": webhook_path,
                "webhook_url": webhook_workspace_url,
                "chat_metrics": chat_metrics,
                "delivery_metrics": delivery_metrics,
                "media_send_diagnostics": media_send_diagnostics,
                "subscription": sub,
                "tenant_metrics": tenant_metrics,
                "manager_id_rows": manager_ids,
                "manager_ids_limit": manager_limit_value,
                "manager_invite_copy_action": "/admin/settings/copy-manager-link",
                "manager_status_rows": [],
                "manager_status_summary": {
                    "connected": 0,
                    "pending": 0,
                    "failed": 0,
                    "not_sent": 0,
                    "deactivated": 0,
                    "total": 0,
                },
                "message": None,
                "error": "Ваш тарифный план не позволяет добавлять больше менеджеров.",
                "business_hours": business_hours_vm,
                "business_hours_preview_action": "/app/settings/business-hours/preview",
                "show_business_hours_card": True,
            },
            status_code=400,
        )
    bot_settings.manager_account_id = manager_ids_csv
    bot_settings.admin_account_id = admin_account_id.strip()
    mode = (routing_mode or "round_robin").strip().lower()
    if mode not in {"round_robin", "random"}:
        mode = "round_robin"
    bot_settings.routing_mode = mode
    bot_settings.request_customer_phone = str(request_customer_phone or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }
    db.add(bot_settings)
    set_template_text(db, TEMPLATE_PRESTART, prestart_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_START, start_message, workspace_id=workspace_id)
    set_template_text(db, TEMPLATE_AFTER_PHONE, after_phone_message, workspace_id=workspace_id)
    db.commit()

    replies = _load_quick_replies_with_media(db, workspace_id=workspace_id)
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
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
            "webhook_url": webhook_workspace_url,
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "media_send_diagnostics": media_send_diagnostics,
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "manager_id_rows": manager_ids,
            "manager_ids_limit": manager_limit_value,
            "manager_invite_copy_action": "/admin/settings/copy-manager-link",
            "manager_status_rows": [],
            "manager_status_summary": {
                "connected": 0,
                "pending": 0,
                "failed": 0,
                "not_sent": 0,
                "deactivated": 0,
                "total": 0,
            },
            "message": "Настройки сохранены",
            "error": None,
            "business_hours": business_hours_vm,
            "business_hours_preview_action": "/app/settings/business-hours/preview",
            "show_business_hours_card": True,
        },
    )


@app.post("/admin/settings/send-manager-link", response_class=HTMLResponse)
async def admin_send_manager_link(
    request: Request,
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)
    form_data = await request.form()
    workspace_id = DEFAULT_WORKSPACE_ID
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    webhook_workspace_url = _workspace_webhook_url(settings_row)
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    target_manager_id = str(form_data.get("send_manager_id", "") or "").strip()

    ok, msg = await _send_manager_invite_link_to_max(
        db,
        workspace_id=workspace_id,
        manager_id=target_manager_id,
        actor_user_id=None,
    )
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": settings_row,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id),
            "template_start": get_template_text(db, TEMPLATE_START, workspace_id=workspace_id),
            "template_after_phone": get_template_text(
                db,
                TEMPLATE_AFTER_PHONE,
                workspace_id=workspace_id,
            ),
            "quick_replies": _load_quick_replies_with_media(db, workspace_id=workspace_id),
            "webhook_path": webhook_path,
            "webhook_url": webhook_workspace_url,
            "chat_metrics": get_chat_metrics(db, workspace_id=workspace_id),
            "delivery_metrics": get_delivery_metrics(db, workspace_id=workspace_id),
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "manager_id_rows": _parse_manager_ids(
                _normalize_manager_ids(get_or_create_settings(db, workspace_id=workspace_id).manager_account_id)
            ),
            "manager_ids_limit": _manager_limit_for_workspace(db, workspace_id=workspace_id),
            "manager_invite_copy_action": "/admin/settings/copy-manager-link",
            "manager_status_rows": [],
            "manager_status_summary": {
                "connected": 0,
                "pending": 0,
                "failed": 0,
                "not_sent": 0,
                "deactivated": 0,
                "total": 0,
            },
            "message": (msg if ok else None),
            "error": (None if ok else msg),
        },
        status_code=(200 if ok else 400),
    )


@app.post("/admin/settings/copy-manager-link", response_class=JSONResponse)
async def admin_copy_manager_link(
    request: Request,
    request_customer_phone: str = Form(""),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    if _admin is None:
        return JSONResponse({"ok": False, "error": "Сессия администратора истекла."}, status_code=401)
    form_data = await request.form()
    workspace_id = DEFAULT_WORKSPACE_ID
    manager_ids = _extract_manager_ids_from_form(form_data)
    manager_ids_csv = _normalize_manager_ids(",".join(manager_ids))
    target_manager_id = str(form_data.get("copy_manager_id", "") or "").strip()
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    limits = resolve_subscription_limits(db, subscription=sub)
    manager_limit_value = max(1, int(limits.get("manager_limit", 0) or 0))
    if len(manager_ids) > manager_limit_value:
        return JSONResponse(
            {
                "ok": False,
                "error": "Ваш тарифный план не позволяет добавлять больше менеджеров.",
            },
            status_code=400,
        )
    bot_settings.manager_account_id = manager_ids_csv
    bot_settings.admin_account_id = str(form_data.get("admin_account_id", "") or "").strip()
    mode = str(form_data.get("routing_mode", "round_robin") or "round_robin").strip().lower()
    if mode not in {"round_robin", "random"}:
        mode = "round_robin"
    bot_settings.routing_mode = mode
    bot_settings.request_customer_phone = str(request_customer_phone or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }
    quick_replies_limit_value = max(1, int(limits.get("quick_replies_limit", 0) or 0))
    quick_replies_count = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
        )
        .count()
    )
    if quick_replies_count >= quick_replies_limit_value:
        return JSONResponse(
            {
                "ok": False,
                "error": "Ваш тарифный план не позволяет создавать больше быстрых ответов.",
            },
            status_code=400,
        )
    db.add(bot_settings)
    db.commit()
    ok, msg, invite_link = _build_manager_invite_link_for_copy(
        db,
        workspace_id=workspace_id,
        manager_id=target_manager_id,
        actor_user_id=None,
    )
    if not ok:
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    return JSONResponse({"ok": True, "message": msg, "link": invite_link}, status_code=200)


@app.get("/admin/settings/storage-health", response_class=JSONResponse)
def admin_storage_health(
    _admin: str = Depends(require_admin),
) -> JSONResponse:
    health = get_storage_health()
    status_code = 200 if bool(health.get("ok", False)) else 503
    return JSONResponse({"ok": bool(health.get("ok", False)), "health": health}, status_code=status_code)


@app.get("/app/settings/storage-health", response_class=JSONResponse)
def app_storage_health(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    health = get_storage_health()
    status_code = 200 if bool(health.get("ok", False)) else 503
    return JSONResponse({"ok": bool(health.get("ok", False)), "health": health}, status_code=status_code)


@app.post("/admin/quick-replies", response_class=HTMLResponse)
async def create_quick_reply(
    request: Request,
    command: str = Form(...),
    title: str = Form(...),
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    media_order: str = Form(""),
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
    webhook_workspace_url = _workspace_webhook_url(get_or_create_settings(db, workspace_id=workspace_id))
    sub = get_or_create_subscription(db, workspace_id=workspace_id)

    normalized = command.strip().lstrip("/")
    normalized = normalized.lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")

    media_send_diagnostics = get_media_diagnostics_metrics(db, workspace_id=workspace_id)
    if (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == 0,
            QuickReply.command == normalized,
        )
        .first()
    ):
        bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
        replies = _load_quick_replies_with_media(db, workspace_id=workspace_id)
        chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
        delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
        tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
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
                "webhook_url": webhook_workspace_url,
                "chat_metrics": chat_metrics,
                "delivery_metrics": delivery_metrics,
                "media_send_diagnostics": media_send_diagnostics,
                "subscription": sub,
                "tenant_metrics": tenant_metrics,
                "manager_status_rows": [],
                "manager_status_summary": {
                    "connected": 0,
                    "pending": 0,
                    "failed": 0,
                    "not_sent": 0,
                    "deactivated": 0,
                    "total": 0,
                },
                "message": None,
                "error": f"Команда /{normalized} уже существует",
            },
            status_code=400,
        )

    reply = QuickReply(
        workspace_id=workspace_id,
        owner_user_id=0,
        command=normalized,
        title=title.strip(),
        text=text.strip(),
        image_path=None,
    )
    db.add(reply)
    db.commit()
    sort_tokens = [token.strip() for token in (media_order or "").split(",") if token.strip()]
    await _save_quick_reply_media_files(
        db=db,
        reply=reply,
        files=[item for item in photos if item and item.filename],
        sort_order_tokens=sort_tokens,
    )
    _normalize_quick_reply_media_order(db, reply.id)

    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    webhook_workspace_url = _workspace_webhook_url(bot_settings)
    replies = _load_quick_replies_with_media(db, workspace_id=workspace_id)
    chat_metrics = get_chat_metrics(db, workspace_id=workspace_id)
    delivery_metrics = get_delivery_metrics(db, workspace_id=workspace_id)
    tenant_metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    media_send_diagnostics = get_media_diagnostics_metrics(db, workspace_id=workspace_id)
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
            "webhook_url": webhook_workspace_url,
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "media_send_diagnostics": media_send_diagnostics,
            "subscription": sub,
            "tenant_metrics": tenant_metrics,
            "manager_status_rows": [],
            "manager_status_summary": {
                "connected": 0,
                "pending": 0,
                "failed": 0,
                "not_sent": 0,
                "deactivated": 0,
                "total": 0,
            },
            "message": f"Быстрый ответ /{normalized} добавлен",
            "error": None,
            "quick_reply_edit_target": None,
        },
    )


async def _update_quick_reply_data(
    db: Session,
    *,
    reply_id: int,
    command: str,
    title: str,
    text: str,
    photos: list[UploadFile],
    media_order: str,
    remove_media_paths: str = "",
    workspace_id: int,
    actor_user_id: int | None,
    owner_user_id: int = 0,
) -> QuickReply:
    reply = (
        db.query(QuickReply)
        .filter(
            QuickReply.id == reply_id,
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == int(owner_user_id or 0),
        )
        .first()
    )
    if reply is None:
        raise HTTPException(status_code=404, detail="Быстрый ответ не найден")

    normalized = command.strip().lstrip("/").lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")
    duplicate = (
        db.query(QuickReply.id)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == int(owner_user_id or 0),
            QuickReply.command == normalized,
            QuickReply.id != reply_id,
        )
        .first()
    )
    if duplicate:
        raise HTTPException(status_code=400, detail=f"Команда /{normalized} уже существует")

    new_files = [item for item in photos if item and item.filename]
    removed_paths = {
        token.strip()
        for token in str(remove_media_paths or "").split(",")
        if token and token.strip()
    }
    for upload in new_files:
        if upload.size is not None and int(upload.size) > _QUICK_REPLY_PHOTO_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Фото в быстром ответе не должно превышать 1 МБ.")
    reply.command = normalized
    reply.title = title.strip()
    reply.text = text.strip()
    db.add(reply)
    db.commit()
    db.refresh(reply)
    if removed_paths:
        for media in _quick_reply_media_rows(db, reply.id):
            media_path_value = str(media.media_path or "").strip()
            if not media_path_value or media_path_value not in removed_paths:
                continue
            remove_quick_reply_media_asset_link(
                db,
                quick_reply_id=int(reply.id),
                media_path=media_path_value,
            )
            _try_delete_unreferenced_media_file(db, media_path=media_path_value)
            db.delete(media)
        db.commit()

    sort_tokens = [token.strip() for token in (media_order or "").split(",") if token.strip()]
    if new_files:
        await _save_quick_reply_media_files(
            db=db,
            reply=reply,
            files=new_files,
            sort_order_tokens=sort_tokens,
        )
    # Keep existing media order synchronized even when files are not re-uploaded.
    if sort_tokens:
        rows = _quick_reply_media_rows(db, reply.id)
        token_to_media: dict[str, QuickReplyMedia] = {}
        for media in rows:
            media_path_value = str(media.media_path or "").strip()
            if media_path_value:
                token_to_media[f"existing:{media_path_value}"] = media
        next_idx = 0
        changed = False
        used_ids: set[int] = set()
        for token in sort_tokens:
            media = token_to_media.get(token)
            if media is None:
                continue
            used_ids.add(int(media.id))
            if int(media.sort_order or 0) != next_idx:
                media.sort_order = next_idx
                db.add(media)
                changed = True
            next_idx += 1
        # Append any not-mentioned rows preserving current order after explicitly ordered items.
        for media in rows:
            if int(media.id) in used_ids:
                continue
            if int(media.sort_order or 0) != next_idx:
                media.sort_order = next_idx
                db.add(media)
                changed = True
            next_idx += 1
        if changed:
            db.commit()
    _normalize_quick_reply_media_order(db, reply.id)

    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action="quick_reply_updated",
            object_type="quick_reply",
            object_id=str(reply.id),
            details_json=safe_json_dumps({"command": reply.command, "title": reply.title}),
        )
    )
    db.commit()
    return reply


@app.post("/admin/quick-replies/{reply_id}/update", response_class=HTMLResponse)
async def admin_update_quick_reply(
    request: Request,
    reply_id: int,
    command: str = Form(""),
    title: str = Form(""),
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    media_order: str = Form(""),
    remove_media_paths: str = Form(""),
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
    try:
        await _update_quick_reply_data(
            db=db,
            reply_id=reply_id,
            command=command,
            title=title,
            text=text,
            photos=photos,
            media_order=media_order,
            remove_media_paths=remove_media_paths,
            workspace_id=workspace_id,
            actor_user_id=None,
            owner_user_id=0,
        )
    except HTTPException as exc:
        page = admin_page(
            request=request,
            edit_quick_reply_id=reply_id,
            _admin=_admin,
            db=db,
        )
        page.context["error"] = str(exc.detail)
        page.status_code = int(exc.status_code)
        return page
    return RedirectResponse(url="/admin", status_code=302)


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
        _delete_quick_reply_media_files(db, reply_id=reply.id)
        if reply.image_path:
            image_path_value = str(reply.image_path or "").strip()
            # Legacy safety: only delete local static file when path is actually local.
            if image_path_value.startswith("/static/"):
                relative_static_path = image_path_value.removeprefix("/static/")
                if relative_static_path:
                    img_path = Path("app/static") / relative_static_path
                    if img_path.exists():
                        try:
                            img_path.unlink()
                        except Exception:
                            # Prevent admin quick-reply delete 500 on FS edge-cases.
                            pass
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
    ui = _admin_chats_ui()
    ui["current_user"] = None
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
        if not _is_unlimited_plan(db, workspace_id=workspace_id):
            folder_limit_value = _folder_limit_for_workspace(db, workspace_id=workspace_id)
            folder_count = db.query(ChatFolder).filter(ChatFolder.workspace_id == workspace_id).count()
            if folder_count >= folder_limit_value:
                return RedirectResponse(url=f"/admin/chats?q={q}&folder_limit=1", status_code=302)
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            _assign_new_folder_to_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
                new_folder_id=created.id,
            )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    conv_qs = f"&conversation_id={conversation_id}" if conversation_id is not None else ""
    view_qs = f"&view={view}" if view else ""
    return RedirectResponse(
        url=f"/admin/chats?q={q}{folder_qs}{conv_qs}{view_qs}",
        status_code=302,
    )


@app.post("/admin/chats/folders/delete", response_class=RedirectResponse)
def admin_chat_delete_folders(
    request: Request,
    folder_ids: str = Form(""),
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    target_ids = _resolve_folder_ids_from_form(folder_ids_csv=folder_ids, folder_id=folder_id)
    removed_ids = delete_chat_folders(
        db,
        folder_ids=target_ids,
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    removed_set = {int(value) for value in removed_ids}
    next_folder_filter = (
        current_folder_id
        if current_folder_id is not None and int(current_folder_id) not in removed_set
        else None
    )
    folder_qs = f"&folder_id={int(next_folder_filter)}" if next_folder_filter is not None else ""
    removed_suffix = "1" if removed_ids else "0"
    removed_count_suffix = f"&folders_removed_count={len(removed_ids)}" if removed_ids else ""
    return RedirectResponse(
        url=f"/admin/chats?q={q}&view={view}{folder_qs}&folders_removed={removed_suffix}{removed_count_suffix}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def admin_chat_mark_unread(
    conversation_id: int,
    current_conversation_id: int | None = Form(default=None),
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
    target_conversation = (
        current_conversation_id
        if current_conversation_id is not None
        else conversation_id
    )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={target_conversation}&q={q}&view={view}&mark_read=0&unread={suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def admin_chat_move_folder(
    conversation_id: int,
    folder_id: int = Form(0),
    folder_ids: str = Form(""),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    next_folder_ids = _resolve_folder_ids_from_form(
        folder_ids_csv=folder_ids,
        folder_id=folder_id,
    )
    ok = replace_conversation_folder_links(
        db=db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    suffix = "1" if ok else "0"
    multi_suffix = "1" if ok and len(next_folder_ids) > 1 else "0"
    folder_qs = f"&folder_id={current_folder_id}" if current_folder_id is not None else ""
    return RedirectResponse(
        url=(
            f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}"
            f"&foldered={suffix}&foldered_multi={multi_suffix}{folder_qs}"
        ),
        status_code=302,
    )


@app.post("/admin/chats/bulk-action", response_class=RedirectResponse)
async def admin_chat_bulk_action(
    request: Request,
    _admin: str = Depends(require_admin),
    current_user: ServiceUser | None = Depends(get_current_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    payload = await _parse_bulk_action_request_payload(request)
    action = str(payload.get("action") or "").strip().lower()
    conversation_ids = _parse_conversation_ids_from_payload(payload)
    q_value = str(payload.get("q") or "").strip()
    view_value = str(payload.get("view") or "").strip()
    current_folder_id = _parse_optional_int(payload.get("current_folder_id"))

    moved_total = 0
    deleted_total = 0
    moved_multi = False
    workspace_id = DEFAULT_WORKSPACE_ID

    if action == "move":
        folder_ids = _parse_folder_ids_from_payload(payload)
        moved_multi = len(folder_ids) > 1
        for conversation_id in conversation_ids:
            moved = replace_conversation_folder_links(
                db=db,
                conversation_id=conversation_id,
                folder_ids=folder_ids,
                workspace_id=workspace_id,
            )
            if moved:
                moved_total += 1
    elif action == "delete":
        if current_user is not None:
            _ensure_user_can_delete_chats(current_user)
        actor_user_id = int(current_user.id) if current_user is not None else None
        for conversation_id in conversation_ids:
            deleted = delete_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
            )
            if not deleted:
                continue
            deleted_total += 1
            db.add(
                AuditLog(
                    workspace_id=workspace_id,
                    actor_user_id=actor_user_id,
                    action="customer_deleted",
                    object_type="conversation",
                    object_id=str(conversation_id),
                    details_json=safe_json_dumps({"source": "admin_chats_bulk"}),
                )
            )
            db.commit()
    else:
        raise HTTPException(status_code=400, detail="invalid_bulk_action")

    redirect_url = _bulk_operation_redirect_url(
        base_path="/admin/chats",
        q=q_value,
        view=view_value,
        folder_id=current_folder_id,
        deleted_total=deleted_total,
        moved_total=moved_total,
        moved_multi=moved_multi,
    )
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/admin/chats/{conversation_id}/pin", response_class=RedirectResponse)
def admin_chat_pin(
    request: Request,
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    pinned, reason = pin_conversation_for_user(
        db,
        workspace_id=DEFAULT_WORKSPACE_ID,
        service_user_id=0,
        conversation_id=conversation_id,
    )
    if pinned:
        db.add(
            AuditLog(
                workspace_id=DEFAULT_WORKSPACE_ID,
                actor_user_id=None,
                action="chat_pinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "admin_chats"}),
            )
        )
        db.commit()
    suffix = "1" if pinned else "0"
    limit_suffix = "&pin_limit=1" if reason == "pinned_chats_limit_exceeded" else ""
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&pinned={suffix}{limit_suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/unpin", response_class=RedirectResponse)
def admin_chat_unpin(
    request: Request,
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    unpinned = unpin_conversation_for_user(
        db,
        workspace_id=DEFAULT_WORKSPACE_ID,
        service_user_id=0,
        conversation_id=conversation_id,
    )
    if unpinned:
        db.add(
            AuditLog(
                workspace_id=DEFAULT_WORKSPACE_ID,
                actor_user_id=None,
                action="chat_unpinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "admin_chats"}),
            )
        )
        db.commit()
    suffix = "1" if unpinned else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&unpinned={suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/pins/reorder", response_class=JSONResponse)
async def admin_chat_pins_reorder(
    request: Request,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="admin_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    payload = await request.json()
    ids_raw = payload.get("conversation_ids") if isinstance(payload, dict) else None
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ordered_ids: list[int] = []
    seen: set[int] = set()
    for item in ids_raw:
        try:
            conv_id = int(item)
        except (TypeError, ValueError):
            continue
        if conv_id <= 0 or conv_id in seen:
            continue
        seen.add(conv_id)
        ordered_ids.append(conv_id)
    if not ordered_ids:
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ok = reorder_pins_for_user(
        db,
        workspace_id=DEFAULT_WORKSPACE_ID,
        service_user_id=0,
        ordered_conversation_ids=ordered_ids,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="invalid_pin_order")
    db.add(
        AuditLog(
            workspace_id=DEFAULT_WORKSPACE_ID,
            actor_user_id=None,
            action="chat_pins_reordered",
            object_type="conversation_pin",
            object_id="0",
            details_json=safe_json_dumps({"conversation_ids": ordered_ids, "scope": "admin_chats"}),
        )
    )
    db.commit()
    return JSONResponse({"ok": True, "ordered_count": len(ordered_ids)}, status_code=200)


def _render_customer_profile_page(
    *,
    request: Request,
    db: Session,
    conversation_id: int,
    q: str,
    view: str,
    folder_id: int | None,
    page_path: str,
    workspace_id: int | None,
    endpoint_query_suffix: str = "",
) -> HTMLResponse:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Чат не найден")

    meta_query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    profile_query = db.query(CustomerProfile).filter(
        CustomerProfile.customer_account_id == conversation.customer_account_id
    )
    sender_query = db.query(MessageLog.sender_account_id).filter(MessageLog.conversation_id == conversation_id)
    if workspace_id is not None:
        meta_query = meta_query.filter(ConversationMeta.workspace_id == workspace_id)
        profile_query = profile_query.filter(CustomerProfile.workspace_id == workspace_id)
        sender_query = sender_query.filter(MessageLog.workspace_id == workspace_id)

    meta = meta_query.first()
    profile = profile_query.first()
    sender_ids = [
        str(row[0])
        for row in sender_query.distinct().limit(20).all()
        if row and row[0]
    ]

    safe_q = q.strip()
    safe_view = "chat" if view.strip().lower() == "chat" else ""
    back_to_chat_url = f"{page_path}?conversation_id={conversation_id}&q={quote_plus(safe_q)}&view=chat"
    if folder_id is not None:
        back_to_chat_url += f"&folder_id={folder_id}"
    if endpoint_query_suffix:
        back_to_chat_url += endpoint_query_suffix.replace("?", "&")

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
    ]
    technical_rows = [
        {"label": "conversation_id", "value": str(conversation.id)},
        {"label": "customer_account_id", "value": conversation.customer_account_id or "—"},
        {
            "label": "source_chat_id",
            "value": (profile.source_chat_id if profile and profile.source_chat_id else "—"),
        },
        {"label": "manager_owner_id", "value": (meta.manager_owner_id if meta and meta.manager_owner_id else "—")},
        {"label": "sender_ids (последние)", "value": "\n".join(sender_ids) if sender_ids else "—"},
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
            "back_to_list_url": f"{page_path}?q={quote_plus(safe_q)}",
        },
    )


@app.get("/admin/chats/{conversation_id}/profile", response_class=HTMLResponse)
def admin_chat_customer_profile(
    request: Request,
    conversation_id: int,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_customer_profile_page(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        page_path="/admin/chats",
        workspace_id=DEFAULT_WORKSPACE_ID,
    )


@app.get("/app/chats/{conversation_id}/profile", response_class=HTMLResponse)
def app_chat_customer_profile(
    request: Request,
    conversation_id: int,
    q: str = "",
    view: str = "",
    folder_id: int | None = None,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    resolved_workspace_id, _, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=resolved_workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return _render_customer_profile_page(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        page_path="/app/chats",
        workspace_id=resolved_workspace_id,
        endpoint_query_suffix=workspace_qs,
    )


@app.post("/admin/chats/{conversation_id}/send", response_class=RedirectResponse)
async def admin_chats_send_message(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    edit_message_id: int | None = Form(default=None),
    photos: list[UploadFile] = File(default=[]),
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
    image_paths: list[str] = []
    uploads_to_validate = [item for item in photos if item and item.filename]
    if not uploads_to_validate:
        form_data = await request.form()
        legacy_photo = form_data.get("photo")
        if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
            uploads_to_validate = [legacy_photo]
    validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
    if validated_uploads:
        image_paths = _store_uploaded_images(validated_uploads)

    if edit_message_id is not None:
        updated = None
        if text_value and not image_paths:
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

    if text_value.startswith("/") and not image_paths:
        sent_ok, quick_error = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
            owner_user_id=0,
        )
        suffix = "1" if sent_ok else "0"
        redirect_url = f"/admin/chats?conversation_id={conversation_id}&quick={suffix}"
        if not sent_ok and quick_error:
            redirect_url += f"&quick_error={_encode_chat_error(quick_error)}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=redirect_url, status_code=302)

    schedule_at_value = await _resolve_schedule_at_value(request, schedule_at)
    send_error_reason = ""
    try:
        sent_ok, send_error_reason = await send_admin_chat_message(
            db=db,
            conversation_id=conversation_id,
            text=text_value,
            image_paths=image_paths,
            workspace_id=workspace_id,
            schedule_at_iso=schedule_at_value,
        )
    except Exception:
        _cleanup_uploaded_images(image_paths)
        raise
    if not sent_ok:
        _cleanup_uploaded_images(image_paths)
    scheduled_at_clean = schedule_at_value
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    redirect_url = f"/admin/chats?conversation_id={conversation_id}&{flag_name}={suffix}"
    if not sent_ok and send_error_reason:
        redirect_url += f"&send_error={_encode_chat_error(send_error_reason)}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view_value == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=redirect_url, status_code=302)


async def _handle_send_async(
    *,
    db: Session,
    conversation_id: int,
    workspace_id: int,
    text_value: str,
    image_paths: list[str],
    quick_reply_id: int = 0,
) -> JSONResponse:
    """Enqueue a message without calling Max API. Returns JSON for optimistic UI insert.

    When quick_reply_id > 0, media is fetched from the DB quick reply (no re-upload).
    When quick_reply_id = 0, image_paths from uploaded files are used.
    """
    conversation = get_conversation_by_id(db, conversation_id, workspace_id=workspace_id)
    if conversation is None:
        return JSONResponse({"ok": False, "error": "Диалог не найден"}, status_code=404)

    qr_photo_urls: list[str] = []
    if quick_reply_id > 0:
        media_rows = (
            db.query(QuickReplyMedia)
            .filter(QuickReplyMedia.quick_reply_id == quick_reply_id)
            .order_by(QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
            .all()
        )
        if media_rows:
            qr_photo_urls = [str(row.media_path or "").strip() for row in media_rows if str(row.media_path or "").strip()]
        if not qr_photo_urls:
            qr_photo_urls = get_quick_reply_media_paths(db, quick_reply_id=quick_reply_id)
        if not qr_photo_urls:
            qr = db.query(QuickReply).filter(QuickReply.id == quick_reply_id).first()
            if qr and qr.image_path:
                qr_photo_urls = [str(qr.image_path).strip()]

    photo_urls = qr_photo_urls if qr_photo_urls else image_paths
    has_content = bool(text_value or photo_urls)
    if not has_content:
        return JSONResponse({"ok": False, "error": "Нет содержимого"}, status_code=400)

    if photo_urls:
        await queue_only_send_media_group(
            db,
            conversation_id=conversation_id,
            target_chat_id=conversation.chat_id,
            target_user_id=conversation.customer_account_id,
            photo_urls=photo_urls,
            text=text_value,
            text_format="markdown",
            source="bot_system",
        )
    else:
        await queue_only_send_text(
            db,
            conversation_id=conversation_id,
            target_chat_id=conversation.chat_id,
            target_user_id=conversation.customer_account_id,
            text=text_value,
            text_format="markdown",
            source="bot_system",
        )
    msg = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.conversation_id == conversation_id,
            ChatMessage.direction == "bot",
        )
        .order_by(ChatMessage.id.desc())
        .first()
    )
    asyncio.create_task(_kick_outbox_once())
    msg_id = int(msg.id) if msg else 0
    created_at = getattr(msg, "created_at", None)
    display_image_urls = photo_urls if photo_urls else image_paths
    return JSONResponse({
        "ok": True,
        "message": {
            "id": msg_id,
            "text": text_value,
            "direction": "bot",
            "source": "bot_system",
            "delivery_state": "queued",
            "is_read_by_customer": False,
            "image_urls": display_image_urls,
            "image_url": display_image_urls[0] if display_image_urls else "",
            "created_at_label": _to_moscow_chat_label(created_at),
            "is_scheduled_pending": False,
            "delivery_next_retry_at": "",
            "delivery_error": "",
        },
    })


@app.post("/admin/chats/{conversation_id}/send-async")
async def admin_chats_send_message_async(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    quick_reply_id: int = Form(0),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _check_rate_limit_or_raise(
        request,
        scope="admin_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    workspace_id = DEFAULT_WORKSPACE_ID
    text_value = text.strip()
    image_paths: list[str] = []
    if not (quick_reply_id > 0):
        uploads_to_validate = [item for item in photos if item and item.filename]
        if not uploads_to_validate:
            form_data = await request.form()
            legacy_photo = form_data.get("photo")
            if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
                uploads_to_validate = [legacy_photo]
        validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
        image_paths = _store_uploaded_images(validated_uploads) if validated_uploads else []
    return await _handle_send_async(
        db=db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
        text_value=text_value,
        image_paths=image_paths,
        quick_reply_id=int(quick_reply_id or 0),
    )


@app.post("/app/chats/{conversation_id}/send-async")
async def app_chats_send_message_async(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    quick_reply_id: int = Form(0),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _check_rate_limit_or_raise(
        request,
        scope="admin_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    resolved_workspace_id, _scoped_workspace, _is_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    text_value = text.strip()
    image_paths: list[str] = []
    if not (quick_reply_id > 0):
        uploads_to_validate = [item for item in photos if item and item.filename]
        if not uploads_to_validate:
            form_data = await request.form()
            legacy_photo = form_data.get("photo")
            if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
                uploads_to_validate = [legacy_photo]
        validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
        image_paths = _store_uploaded_images(validated_uploads) if validated_uploads else []
    return await _handle_send_async(
        db=db,
        conversation_id=conversation_id,
        workspace_id=resolved_workspace_id,
        text_value=text_value,
        image_paths=image_paths,
        quick_reply_id=int(quick_reply_id or 0),
    )


@app.post("/mini/manager/chats/{conversation_id}/send-async")
async def mini_manager_chats_send_message_async(
    request: Request,
    conversation_id: int,
    token: str = Form(""),
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    quick_reply_id: int = Form(0),
    db: Session = Depends(get_db),
) -> JSONResponse:
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    _check_rate_limit_or_raise(
        request,
        scope="admin_send",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 5),
    )
    text_value = text.strip()
    image_paths: list[str] = []
    if not (quick_reply_id > 0):
        uploads_to_validate = [item for item in photos if item and item.filename]
        if not uploads_to_validate:
            form_data = await request.form()
            legacy_photo = form_data.get("photo")
            if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
                uploads_to_validate = [legacy_photo]
        validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
        image_paths = _store_uploaded_images(validated_uploads) if validated_uploads else []
    return await _handle_send_async(
        db=db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
        text_value=text_value,
        image_paths=image_paths,
        quick_reply_id=int(quick_reply_id or 0),
    )


@app.post("/admin/chats/{conversation_id}/quick-reply", response_class=RedirectResponse)
async def admin_chats_send_quick_reply(
    conversation_id: int,
    command: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    workspace_id = DEFAULT_WORKSPACE_ID
    sent_ok, quick_error = await send_admin_quick_reply(
        db=db,
        conversation_id=conversation_id,
        command_text=command,
        workspace_id=workspace_id,
        owner_user_id=0,
    )
    suffix = "1" if sent_ok else "0"
    quick_qs = f"&quick_error={_encode_chat_error(quick_error)}" if (not sent_ok and quick_error) else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&quick={suffix}{quick_qs}",
        status_code=302,
    )


@app.get("/app/chats/{conversation_id}/quick-options")
def app_chats_quick_options(
    conversation_id: int,
    q: str = "",
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> dict:
    resolved_workspace_id, _scoped_workspace, _is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    quick_replies = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == resolved_workspace_id,
            QuickReply.owner_user_id == quick_reply_owner_id,
            QuickReply.is_active.is_(True),
        )
        .order_by(QuickReply.command.asc())
        .all()
    )
    query = q.strip().lstrip("/").lower()
    items = []
    for item in quick_replies:
        if query:
            hay = f"{item.command} {item.title} {item.text}".lower()
            if query not in hay:
                continue
        items.append(
            {
                "command": item.command,
                "title": item.title,
                "text": item.text,
                "has_image": bool(item.image_path),
            }
        )
        if len(items) >= 12:
            break
    return {"conversation_id": conversation_id, "items": items}


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
            QuickReply.owner_user_id == 0,
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
    current_user: ServiceUser | None = Depends(get_current_service_user),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    if current_user is not None:
        _ensure_user_can_delete_chats(current_user)
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
    current_user: ServiceUser = Depends(require_service_user),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _ensure_user_can_delete_chats(current_user)
    workspace_id = DEFAULT_WORKSPACE_ID
    deleted = delete_conversation(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )
    if deleted:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_deleted",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"source": "admin_chats"}),
            )
        )
        db.commit()
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


@app.post("/admin/chats/{conversation_id}/block-user", response_class=RedirectResponse)
def admin_chats_block_conversation_customer(
    request: Request,
    conversation_id: int,
    block_reason: str = Form(""),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    current_user: ServiceUser = Depends(require_service_user),
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
    blocked = block_conversation_customer(
        db,
        conversation_id=conversation_id,
        actor_user_id=int(current_user.id),
        reason=block_reason,
        workspace_id=workspace_id,
    )
    if blocked:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_blocked",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"reason": (block_reason or "").strip()}),
            )
        )
        db.commit()
    suffix = "1" if blocked else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={quote_plus(q.strip())}&view={view}&blocked={suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/unblock-user", response_class=RedirectResponse)
def admin_chats_unblock_conversation_customer(
    request: Request,
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    current_user: ServiceUser = Depends(require_service_user),
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
    unblocked = unblock_conversation_customer(
        db,
        conversation_id=conversation_id,
        actor_user_id=int(current_user.id),
        workspace_id=workspace_id,
    )
    if unblocked:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_unblocked",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"source": "admin_chats"}),
            )
        )
        db.commit()
    suffix = "1" if unblocked else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={quote_plus(q.strip())}&view={view}&unblocked={suffix}{folder_qs}",
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
    expected_manager_ids = _parse_manager_ids(settings_db.manager_account_id)
    if expected_manager_ids and manager_id not in expected_manager_ids:
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


def _encode_chat_error(reason: str | None) -> str:
    text = str(reason or "").strip()
    if not text:
        return ""
    return quote_plus(text[:400])


def _admin_chats_ui() -> dict[str, str | bool]:
    return {
        "page_title": "Max Admin Chats",
        "page_path": "/admin/chats",
        "page_query_prefix": "/admin/chats?",
        "updates_endpoint": "/admin/chats/updates",
        "ws_endpoint": "/admin/chats/ws",
        "access_token": "",
        "create_folder_action": "/admin/chats/folders",
        "show_admin_nav": True,
        "settings_href": "/admin",
        "logout_action": "/admin/logout",
        "show_profile_links": True,
        "show_delete_user": True,
        "show_rename_user": True,
        "show_block_user": True,
        "allow_message_edit_actions": True,
        "send_action_prefix": "/admin/chats/",
        "send_action_suffix": "",
        "delete_user_action_prefix": "/admin/chats/",
        "rename_user_action_prefix": "/admin/chats/",
        "block_user_action_prefix": "/admin/chats/",
        "unblock_user_action_prefix": "/admin/chats/",
        "profile_href_prefix": "/admin/chats/",
        "mark_unread_prefix": "/admin/chats/",
        "move_folder_prefix": "/admin/chats/",
        "pin_prefix": "/admin/chats/",
        "unpin_prefix": "/admin/chats/",
        "pins_reorder_endpoint": "/admin/chats/pins/reorder",
        "show_pins_info": True,
        "create_folder_endpoint": "/admin/chats/folders",
        "folder_delete_endpoint": "/admin/chats/folders/delete",
        "delete_message_prefix": "/admin/chats/",
        "bulk_action_endpoint": "/admin/chats/bulk-action",
        "can_delete_chats": True,
        "endpoint_query_suffix": "",
    }


def _manager_mini_ui(token: str) -> dict[str, str | bool]:
    token_value = quote_plus(token)
    query_suffix = f"?token={token_value}"
    return {
        "page_title": "Max Manager Mini App",
        "page_path": "/mini/manager",
        "page_query_prefix": f"/mini/manager?token={token_value}&",
        "updates_endpoint": "/mini/manager/chats/updates",
        "ws_endpoint": "/mini/manager/chats/ws",
        "access_token": token,
        "create_folder_action": f"/mini/manager/chats/folders{query_suffix}",
        "show_admin_nav": True,
        "settings_href": f"/mini/manager?token={token_value}",
        "logout_action": "/app/logout",
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
        "pin_prefix": "/mini/manager/chats/",
        "unpin_prefix": "/mini/manager/chats/",
        "pins_reorder_endpoint": "/mini/manager/chats/pins/reorder",
        "show_pins_info": False,
        "create_folder_endpoint": "/mini/manager/chats/folders",
        "folder_delete_endpoint": "/mini/manager/chats/folders/delete",
        "delete_message_prefix": "/mini/manager/chats/",
        "bulk_action_endpoint": "/mini/manager/chats/bulk-action",
        "can_delete_chats": False,
        "endpoint_query_suffix": query_suffix,
    }


def _app_chats_ui() -> dict[str, str | bool]:
    return {
        "ws_endpoint": "/app/chats/ws",
    }


def _build_pins_redirect_url(
    *,
    base_path: str,
    conversation_id: int,
    q: str,
    view: str,
    folder_id: int | None = None,
    pinned: bool | None = None,
    unpinned: bool | None = None,
    pin_limit: bool = False,
    endpoint_query_suffix: str = "",
) -> str:
    url = f"{base_path}?conversation_id={conversation_id}&q={q}&view={view}"
    if pinned is not None:
        url += f"&pinned={'1' if pinned else '0'}"
    if unpinned is not None:
        url += f"&unpinned={'1' if unpinned else '0'}"
    if pin_limit:
        url += "&pin_limit=1"
    if folder_id is not None:
        url += f"&folder_id={folder_id}"
    if endpoint_query_suffix:
        url += endpoint_query_suffix.replace("?", "&")
    return url


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

    if not current_user.is_active or current_user.is_blocked:
        raise HTTPException(status_code=403, detail="Доступ запрещен")

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


def _render_miniapp_landing(request: Request) -> HTMLResponse:
    context = {
        "request": request,
        "channel_url": "https://max.ru/id372400681880_biz",
    }
    try:
        return templates.TemplateResponse(request, "miniapp_landing.html", context)
    except TemplateNotFound:
        # Safe fallback to prevent 500 in case template deployment lags behind code.
        return HTMLResponse(
            (
                "<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                "<title>FeedPilot mini-app</title></head><body style='margin:0;"
                "min-height:100vh;display:grid;place-items:center;background:#0f1730;"
                "font-family:Inter,Arial,sans-serif;color:#eef4ff;'>"
                "<main style='max-width:560px;padding:20px;text-align:center;'>"
                "<h1 style='margin:0 0 14px;font-size:26px;line-height:1.2;'>"
                "Хочешь больше выгодных предложений с кэшбэком, тогда подпишись на наш канал"
                "</h1><a href='https://max.ru/id372400681880_biz' target='_blank' rel='noopener noreferrer' "
                "style='display:inline-block;padding:12px 20px;border-radius:12px;"
                "background:#2f8dff;color:#fff;text-decoration:none;font-weight:800;'>"
                "Перейти в канал</a></main></body></html>"
            ),
            status_code=200,
        )


def _is_miniapp_landing_request(request: Request) -> bool:
    miniapp_flag = str(
        request.query_params.get("miniapp")
        or request.query_params.get("mini")
        or request.query_params.get("from")
        or request.query_params.get("source")
        or ""
    ).strip().lower()
    if miniapp_flag in {"1", "true", "yes", "on"}:
        return True
    if miniapp_flag in {"miniapp", "mini_app", "max-miniapp", "max_miniapp", "max"}:
        return True

    requested_with = str(request.headers.get("x-requested-with") or "").strip().lower()
    if any(marker in requested_with for marker in ("max", "miniapp", "mini_app")):
        return True

    x_miniapp = str(
        request.headers.get("x-miniapp")
        or request.headers.get("x-max-miniapp")
        or ""
    ).strip().lower()
    if x_miniapp in {"1", "true", "yes", "on"}:
        return True

    user_agent = str(request.headers.get("user-agent") or "").strip().lower()
    if any(marker in user_agent for marker in ("max-miniapp", "maxapp", "max webview", "maxwebview", "miniapp")):
        return True
    if "max" in user_agent and any(marker in user_agent for marker in ("webview", "qtwebengine", "messenger")):
        return True

    for header_name in ("referer", "origin"):
        raw_value = str(request.headers.get(header_name) or "").strip().lower()
        if not raw_value:
            continue
        try:
            parsed = urlsplit(raw_value)
        except Exception:
            continue
        host = str(parsed.netloc or "").strip().lower()
        if host.endswith("max.ru") or host.endswith(".max.ru"):
            return True

    fetch_dest = str(request.headers.get("sec-fetch-dest") or "").strip().lower()
    if fetch_dest in {"iframe", "embed", "object"}:
        referer_value = str(request.headers.get("referer") or "").strip().lower()
        if "max.ru" in referer_value:
            return True

    return False


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


def _workspace_restricted_login_error(db: Session, username: str) -> str | None:
    normalized = (username or "").strip().lower()
    if not normalized:
        return None
    user = db.query(ServiceUser).filter(ServiceUser.username == normalized).first()
    if user is None or user.role == "superadmin" or not user.workspace_id:
        return None
    workspace = db.query(Workspace).filter(Workspace.id == user.workspace_id).first()
    if workspace is None:
        return None
    if not workspace.is_active or workspace.is_suspended:
        support_row = db.query(PlatformSettings).order_by(PlatformSettings.id.asc()).first()
        tech_email = (
            (support_row.technical_support_email if support_row else "")
            or (settings.support_tech_email or "").strip()
            or "support@example.com"
        )
        finance_email = (
            (support_row.billing_support_email if support_row else "")
            or (settings.support_finance_email or "").strip()
            or "billing@example.com"
        )
        return (
            "Действия вашего профиля ограничены. "
            f"Обратитесь в технический отдел: {tech_email} "
            f"или в финансовый отдел: {finance_email}."
        )
    return None


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
    if _is_miniapp_landing_request(request):
        return _render_miniapp_landing(request)
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
    if _is_miniapp_landing_request(request):
        return _render_miniapp_landing(request)
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
    login_result = sign_in_service_user_with_reason(db, username=username, password=password)
    user = login_result.get("user")
    if user is None:
        if login_result.get("status") in {"workspace_suspended", "workspace_inactive"}:
            restricted_message = _workspace_restricted_login_error(db, username)
            if restricted_message:
                return _render_app_landing(
                    request,
                    login_error=restricted_message,
                    default_username=username.strip().lower(),
                    view="login",
                )
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
    photos: list[UploadFile] = File(default=[]),
    media_order: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_QUICK_REPLY_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    quick_replies_limit_value = _quick_reply_limit_for_workspace(db, workspace_id=workspace_id)
    quick_replies_count = (
        db.query(QuickReply)
        .filter(QuickReply.workspace_id == workspace_id)
        .count()
    )
    if quick_replies_count >= quick_replies_limit_value:
        page = _render_app_settings_page(
            request,
            db=db,
            current_user=current_user,
            error="Ваш тарифный план не позволяет создавать больше быстрых ответов.",
        )
        page.status_code = 400
        return page
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    normalized = command.strip().lstrip("/").lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")
    exists = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == quick_reply_owner_id,
            QuickReply.command == normalized,
        )
        .first()
    )
    if exists:
        return RedirectResponse(url="/app/settings", status_code=302)
    reply = QuickReply(
        workspace_id=workspace_id,
        owner_user_id=quick_reply_owner_id,
        command=normalized,
        title=title.strip(),
        text=text.strip(),
        image_path=None,
    )
    db.add(reply)
    db.commit()
    sort_tokens = [token.strip() for token in (media_order or "").split(",") if token.strip()]
    await _save_quick_reply_media_files(
        db=db,
        reply=reply,
        files=[item for item in photos if item and item.filename],
        sort_order_tokens=sort_tokens,
    )
    sync_quick_reply_media_asset_links(
        db,
        quick_reply_id=int(reply.id),
        workspace_id=int(reply.workspace_id or workspace_id),
    )
    _normalize_quick_reply_media_order(db, reply.id)
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/quick-replies/{reply_id}/update", response_class=HTMLResponse)
async def app_update_quick_reply(
    request: Request,
    reply_id: int,
    command: str = Form(""),
    title: str = Form(""),
    text: str = Form(""),
    photos: list[UploadFile] = File(default=[]),
    media_order: str = Form(""),
    remove_media_paths: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_QUICK_REPLY_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    try:
        reply = await _update_quick_reply_data(
            db=db,
            reply_id=reply_id,
            command=command,
            title=title,
            text=text,
            photos=photos,
            media_order=media_order,
            remove_media_paths=remove_media_paths,
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            owner_user_id=quick_reply_owner_id,
        )
    except HTTPException as exc:
        page = _render_app_settings_page(
            request,
            db=db,
            current_user=current_user,
            error=str(exc.detail),
            edit_quick_reply_id=reply_id,
        )
        page.status_code = int(exc.status_code)
        return page
    return RedirectResponse(url=f"/app/settings?edited_quick_reply={reply.id}", status_code=302)


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
    if current_user.role not in _WORKSPACE_QUICK_REPLY_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    reply = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == quick_reply_owner_id,
            QuickReply.id == reply_id,
        )
        .first()
    )
    if reply:
        _delete_quick_reply_media_files(db, reply_id=reply.id)
        if reply.image_path:
            image_path_value = str(reply.image_path or "").strip()
            # Legacy safety: only delete local static file when path is actually local.
            if image_path_value.startswith("/static/"):
                relative_static_path = image_path_value.removeprefix("/static/")
                if relative_static_path:
                    img_path = Path("app/static") / relative_static_path
                    if img_path.exists():
                        try:
                            img_path.unlink()
                        except Exception:
                            # Prevent app-side quick-reply delete 500 on FS edge-cases.
                            pass
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
    # Chat page can be reopened often during active work; keep a wider bucket.
    _check_rate_limit_or_raise(
        request,
        scope="app_view_page",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 20),
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
    can_delete_chats = _user_can_delete_chats(current_user)
    ui.update(
        {
            "page_title": page_title,
            "page_path": "/app/chats",
            "page_query_prefix": query_scope_prefix,
            "updates_endpoint": "/app/chats/updates",
            "ws_endpoint": "/app/chats/ws",
            "create_folder_action": create_folder_action,
            "show_admin_nav": True,
            "settings_href": settings_href,
            "logout_action": "/app/logout",
            "show_delete_user": can_delete_chats,
            "show_rename_user": True,
            "show_block_user": True,
            "send_action_prefix": "/app/chats/",
            "send_action_suffix": endpoint_scope_suffix,
            "delete_user_action_prefix": "/app/chats/",
            "rename_user_action_prefix": "/app/chats/",
            "block_user_action_prefix": "/app/chats/",
            "unblock_user_action_prefix": "/app/chats/",
            "profile_href_prefix": "/app/chats/",
            "mark_unread_prefix": "/app/chats/",
            "move_folder_prefix": "/app/chats/",
            "pin_prefix": "/app/chats/",
            "unpin_prefix": "/app/chats/",
            "pins_reorder_endpoint": "/app/chats/pins/reorder",
            "show_pins_info": True,
            "create_folder_endpoint": "/app/chats/folders",
            "folder_delete_endpoint": "/app/chats/folders/delete",
            "delete_message_prefix": "/app/chats/",
            "bulk_action_endpoint": "/app/chats/bulk-action",
            "can_delete_chats": can_delete_chats,
            "endpoint_query_suffix": endpoint_scope_suffix,
        }
    )
    ui["current_user"] = current_user
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
    edit_quick_reply_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    return _render_app_settings_page(
        request,
        db=db,
        current_user=current_user,
        edit_quick_reply_id=edit_quick_reply_id,
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
    if current_user.role not in _WORKSPACE_USER_ROLES:
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
    if current_user.role not in _WORKSPACE_USER_ROLES:
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
async def app_update_settings(
    request: Request,
    prestart_message: str = Form(""),
    start_message: str = Form(""),
    after_phone_message: str = Form(""),
    bot_token: str = Form(""),
    manager_account_id: str = Form(""),
    admin_account_id: str = Form(""),
    routing_mode: str = Form("round_robin"),
    request_customer_phone: str = Form(""),
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
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    form_data = await request.form()
    settings_section = str(form_data.get("settings_section", "general") or "general").strip().lower()
    if settings_section == "business_hours":
        business_payload = _extract_business_hours_form(form_data)
        replace_workspace_business_hours(
            db,
            workspace_id=workspace_id,
            enabled=bool(business_payload.get("enabled", False)),
            timezone_name=str(business_payload.get("timezone") or DEFAULT_BUSINESS_TIMEZONE),
            offhours_message=str(business_payload.get("offhours_message") or DEFAULT_OFFHOURS_MESSAGE),
            cooldown_seconds=int(
                business_payload.get("cooldown_seconds") or DEFAULT_OFFHOURS_COOLDOWN_SECONDS
            ),
            slots=list(business_payload.get("slots") or []),
            exceptions=[],
            commit=False,
        )
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="business_hours_updated",
                object_type="workspace_business_hours",
                object_id=str(workspace_id),
                details_json=safe_json_dumps(
                    {
                        "enabled": bool(business_payload.get("enabled", False)),
                        "timezone": str(business_payload.get("timezone") or ""),
                        "slots_count": len(list(business_payload.get("slots") or [])),
                        "cooldown_seconds": int(
                            business_payload.get("cooldown_seconds")
                            or DEFAULT_OFFHOURS_COOLDOWN_SECONDS
                        ),
                    }
                ),
            )
        )
        db.commit()
        return RedirectResponse(url="/app/settings", status_code=302)

    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    previous_webhook_key = (settings_row.webhook_key or "").strip()
    token_clean = (bot_token or "").strip()
    existing_token = (settings_row.bot_token or "").strip()
    if token_clean:
        old_token = existing_token
        settings_row.bot_token = token_clean
        # Token was explicitly provided: allow rotating token to a new value.
        if old_token and old_token != token_clean:
            db.add(
                AuditLog(
                    workspace_id=workspace_id,
                    actor_user_id=current_user.id,
                    action="workspace_bot_token_updated",
                    object_type="bot_settings",
                    object_id=str(settings_row.id),
                    details_json=safe_json_dumps({"source": "app_settings"}),
                )
            )
    elif not existing_token:
        page = _render_app_settings_page(
            request,
            db=db,
            current_user=current_user,
            error="Укажите токен вашего бота Max.",
        )
        page.status_code = 400
        return page
    _ensure_workspace_webhook_key(settings_row)
    should_sync_webhook = bool(token_clean) or not previous_webhook_key
    if should_sync_webhook:
        subscribed_ok, subscribe_error = await _auto_subscribe_workspace_webhook(settings_row)
        if not subscribed_ok:
            db.rollback()
            page = _render_app_settings_page(
                request,
                db=db,
                current_user=current_user,
                error=_user_friendly_bot_connection_error(),
            )
            page.status_code = 400
            return page
    settings_row.admin_account_id = admin_account_id.strip()
    mode = (routing_mode or "round_robin").strip().lower()
    if mode not in {"round_robin", "random"}:
        mode = "round_robin"
    settings_row.routing_mode = mode
    settings_row.request_customer_phone = str(request_customer_phone or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }
    db.add(settings_row)
    _drop_legacy_template_unique_index(db)
    _upsert_template_values_single_commit(
        db,
        workspace_id=workspace_id,
        prestart_message=prestart_message,
        start_message=start_message,
        after_phone_message=after_phone_message,
    )
    db.commit()
    # Manager add/remove is now handled in the manager status block actions.
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/settings/delete-bot-token", response_class=RedirectResponse)
async def app_delete_bot_token(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    had_token = bool((settings_row.bot_token or "").strip())
    if had_token:
        unsubscribed_ok, unsubscribe_error = await _auto_unsubscribe_workspace_webhook(settings_row)
        if not unsubscribed_ok:
            db.rollback()
            page = _render_app_settings_page(
                request,
                db=db,
                current_user=current_user,
                error=_user_friendly_bot_connection_error(),
            )
            page.status_code = 400
            return page
        settings_row.bot_token = ""
        db.add(settings_row)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="workspace_bot_token_deleted",
                object_type="bot_settings",
                object_id=str(settings_row.id),
                details_json='{"source":"app_settings"}',
            )
        )
        db.commit()
    return RedirectResponse(url="/app/settings", status_code=302)


@app.post("/app/settings/business-hours/preview", response_class=JSONResponse)
def app_settings_business_hours_preview(
    request: Request,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    evaluation = evaluate_workspace_business_hours(db, workspace_id=workspace_id)
    next_open_local = evaluation.get("next_open_local")
    if isinstance(next_open_local, datetime):
        next_iso = next_open_local.isoformat()
    else:
        next_iso = None
    return JSONResponse(
        {
            "ok": True,
            "enabled": bool(evaluation.get("enabled", False)),
            "is_open": bool(evaluation.get("is_open", False)),
            "timezone": str(evaluation.get("timezone") or ""),
            "next_open_label": str(evaluation.get("next_open_label") or ""),
            "next_open_at_local": next_iso,
        },
        status_code=200,
    )


@app.post("/app/settings/copy-manager-link", response_class=JSONResponse)
async def app_copy_manager_link(
    request: Request,
    copy_manager_id: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    target_manager_id = (copy_manager_id or "").strip()
    if not target_manager_id:
        return JSONResponse(
            {"ok": False, "error": 'Введите ID менеджера в поле "Добавление нового менеджера".'},
            status_code=400,
        )

    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    existing_ids = _parse_manager_ids(settings_row.manager_account_id)
    merged_ids = list(existing_ids)
    if target_manager_id not in merged_ids:
        merged_ids.append(target_manager_id)

    manager_limit_value = _manager_limit_for_workspace(db, workspace_id=workspace_id)
    if len(merged_ids) > manager_limit_value:
        return JSONResponse(
            {
                "ok": False,
                "error": "Ваш тарифный план не позволяет добавлять больше менеджеров.",
            },
            status_code=400,
        )

    settings_row.manager_account_id = _normalize_manager_ids(",".join(merged_ids))
    db.add(settings_row)
    db.commit()

    ok, msg, invite_link = _build_manager_invite_link_for_copy(
        db,
        workspace_id=workspace_id,
        manager_id=target_manager_id,
        actor_user_id=current_user.id,
    )
    if not ok:
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    return JSONResponse({"ok": True, "message": msg, "link": invite_link}, status_code=200)


@app.post("/app/settings/remove-manager", response_class=JSONResponse)
async def app_remove_manager(
    request: Request,
    remove_manager_id: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    target_manager_id = (remove_manager_id or "").strip()
    if not target_manager_id:
        return JSONResponse({"ok": False, "error": "Не указан ID менеджера для удаления."}, status_code=400)

    settings_row = get_or_create_settings(db, workspace_id=workspace_id)
    manager_ids = _parse_manager_ids(settings_row.manager_account_id)
    updated_ids = [item for item in manager_ids if item != target_manager_id]
    settings_row.manager_account_id = _normalize_manager_ids(",".join(updated_ids))
    db.add(settings_row)

    manager_rows = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.max_account_id == target_manager_id,
        )
        .all()
    )
    revoked_user_ids: list[int] = []
    for row in manager_rows:
        row.is_active = False
        row.is_blocked = True
        db.add(row)
        revoked_user_ids.append(int(row.id))

    db.query(ManagerInvite).filter(
        ManagerInvite.workspace_id == workspace_id,
        ManagerInvite.max_account_id == target_manager_id,
    ).delete(synchronize_session=False)

    object_id = str(revoked_user_ids[0]) if revoked_user_ids else target_manager_id
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=current_user.id,
            action="manager_removed",
            object_type="service_user",
            object_id=object_id,
            details_json=safe_json_dumps(
                {
                    "max_account_id": target_manager_id,
                    "revoked_user_ids": revoked_user_ids,
                    "removed_from_settings": target_manager_id in manager_ids,
                }
            ),
        )
    )
    db.commit()
    for user_id in revoked_user_ids:
        revoke_user_sessions(db, user_id=user_id)

    if target_manager_id not in manager_ids:
        return JSONResponse(
            {"ok": True, "message": f"Менеджер {target_manager_id} уже отсутствует в настройках."},
            status_code=200,
        )
    return JSONResponse({"ok": True, "message": f"Менеджер {target_manager_id} удален."}, status_code=200)


@app.post("/app/settings/manager-delete-permission", response_class=JSONResponse)
async def app_settings_toggle_manager_delete_permission(
    request: Request,
    manager_max_account_id: str = Form(""),
    can_delete_chats: str = Form(""),
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_settings",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 3),
    )
    if current_user.role not in _WORKSPACE_USER_ROLES:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    workspace_id = current_user.workspace_id or DEFAULT_WORKSPACE_ID
    manager_id = str(manager_max_account_id or "").strip()
    if not manager_id:
        return JSONResponse({"ok": False, "error": "Не указан ID менеджера."}, status_code=400)
    manager_row = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.max_account_id == manager_id,
        )
        .first()
    )
    if manager_row is None:
        return JSONResponse({"ok": False, "error": "Менеджер не найден."}, status_code=404)
    next_allowed = str(can_delete_chats or "").strip().lower() in {"1", "true", "yes", "on"}
    manager_row.can_delete_chats = bool(next_allowed)
    db.add(manager_row)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=int(current_user.id),
            action="manager_delete_permission_updated",
            object_type="service_user",
            object_id=str(manager_row.id),
            details_json=safe_json_dumps(
                {
                    "max_account_id": manager_id,
                    "can_delete_chats": bool(next_allowed),
                }
            ),
        )
    )
    db.commit()
    return JSONResponse(
        {
            "ok": True,
            "can_delete_chats": bool(next_allowed),
            "message": (
                f"Разрешение на удаление для {manager_id}: "
                + ("разрешено" if next_allowed else "запрещено")
            ),
        },
        status_code=200,
    )


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
    if current_user.role not in _WORKSPACE_USER_ROLES:
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
    if current_user.role not in _WORKSPACE_USER_ROLES:
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


@app.post("/app/superadmin/tariffs", response_class=RedirectResponse)
def app_superadmin_create_tariff_plan(
    request: Request,
    code: str = Form(""),
    name: str = Form(""),
    description: str = Form(""),
    manager_limit: int = Form(3),
    dialogs_limit: int = Form(500),
    messages_per_month_limit: int = Form(5000),
    quick_replies_limit: int = Form(10),
    folders_limit: int = Form(10),
    pinned_chats_limit: int = Form(5),
    billing_product_code: str = Form(""),
    billing_price_code: str = Form(""),
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
    payload = _sanitize_tariff_plan_form(
        code=code,
        name=name,
        description=description,
        manager_limit=manager_limit,
        dialogs_limit=dialogs_limit,
        messages_per_month_limit=messages_per_month_limit,
        quick_replies_limit=quick_replies_limit,
        folders_limit=folders_limit,
        pinned_chats_limit=pinned_chats_limit,
        billing_product_code=billing_product_code,
        billing_price_code=billing_price_code,
        fallback_code="basic",
    )
    code_value = str(payload["code"])
    if _get_tariff_plan_by_code(db, plan_code=code_value) is not None:
        return RedirectResponse(
            url="/app/superadmin/plans?error=tariff_exists",
            status_code=302,
        )
    plan = TariffPlan(
        code=code_value,
        name=str(payload["name"]),
        description=str(payload["description"]),
        manager_limit=int(payload["manager_limit"]),
        dialogs_limit=int(payload["dialogs_limit"]),
        messages_per_month_limit=int(payload["messages_per_month_limit"]),
        quick_replies_limit=int(payload["quick_replies_limit"]),
        folders_limit=int(payload["folders_limit"]),
        pinned_chats_limit=int(payload["pinned_chats_limit"]),
        is_default=False,
        is_active=True,
        billing_product_code=str(payload["billing_product_code"]),
        billing_price_code=str(payload["billing_price_code"]),
    )
    db.add(plan)
    db.flush()
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="tariff_plan_created",
            object_type="tariff_plan",
            object_id=str(plan.id),
            details_json=safe_json_dumps(
                {
                    "code": plan.code,
                    "name": plan.name,
                    "limits": _tariff_plan_limits_dict(plan),
                    "billing_product_code": plan.billing_product_code,
                    "billing_price_code": plan.billing_price_code,
                }
            ),
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/plans?tariff_created=1", status_code=302)


@app.post("/app/superadmin/tariffs/{plan_id}", response_class=RedirectResponse)
def app_superadmin_update_tariff_plan(
    request: Request,
    plan_id: int,
    name: str = Form(""),
    description: str = Form(""),
    manager_limit: int = Form(3),
    dialogs_limit: int = Form(500),
    messages_per_month_limit: int = Form(5000),
    quick_replies_limit: int = Form(10),
    folders_limit: int = Form(10),
    pinned_chats_limit: int = Form(5),
    billing_product_code: str = Form(""),
    billing_price_code: str = Form(""),
    is_active: str = Form("1"),
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
    plan = db.query(TariffPlan).filter(TariffPlan.id == int(plan_id)).first()
    if plan is None:
        return RedirectResponse(url="/app/superadmin/plans?error=tariff_missing", status_code=302)
    payload = _sanitize_tariff_plan_form(
        code=plan.code,
        name=name,
        description=description,
        manager_limit=manager_limit,
        dialogs_limit=dialogs_limit,
        messages_per_month_limit=messages_per_month_limit,
        quick_replies_limit=quick_replies_limit,
        folders_limit=folders_limit,
        pinned_chats_limit=pinned_chats_limit,
        billing_product_code=billing_product_code,
        billing_price_code=billing_price_code,
        fallback_code=plan.code,
    )
    plan.name = str(payload["name"])
    plan.description = str(payload["description"])
    plan.manager_limit = int(payload["manager_limit"])
    plan.dialogs_limit = int(payload["dialogs_limit"])
    plan.messages_per_month_limit = int(payload["messages_per_month_limit"])
    plan.quick_replies_limit = int(payload["quick_replies_limit"])
    plan.folders_limit = int(payload["folders_limit"])
    plan.pinned_chats_limit = int(payload["pinned_chats_limit"])
    plan.billing_product_code = str(payload["billing_product_code"])
    plan.billing_price_code = str(payload["billing_price_code"])
    plan.is_active = str(is_active or "").strip().lower() in {"1", "true", "yes", "on"}
    if plan.is_default:
        plan.is_active = True
    db.add(plan)

    updated_subscriptions = (
        db.query(Subscription)
        .filter(Subscription.plan_code == plan.code)
        .all()
    )
    for sub in updated_subscriptions:
        _sync_subscription_cached_limits_from_plan(db, subscription=sub)
        db.add(sub)

    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="tariff_plan_updated",
            object_type="tariff_plan",
            object_id=str(plan.id),
            details_json=safe_json_dumps(
                {
                    "code": plan.code,
                    "is_active": bool(plan.is_active),
                    "limits": _tariff_plan_limits_dict(plan),
                    "updated_subscriptions": len(updated_subscriptions),
                }
            ),
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/plans?tariff_updated=1", status_code=302)


@app.post("/app/superadmin/tariffs/{plan_id}/set-default", response_class=RedirectResponse)
def app_superadmin_set_default_tariff_plan(
    request: Request,
    plan_id: int,
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
    target = db.query(TariffPlan).filter(TariffPlan.id == int(plan_id)).first()
    if target is None:
        return RedirectResponse(url="/app/superadmin/plans?error=tariff_missing", status_code=302)
    current_default = db.query(TariffPlan).filter(TariffPlan.is_default.is_(True)).all()
    for plan in current_default:
        if plan.id == target.id:
            continue
        plan.is_default = False
        db.add(plan)
    target.is_default = True
    target.is_active = True
    db.add(target)
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="tariff_default_changed",
            object_type="tariff_plan",
            object_id=str(target.id),
            details_json=safe_json_dumps({"code": target.code}),
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/plans?tariff_default=1", status_code=302)


@app.post("/app/superadmin/tariffs/{plan_id}/delete", response_class=RedirectResponse)
def app_superadmin_delete_tariff_plan(
    request: Request,
    plan_id: int,
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
    plan = db.query(TariffPlan).filter(TariffPlan.id == int(plan_id)).first()
    if plan is None:
        return RedirectResponse(url="/app/superadmin/plans?error=tariff_missing", status_code=302)
    if bool(plan.is_default):
        return RedirectResponse(url="/app/superadmin/plans?error=default_tariff_protected", status_code=302)
    in_use_count = (
        db.query(Subscription)
        .filter(Subscription.plan_code == str(plan.code))
        .count()
    )
    if in_use_count > 0:
        return RedirectResponse(url="/app/superadmin/plans?error=tariff_in_use", status_code=302)
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="tariff_plan_deleted",
            object_type="tariff_plan",
            object_id=str(plan.id),
            details_json=safe_json_dumps({"code": plan.code}),
        )
    )
    db.delete(plan)
    db.commit()
    return RedirectResponse(url="/app/superadmin/plans?tariff_deleted=1", status_code=302)


@app.post("/app/superadmin/workspaces/{workspace_id}/assign-tariff", response_class=RedirectResponse)
def app_superadmin_assign_workspace_tariff(
    request: Request,
    workspace_id: int,
    plan_code: str = Form(""),
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
    _require_superadmin(current_user)
    ws = db.query(Workspace).filter(Workspace.id == int(workspace_id)).first()
    if ws is None:
        return RedirectResponse(url="/app/superadmin/plans?error=workspace_missing", status_code=302)
    normalized_code = _normalize_tariff_plan_code(plan_code)
    target_plan = (
        db.query(TariffPlan)
        .filter(TariffPlan.code == normalized_code, TariffPlan.is_active.is_(True))
        .first()
    )
    if target_plan is None:
        return RedirectResponse(url="/app/superadmin/plans?error=tariff_missing", status_code=302)
    sub = get_or_create_subscription(db, workspace_id=ws.id)
    sub.plan_code = target_plan.code
    sub.status = (status or "active").strip().lower()
    _sync_subscription_cached_limits_from_plan(db, subscription=sub)
    db.add(sub)
    db.add(
        AuditLog(
            workspace_id=ws.id,
            actor_user_id=current_user.id,
            action="workspace_tariff_assigned",
            object_type="subscription",
            object_id=str(sub.id),
            details_json=safe_json_dumps(
                {
                    "workspace_id": int(ws.id),
                    "plan_code": target_plan.code,
                    "status": sub.status,
                }
            ),
        )
    )
    db.commit()
    ensure_workspace_active_by_billing(db, workspace_id=ws.id)
    return RedirectResponse(url="/app/superadmin/plans?workspace_tariff_updated=1", status_code=302)


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
    page: int = 1,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return _render_superadmin_page(
        request=request,
        current_user=current_user,
        db=db,
        tab="audit",
        audit_page=page,
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
    if str(user.role or "").strip().lower() == "superadmin":
        return RedirectResponse(url="/app/superadmin/users?error=superadmin_protected", status_code=302)
    workspace_id = user.workspace_id
    username = user.username
    db.query(UserSession).filter(UserSession.user_id == user_id).delete(synchronize_session=False)
    db.query(ConversationPin).filter(ConversationPin.service_user_id == user_id).delete(synchronize_session=False)
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
            actor_user_id=(None if int(current_user.id) == int(user_id) else current_user.id),
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
        ws.suspended_by_admin = True
        ws.is_suspended = True
        db.add(ws)
        revoked_count = revoke_workspace_sessions(db, workspace_id=workspace_id)
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=current_user.id,
                action="workspace_suspended",
                object_type="workspace",
                object_id=str(workspace_id),
                details_json=f'{{"sessions_revoked":{revoked_count}}}',
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
        ws.suspended_by_admin = False
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


@app.post("/app/superadmin/support-contacts", response_class=RedirectResponse)
def app_superadmin_update_support_contacts(
    request: Request,
    support_tech_email: str = Form(""),
    support_billing_email: str = Form(""),
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
    tech_email = (support_tech_email or "").strip().lower()
    billing_email = (support_billing_email or "").strip().lower()
    if tech_email and not _EMAIL_RE.fullmatch(tech_email):
        raise HTTPException(status_code=400, detail="Некорректный email технического отдела")
    if billing_email and not _EMAIL_RE.fullmatch(billing_email):
        raise HTTPException(status_code=400, detail="Некорректный email финансового отдела")
    row = db.query(PlatformSettings).order_by(PlatformSettings.id.asc()).first()
    if row is None:
        row = PlatformSettings(
            technical_support_email=tech_email,
            billing_support_email=billing_email,
        )
    else:
        row.technical_support_email = tech_email
        row.billing_support_email = billing_email
    db.add(row)
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=current_user.id,
            action="support_contacts_updated",
            object_type="platform_settings",
            object_id=str(row.id or 0),
            details_json=f'{{"tech_email":"{tech_email}","billing_email":"{billing_email}"}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/system", status_code=302)


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
        db.query(ConversationPin).filter(ConversationPin.service_user_id.in_(user_ids)).delete(
            synchronize_session=False
        )
    db.query(ManagerInvite).filter(ManagerInvite.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(OutboxMessage).filter(OutboxMessage.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ManagerDispatch).filter(ManagerDispatch.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(WorkspaceBusinessSlot).filter(WorkspaceBusinessSlot.workspace_id == workspace_id).delete(
        synchronize_session=False
    )
    db.query(WorkspaceBusinessException).filter(
        WorkspaceBusinessException.workspace_id == workspace_id
    ).delete(synchronize_session=False)
    db.query(WorkspaceBusinessHours).filter(WorkspaceBusinessHours.workspace_id == workspace_id).delete(
        synchronize_session=False
    )
    db.query(WorkspaceRetentionPolicy).filter(
        WorkspaceRetentionPolicy.workspace_id == workspace_id
    ).delete(synchronize_session=False)
    db.query(ConversationFolderLink).filter(
        ConversationFolderLink.workspace_id == workspace_id
    ).delete(synchronize_session=False)
    db.query(ConversationPin).filter(ConversationPin.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ConversationMeta).filter(ConversationMeta.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ChatMessageMedia).filter(ChatMessageMedia.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(MessageLog).filter(MessageLog.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ChatMessage).filter(ChatMessage.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(Conversation).filter(Conversation.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(CustomerProfile).filter(CustomerProfile.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ChatFolder).filter(ChatFolder.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(IntroStep).filter(IntroStep.workspace_id == workspace_id).delete(synchronize_session=False)
    workspace_reply_ids = [
        row[0]
        for row in db.query(QuickReply.id)
        .filter(QuickReply.workspace_id == workspace_id)
        .all()
    ]
    if workspace_reply_ids:
        db.query(QuickReplyMedia).filter(
            QuickReplyMedia.quick_reply_id.in_(workspace_reply_ids)
        ).delete(synchronize_session=False)
        db.query(QuickReplyMediaAssetLink).filter(
            QuickReplyMediaAssetLink.quick_reply_id.in_(workspace_reply_ids)
        ).delete(synchronize_session=False)
    db.query(QuickReply).filter(QuickReply.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(QuickReplyMediaAssetLink).filter(
        QuickReplyMediaAssetLink.workspace_id == workspace_id
    ).delete(synchronize_session=False)
    db.query(MediaAsset).filter(MediaAsset.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(MessageTemplate).filter(MessageTemplate.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(BotSettings).filter(BotSettings.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(Subscription).filter(Subscription.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(TenantAlert).filter(TenantAlert.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(BillingEvent).filter(BillingEvent.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(ServiceUser).filter(ServiceUser.workspace_id == workspace_id).delete(synchronize_session=False)
    db.query(AuditLog).filter(AuditLog.workspace_id == workspace_id).delete(synchronize_session=False)
    db.delete(ws)
    actor_deleted_with_workspace = False
    if current_user is not None and current_user.workspace_id is not None:
        actor_deleted_with_workspace = int(current_user.workspace_id) == int(workspace_id)
    db.add(
        AuditLog(
            workspace_id=None,
            actor_user_id=(None if actor_deleted_with_workspace else current_user.id),
            action="workspace_deleted",
            object_type="workspace",
            object_id=str(workspace_id),
            details_json=f'{{"name":"{ws_name}"}}',
        )
    )
    db.commit()
    return RedirectResponse(url="/app/superadmin/workspaces", status_code=302)


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


@app.post("/app/superadmin/backups/delete")
def app_superadmin_delete_backup(
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
    if not target_name:
        return RedirectResponse(url="/app/superadmin/backups/view?error=backup_missing", status_code=302)
    try:
        deleted_path = delete_sqlite_backup(target_name)
    except (FileNotFoundError, ValueError):
        return RedirectResponse(url="/app/superadmin/backups/view?error=backup_missing", status_code=302)
    with SessionLocal() as db:
        db.add(
            AuditLog(
                workspace_id=None,
                actor_user_id=current_user.id,
                action="backup_deleted",
                object_type="backup",
                object_id=deleted_path.name,
                details_json="{}",
            )
        )
        db.commit()
    return RedirectResponse(url="/app/superadmin/backups/view?backup_deleted=1", status_code=302)


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
        if not _is_unlimited_plan(db, workspace_id=workspace_id):
            folders_count = db.query(ChatFolder).filter(ChatFolder.workspace_id == workspace_id).count()
            folders_limit = _folder_limit_for_workspace(db, workspace_id=workspace_id)
            if folders_count >= folders_limit:
                folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
                conv_qs = f"&conversation_id={conversation_id}" if conversation_id is not None else ""
                view_qs = f"&view={view}" if view else ""
                workspace_qs = _workspace_scope_query_suffix(
                    workspace_id=workspace_id,
                    is_scoped=is_superadmin_scoped,
                )
                return RedirectResponse(
                    url=(
                        f"/app/chats?q={q}{folder_qs}{conv_qs}{view_qs}{workspace_qs}"
                        "&folder_limit=1"
                    ),
                    status_code=302,
                )
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            _assign_new_folder_to_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
                new_folder_id=created.id,
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


@app.post("/app/chats/folders/delete", response_class=RedirectResponse)
def app_chat_delete_folders(
    request: Request,
    folder_ids: str = Form(""),
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    target_ids = _resolve_folder_ids_from_form(folder_ids_csv=folder_ids, folder_id=folder_id)
    removed_ids = delete_chat_folders(
        db,
        folder_ids=target_ids,
        workspace_id=workspace_id,
    )
    removed_set = {int(value) for value in removed_ids}
    next_folder_filter = (
        current_folder_id
        if current_folder_id is not None and int(current_folder_id) not in removed_set
        else None
    )
    folder_qs = f"&folder_id={int(next_folder_filter)}" if next_folder_filter is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    removed_suffix = "1" if removed_ids else "0"
    removed_count_suffix = f"&folders_removed_count={len(removed_ids)}" if removed_ids else ""
    return RedirectResponse(
        url=(
            f"/app/chats?q={q}&view={view}{folder_qs}{workspace_qs}"
            f"&folders_removed={removed_suffix}{removed_count_suffix}"
        ),
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def app_chat_mark_unread(
    conversation_id: int,
    current_conversation_id: int | None = Form(default=None),
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
    target_conversation = (
        current_conversation_id
        if current_conversation_id is not None
        else conversation_id
    )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=f"/app/chats?conversation_id={target_conversation}&q={q}&view={view}&mark_read=0&unread={suffix}{folder_qs}{workspace_qs}",
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def app_chat_move_folder(
    conversation_id: int,
    folder_id: int = Form(0),
    folder_ids: str = Form(""),
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
    next_folder_ids = _resolve_folder_ids_from_form(
        folder_ids_csv=folder_ids,
        folder_id=folder_id,
    )
    ok = replace_conversation_folder_links(
        db=db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=workspace_id,
    )
    suffix = "1" if ok else "0"
    multi_suffix = "1" if ok and len(next_folder_ids) > 1 else "0"
    folder_qs = f"&folder_id={current_folder_id}" if current_folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}&q={q}&view={view}"
            f"&foldered={suffix}&foldered_multi={multi_suffix}{folder_qs}{workspace_qs}"
        ),
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/pin", response_class=RedirectResponse)
def app_chat_pin(
    request: Request,
    conversation_id: int,
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
    pinned, reason = pin_conversation_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=int(current_user.id or 0),
        conversation_id=conversation_id,
    )
    if pinned:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="chat_pinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "app_chats"}),
            )
        )
        db.commit()
    suffix = "1" if pinned else "0"
    limit_suffix = "&pin_limit=1" if reason == "pinned_chats_limit_exceeded" else ""
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}&q={q}&view={view}"
            f"&pinned={suffix}{limit_suffix}{folder_qs}{workspace_qs}"
        ),
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/unpin", response_class=RedirectResponse)
def app_chat_unpin(
    request: Request,
    conversation_id: int,
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
    unpinned = unpin_conversation_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=int(current_user.id or 0),
        conversation_id=conversation_id,
    )
    if unpinned:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="chat_unpinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "app_chats"}),
            )
        )
        db.commit()
    suffix = "1" if unpinned else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}&q={q}&view={view}"
            f"&unpinned={suffix}{folder_qs}{workspace_qs}"
        ),
        status_code=302,
    )


@app.post("/app/chats/pins/reorder", response_class=JSONResponse)
async def app_chat_pins_reorder(
    request: Request,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    workspace_id, _scoped_workspace, _is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    payload = await request.json()
    ids_raw = payload.get("conversation_ids") if isinstance(payload, dict) else None
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ordered_ids: list[int] = []
    seen: set[int] = set()
    for item in ids_raw:
        try:
            conv_id = int(item)
        except (TypeError, ValueError):
            continue
        if conv_id <= 0 or conv_id in seen:
            continue
        seen.add(conv_id)
        ordered_ids.append(conv_id)
    if not ordered_ids:
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ok = reorder_pins_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=int(current_user.id or 0),
        ordered_conversation_ids=ordered_ids,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="invalid_pin_order")
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=int(current_user.id),
            action="chat_pins_reordered",
            object_type="conversation_pin",
            object_id=str(current_user.id),
            details_json=safe_json_dumps({"conversation_ids": ordered_ids, "scope": "app_chats"}),
        )
    )
    db.commit()
    return JSONResponse({"ok": True, "ordered_count": len(ordered_ids)}, status_code=200)


@app.post("/app/chats/{conversation_id}/send", response_class=RedirectResponse)
async def app_chats_send_message(
    request: Request,
    conversation_id: int,
    text: str = Form(""),
    edit_message_id: int | None = Form(default=None),
    photos: list[UploadFile] = File(default=[]),
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
    uploads_to_validate = [item for item in photos if item and item.filename]
    if not uploads_to_validate:
        form_data = await request.form()
        legacy_photo = form_data.get("photo")
        if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
            uploads_to_validate = [legacy_photo]
    validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
    image_paths = _store_uploaded_images(validated_uploads)

    if edit_message_id is not None:
        updated = None
        if text_value and not image_paths:
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

    if text_value.startswith("/") and not image_paths:
        quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
        sent_ok, quick_error = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
            owner_user_id=quick_reply_owner_id,
        )
        suffix = "1" if sent_ok else "0"
        redirect_url = f"/app/chats?conversation_id={conversation_id}&quick={suffix}"
        if not sent_ok and quick_error:
            redirect_url += f"&quick_error={_encode_chat_error(quick_error)}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=f"{redirect_url}{workspace_qs}", status_code=302)

    schedule_at_value = await _resolve_schedule_at_value(request, schedule_at)
    sent_ok = False
    try:
        sent_ok, send_error_reason = await send_admin_chat_message(
            db=db,
            conversation_id=conversation_id,
            text=text_value,
            image_paths=image_paths,
            workspace_id=workspace_id,
            schedule_at_iso=schedule_at_value,
        )
    finally:
        if not sent_ok and image_paths:
            _cleanup_uploaded_images(image_paths)
    scheduled_at_clean = schedule_at_value
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    redirect_url = f"/app/chats?conversation_id={conversation_id}&{flag_name}={suffix}"
    if not sent_ok and send_error_reason:
        redirect_url += f"&send_error={_encode_chat_error(send_error_reason)}"
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
    quick_reply_owner_id = _quick_reply_owner_user_id(current_user)
    sent_ok, quick_error = await send_admin_quick_reply(
        db=db,
        conversation_id=conversation_id,
        command_text=command,
        workspace_id=workspace_id,
        owner_user_id=quick_reply_owner_id,
    )
    suffix = "1" if sent_ok else "0"
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}&quick={suffix}"
            + (f"&quick_error={_encode_chat_error(quick_error)}" if (not sent_ok and quick_error) else "")
            + f"{workspace_qs}"
        ),
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
    _ensure_user_can_delete_chats(current_user)
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
    _ensure_user_can_delete_chats(current_user)
    deleted = delete_conversation(db, conversation_id=conversation_id, workspace_id=workspace_id)
    if deleted:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_deleted",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"source": "app_chats"}),
            )
        )
        db.commit()
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


@app.post("/app/chats/{conversation_id}/block-user", response_class=RedirectResponse)
def app_chats_block_conversation_customer(
    request: Request,
    conversation_id: int,
    block_reason: str = Form(""),
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
    blocked = block_conversation_customer(
        db,
        conversation_id=conversation_id,
        actor_user_id=int(current_user.id),
        reason=block_reason,
        workspace_id=workspace_id,
    )
    if blocked:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_blocked",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"reason": (block_reason or "").strip()}),
            )
        )
        db.commit()
    suffix = "1" if blocked else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}"
            f"&q={quote_plus(q.strip())}&view={view}&blocked={suffix}{folder_qs}{workspace_qs}"
        ),
        status_code=302,
    )


@app.post("/app/chats/{conversation_id}/unblock-user", response_class=RedirectResponse)
def app_chats_unblock_conversation_customer(
    request: Request,
    conversation_id: int,
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
    unblocked = unblock_conversation_customer(
        db,
        conversation_id=conversation_id,
        actor_user_id=int(current_user.id),
        workspace_id=workspace_id,
    )
    if unblocked:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=int(current_user.id),
                action="customer_unblocked",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"source": "app_chats"}),
            )
        )
        db.commit()
    suffix = "1" if unblocked else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )
    return RedirectResponse(
        url=(
            f"/app/chats?conversation_id={conversation_id}"
            f"&q={quote_plus(q.strip())}&view={view}&unblocked={suffix}{folder_qs}{workspace_qs}"
        ),
        status_code=302,
    )


@app.post("/app/chats/bulk-action", response_class=RedirectResponse)
async def app_chat_bulk_action(
    request: Request,
    workspace_id: int | None = None,
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _enforce_same_origin(request)
    _check_rate_limit_or_raise(
        request,
        scope="app_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    workspace_id, _scoped_workspace, is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    payload = await _parse_bulk_action_request_payload(request)
    action = str(payload.get("action") or "").strip().lower()
    conversation_ids = _parse_conversation_ids_from_payload(payload)
    q_value = str(payload.get("q") or "").strip()
    view_value = str(payload.get("view") or "").strip()
    current_folder_id = _parse_optional_int(payload.get("current_folder_id"))
    workspace_qs = _workspace_scope_query_suffix(
        workspace_id=workspace_id,
        is_scoped=is_superadmin_scoped,
    )

    moved_total = 0
    deleted_total = 0
    moved_multi = False

    if action == "move":
        folder_ids = _parse_folder_ids_from_payload(payload)
        moved_multi = len(folder_ids) > 1
        for conversation_id in conversation_ids:
            moved = replace_conversation_folder_links(
                db=db,
                conversation_id=conversation_id,
                folder_ids=folder_ids,
                workspace_id=workspace_id,
            )
            if moved:
                moved_total += 1
    elif action == "delete":
        _ensure_user_can_delete_chats(current_user)
        for conversation_id in conversation_ids:
            deleted = delete_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
            )
            if not deleted:
                continue
            deleted_total += 1
            db.add(
                AuditLog(
                    workspace_id=workspace_id,
                    actor_user_id=int(current_user.id),
                    action="customer_deleted",
                    object_type="conversation",
                    object_id=str(conversation_id),
                    details_json=safe_json_dumps({"source": "app_chats_bulk"}),
                )
            )
            db.commit()
    else:
        raise HTTPException(status_code=400, detail="invalid_bulk_action")

    redirect_url = _bulk_operation_redirect_url(
        base_path="/app/chats",
        q=q_value,
        view=view_value,
        folder_id=current_folder_id,
        workspace_qs=workspace_qs,
        deleted_total=deleted_total,
        moved_total=moved_total,
        moved_multi=moved_multi,
    )
    return RedirectResponse(url=redirect_url, status_code=302)


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
        # UI request: suppress generic "message sent" toast in chats.
        pass
    send_error_reason = str(request.query_params.get("send_error") or "").strip()
    quick_error_reason = str(request.query_params.get("quick_error") or "").strip()
    if sent_flag == "0":
        # UI request: suppress generic send-error toast in chats.
        pass
    if scheduled_flag == "1":
        op_message = "Сообщение запланировано"
    if scheduled_flag == "0":
        op_error = (
            f"Ошибка отправки сообщения: {send_error_reason}"
            if send_error_reason
            else "Не удалось запланировать сообщение"
        )
    if quick_flag == "1":
        op_message = "Быстрый ответ отправлен"
    if quick_flag == "0":
        op_error = (
            f"Ошибка отправки сообщения: {quick_error_reason}"
            if quick_error_reason
            else "Не удалось отправить быстрый ответ"
        )
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
    if request.query_params.get("foldered_multi") == "1":
        op_message = "Чат добавлен в несколько папок"
    bulk_moved_value = _parse_optional_int(request.query_params.get("bulk_moved"))
    if bulk_moved_value and bulk_moved_value > 0:
        op_message = f"Перемещено чатов: {bulk_moved_value}"
    bulk_removed_value = _parse_optional_int(request.query_params.get("bulk_removed"))
    if bulk_removed_value and bulk_removed_value > 0:
        op_message = f"Удалено чатов: {bulk_removed_value}"
    if request.query_params.get("pinned") == "1":
        op_message = "Чат закреплен"
    if request.query_params.get("pinned") == "0":
        op_error = "Не удалось закрепить чат"
    if request.query_params.get("unpinned") == "1":
        op_message = "Чат откреплен"
    if request.query_params.get("unpinned") == "0":
        op_error = "Не удалось открепить чат"
    if request.query_params.get("pin_limit") == "1":
        op_error = "Достигнут лимит закрепленных чатов по вашему тарифу"
    if request.query_params.get("blocked") == "1":
        op_message = "Пользователь заблокирован"
    if request.query_params.get("blocked") == "0":
        op_error = "Не удалось заблокировать пользователя"
    if request.query_params.get("unblocked") == "1":
        op_message = "Пользователь разблокирован"
    if request.query_params.get("unblocked") == "0":
        op_error = "Не удалось разблокировать пользователя"
    error_reason = str(request.query_params.get("error_reason") or "").strip()
    if error_reason and op_error:
        normalized_error = op_error.rstrip(".")
        op_error = f"{normalized_error}: {error_reason}"
    return op_message, op_error


def _thread_summary_dict(item: object) -> dict[str, object]:
    conversation_id = int(getattr(item, "conversation_id", 0) or 0)
    thread_time_label = str(getattr(item, "last_message_time_label", "") or "").strip()
    if not thread_time_label:
        thread_time_label = _to_moscow_chat_label(getattr(item, "last_message_created_at", None))
    return {
        "conversation_id": conversation_id,
        "chat_id": str(getattr(item, "chat_id", "") or ""),
        "customer_account_id": str(getattr(item, "customer_account_id", "") or ""),
        "ticket_no": getattr(item, "ticket_no", None),
        "customer_label": str(getattr(item, "customer_label", "") or ""),
        "is_unread": bool(getattr(item, "is_unread", False)),
        "is_pinned": bool(getattr(item, "is_pinned", False)),
        "pin_order": (
            int(getattr(item, "pin_order"))
            if getattr(item, "pin_order", None) is not None
            else None
        ),
        "is_blocked": bool(getattr(item, "is_blocked", False)),
        "is_new": str(getattr(item, "status", "") or "").strip().lower() == "new",
        "has_delivery_errors": bool(getattr(item, "has_delivery_errors", False)),
        "last_message_preview": str(getattr(item, "last_message_preview", "") or ""),
        "last_message_created_at": (
            getattr(item, "last_message_created_at").isoformat()
            if isinstance(getattr(item, "last_message_created_at", None), datetime)
            else ""
        ),
        "last_message_time_label": thread_time_label,
        "last_message_from_customer_max": bool(
            getattr(item, "last_message_from_customer_max", False)
        ),
        "folder_id": getattr(item, "folder_id", None),
        "folder_name": str(getattr(item, "folder_name", "") or ""),
        "folder_ids": [int(v) for v in (getattr(item, "folder_ids", []) or [])],
        "folder_names": [str(v) for v in (getattr(item, "folder_names", []) or [])],
    }


def _message_summary_dict(item: ChatMessage) -> dict[str, object]:
    text_value = str(item.text or "")
    delivery_state_value = str(item.delivery_state or "sent")
    delivery_next_retry_at_raw = getattr(item, "delivery_next_retry_at", None)
    image_urls = get_message_media_urls(item)
    is_scheduled_message = bool(getattr(item, "is_scheduled_message", False))
    is_scheduled_pending = bool(
        delivery_state_value == "queued"
        and (
            is_scheduled_message
            or (
                isinstance(delivery_next_retry_at_raw, datetime)
                and delivery_next_retry_at_raw > datetime.utcnow()
            )
        )
    )
    return {
        "id": int(item.id),
        "direction": str(item.direction or ""),
        "source": str(item.source or ""),
        "text": text_value,
        "image_url": str(item.image_url or ""),
        "image_urls": image_urls,
        "delivery_state": delivery_state_value,
        "delivery_error": str(item.delivery_error or ""),
        "max_message_mid": str(item.max_message_mid or ""),
        "is_read_by_customer": bool(getattr(item, "is_read_by_customer", False)),
        "read_at": (
            item.read_at.isoformat()
            if isinstance(getattr(item, "read_at", None), datetime)
            else ""
        ),
        "delivery_next_retry_at": (
            delivery_next_retry_at_raw.isoformat()
            if isinstance(delivery_next_retry_at_raw, datetime)
            else ""
        ),
        "is_scheduled_message": is_scheduled_message,
        "is_scheduled_pending": is_scheduled_pending,
        "created_at": (
            item.created_at.isoformat()
            if isinstance(getattr(item, "created_at", None), datetime)
            else ""
        ),
        "created_at_label": _to_moscow_chat_label(getattr(item, "created_at", None)),
    }


def _hydrate_message_media_urls(db: Session, messages: list[ChatMessage]) -> None:
    if not messages:
        return
    message_ids = [int(getattr(msg, "id", 0) or 0) for msg in messages if int(getattr(msg, "id", 0) or 0) > 0]
    if not message_ids:
        return
    by_message = list_chat_message_media_urls_map(db, chat_message_ids=message_ids)
    for msg in messages:
        msg_id = int(getattr(msg, "id", 0) or 0)
        setattr(msg, "image_urls", get_message_media_urls(msg, linked_urls_map=by_message))


def _select_active_thread(
    *,
    threads: list[object],
    conversation_id: int | None,
) -> object | None:
    has_explicit_conversation = conversation_id is not None
    active_thread = None
    if conversation_id is not None:
        for item in threads:
            if int(getattr(item, "conversation_id", 0) or 0) == int(conversation_id):
                active_thread = item
                break
    if active_thread is None and threads and not has_explicit_conversation:
        active_thread = threads[0]
    return active_thread


def _threads_signature(threads: list[object]) -> str:
    signature_source = "|".join(
        (
            f"{int(getattr(item, 'conversation_id', 0) or 0)}:"
            f"{int(getattr(item, 'last_activity_id', 0) or 0)}:"
            f"{1 if bool(getattr(item, 'is_unread', False)) else 0}:"
            f"{1 if bool(getattr(item, 'is_pinned', False)) else 0}:"
            f"{int(getattr(item, 'pin_order') or 0)}"
        )
        for item in threads
    )
    return hashlib.sha256(signature_source.encode("utf-8")).hexdigest()[:16]


def _folder_unread_counts(threads: list[object]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for item in threads:
        if not bool(getattr(item, "is_unread", False)):
            continue
        raw_folder_ids = getattr(item, "folder_ids", []) or []
        folder_ids: set[int] = set()
        for value in raw_folder_ids:
            try:
                folder_id = int(value)
            except (TypeError, ValueError):
                continue
            if folder_id > 0:
                folder_ids.add(folder_id)
        for folder_id in folder_ids:
            counts[folder_id] = int(counts.get(folder_id, 0)) + 1
    return counts


def _unread_chat_totals(threads: list[object]) -> tuple[int, int]:
    """
    Returns:
      - total unread chats
      - unread chats without any folder
    """
    unread_total = 0
    unread_without_folder = 0
    for item in threads:
        if not bool(getattr(item, "is_unread", False)):
            continue
        unread_total += 1
        raw_folder_ids = getattr(item, "folder_ids", []) or []
        has_any_folder = False
        for value in raw_folder_ids:
            try:
                folder_id = int(value)
            except (TypeError, ValueError):
                continue
            if folder_id > 0:
                has_any_folder = True
                break
        if not has_any_folder:
            unread_without_folder += 1
    return unread_total, unread_without_folder


def _messages_signature(messages: list[object]) -> str:
    signature_source = "|".join(
        (
            f"{int(getattr(item, 'id', 0) or 0)}:"
            f"{str(getattr(item, 'delivery_state', '') or '')}:"
            f"{1 if bool(getattr(item, 'is_read_by_customer', False)) else 0}:"
            f"{(getattr(item, 'read_at').isoformat() if isinstance(getattr(item, 'read_at', None), datetime) else '')}:"
            f"{(getattr(item, 'delivery_next_retry_at').isoformat() if isinstance(getattr(item, 'delivery_next_retry_at', None), datetime) else str(getattr(item, 'delivery_next_retry_at', '') or ''))}:"
            f"{','.join(str(url).strip() for url in (getattr(item, 'image_urls', []) or []) if str(url).strip())}:"
            f"{str(getattr(item, 'image_url', '') or '')}:"
            f"{str(getattr(item, 'text', '') or '')}"
        )
        for item in messages
    )
    return hashlib.sha256(signature_source.encode("utf-8")).hexdigest()[:16]


def _messages_signature_window(messages: list[object], *, limit: int) -> list[object]:
    limit_value = max(0, int(limit or 0))
    if limit_value <= 0:
        return list(messages or [])
    rows = list(messages or [])
    if len(rows) <= limit_value:
        return rows
    return rows[-limit_value:]


def _build_chat_updates_payload(
    *,
    db: Session,
    workspace_id: int,
    service_user_id: int = 0,
    query: str,
    folder_id: int | None,
    conversation_id: int | None,
    mark_read: bool,
    last_message_id: int | None = None,
    threads_signature: str = "",
    messages_signature: str = "",
) -> dict[str, object]:
    all_threads = load_chat_threads(
        db,
        query=query,
        workspace_id=workspace_id,
        service_user_id=int(service_user_id or 0),
    )
    threads = _filter_threads_by_folder(all_threads, folder_id)

    active_thread = _select_active_thread(
        threads=threads,
        conversation_id=conversation_id,
    )

    messages: list[ChatMessage] = []
    if active_thread is not None:
        if mark_read:
            mark_thread_read(db, int(active_thread.conversation_id), workspace_id=workspace_id)
            # Keep current payload consistent in the same request cycle:
            # once thread is marked as read, avoid stale unread badges until next poll.
            try:
                setattr(active_thread, "is_unread", False)
            except Exception:
                pass
            for item in threads:
                if int(getattr(item, "conversation_id", 0) or 0) == int(active_thread.conversation_id):
                    try:
                        setattr(item, "is_unread", False)
                    except Exception:
                        pass
                    break
        messages = load_chat_messages(
            db,
            int(active_thread.conversation_id),
            workspace_id=workspace_id,
            limit=_CHAT_UPDATES_MESSAGES_LIMIT,
        )
        _hydrate_message_media_urls(db, messages)

    current_threads_signature = _threads_signature(threads)
    current_messages_signature = _messages_signature(
        _messages_signature_window(messages, limit=_CHAT_UPDATES_MESSAGES_LIMIT)
    )
    folder_unread_counts = _folder_unread_counts(all_threads)
    unread_total_count, unread_without_folder_count = _unread_chat_totals(all_threads)

    active_last_message_id = int(messages[-1].id) if messages else 0
    previous_last_message_id = int(last_message_id or 0)
    previous_threads_signature = str(threads_signature or "").strip()
    previous_messages_signature = str(messages_signature or "").strip()
    messages_changed = False
    threads_changed = False
    if previous_last_message_id > 0 or previous_messages_signature:
        # If requested conversation no longer exists (e.g. was deleted/recreated),
        # avoid sending perpetual "changed" flags that trigger soft-refresh loops.
        if active_thread is None and conversation_id is not None:
            messages_changed = False
        else:
            if previous_last_message_id > 0:
                messages_changed = active_last_message_id != previous_last_message_id
            if previous_messages_signature:
                messages_changed = messages_changed or (
                    current_messages_signature != previous_messages_signature
                )
    if previous_threads_signature:
        threads_changed = current_threads_signature != previous_threads_signature

    return {
        "active_conversation_id": int(active_thread.conversation_id) if active_thread is not None else 0,
        "active_last_message_id": active_last_message_id,
        "active_messages_signature": current_messages_signature,
        "threads_signature": current_threads_signature,
        "threads_count": len(threads),
        "messages_changed": messages_changed,
        "threads_changed": threads_changed,
        "threads": [_thread_summary_dict(item) for item in threads],
        "messages": [_message_summary_dict(item) for item in messages],
        "folder_unread_counts": {int(key): int(value) for key, value in folder_unread_counts.items()},
        "unread_total_count": int(unread_total_count),
        "unread_without_folder_count": int(unread_without_folder_count),
        "now_utc": datetime.utcnow().isoformat(),
    }


def _ws_incoming_hint_event(
    *,
    workspace_id: int,
    conversation_id: int | None,
    source: str = "incoming_message",
    seq: int = 0,
) -> dict[str, object]:
    return {
        "type": "incoming_hint",
        "workspace_id": int(workspace_id or DEFAULT_WORKSPACE_ID),
        "conversation_id": int(conversation_id or 0),
        "seq": int(seq or 0),
        "source": str(source or "incoming_message"),
        "now_utc": datetime.utcnow().isoformat(),
    }


def _ws_resync_requested_event(
    *,
    workspace_id: int,
    source: str = "seq_gap",
) -> dict[str, object]:
    return {
        "type": "resync_requested",
        "workspace_id": int(workspace_id or DEFAULT_WORKSPACE_ID),
        "source": str(source or "seq_gap"),
        "now_utc": datetime.utcnow().isoformat(),
    }


def _extract_conversation_id_from_result(result: object) -> int | None:
    if not isinstance(result, dict):
        return None
    raw = result.get("conversation_id")
    try:
        value = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        value = 0
    return value if value > 0 else None


def _resolve_ws_admin_username(websocket: WebSocket, db: Session) -> str | None:
    # Legacy admin session from /admin/login.
    session_data = websocket.scope.get("session")
    if isinstance(session_data, dict) and bool(session_data.get("is_admin")):
        return str(session_data.get("admin_username") or settings.admin_username)

    # Backward-compatible superadmin access via tenant session cookie.
    token = str(websocket.cookies.get("tenant_session", "")).strip()
    if not token:
        return None
    token_hash = auth_sha256(token)
    now = datetime.now(UTC).replace(tzinfo=None)
    session_row = (
        db.query(UserSession)
        .filter(
            UserSession.session_token_hash == token_hash,
            UserSession.is_revoked.is_(False),
            UserSession.expires_at > now,
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
    if user.role != "superadmin":
        return None
    return str(user.username or "superadmin")


async def _broadcast_workspace_chat_update(
    *,
    workspace_id: int,
    conversation_id: int | None = None,
    source: str = "incoming_message",
) -> None:
    try:
        workspace_value = int(workspace_id or DEFAULT_WORKSPACE_ID)
        conversation_value = int(conversation_id or 0)
        seq_value = int(await chat_realtime_hub.next_workspace_seq(workspace_id=workspace_value))
        if not chat_realtime_hub.should_emit_incoming_hint(
            workspace_id=workspace_value,
            conversation_id=conversation_value,
            min_interval_ms=200,
        ):
            _ws_hint_log(
                "skip_dedupe",
                workspace_id=workspace_value,
                conversation_id=conversation_value,
                source=source,
                seq=seq_value,
            )
            return
        if not chat_realtime_hub.should_emit_workspace_hint_rate_limited(
            workspace_id=workspace_value,
            window_seconds=1.0,
            max_events_per_window=12,
        ):
            # If burst limiter is hit, ask clients to perform a safe polling resync.
            delivered = await chat_realtime_hub.broadcast_workspace(
                workspace_id=workspace_value,
                event=_ws_resync_requested_event(
                    workspace_id=workspace_value,
                    source="rate_limited_hint",
                ),
            )
            _ws_hint_log(
                "resync_requested_rate_limited",
                workspace_id=workspace_value,
                conversation_id=conversation_value,
                source=source,
                seq=seq_value,
                delivered=delivered,
            )
            return
        delivered = await chat_realtime_hub.broadcast_workspace(
            workspace_id=workspace_value,
            event=_ws_incoming_hint_event(
                workspace_id=workspace_value,
                conversation_id=conversation_value,
                source=source,
                seq=seq_value,
            ),
        )
        _ws_hint_log(
            "incoming_hint_emitted",
            workspace_id=workspace_value,
            conversation_id=conversation_value,
            source=source,
            seq=seq_value,
            delivered=delivered,
        )
    except Exception:
        # Realtime must never break normal webhook/sync flow.
        logging.getLogger(__name__).exception("[WS_HINT] broadcast_error")


def _resolve_conversation_hint_id(
    db: Session,
    *,
    workspace_id: int,
    chat_id: str,
    sender_id: str,
) -> int:
    """
    Best-effort conversation resolver for incoming realtime hints.
    Safe fallback: 0 (global hint) when conversation cannot be matched.
    """
    chat_value = str(chat_id or "").strip()
    sender_value = str(sender_id or "").strip()
    if not chat_value and not sender_value:
        return 0
    query = db.query(Conversation.id).filter(Conversation.workspace_id == int(workspace_id or DEFAULT_WORKSPACE_ID))
    if chat_value:
        query = query.filter(Conversation.chat_id == chat_value)
    if sender_value:
        query = query.filter(Conversation.customer_account_id == sender_value)
    row = query.order_by(Conversation.id.desc()).first()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _resolve_service_user_from_ws(websocket: WebSocket, db: Session) -> ServiceUser | None:
    """
    WS-friendly equivalent of get_current_service_user(request, db).
    """
    try:
        token = str(websocket.cookies.get("tenant_session", "")).strip()
        if not token:
            return None
        token_hash = auth_sha256(token)
        session_row = (
            db.query(UserSession)
            .filter(
                UserSession.session_token_hash == token_hash,
                UserSession.is_revoked.is_(False),
                UserSession.expires_at > datetime.now(UTC).replace(tzinfo=None),
            )
            .first()
        )
        if session_row is None:
            return None
        user = db.query(ServiceUser).filter(ServiceUser.id == session_row.user_id).first()
        if user is None or not user.is_active or bool(user.is_blocked):
            return None
        if user.workspace_id:
            workspace = db.query(Workspace).filter(Workspace.id == user.workspace_id).first()
            if workspace is None or not bool(workspace.is_active) or bool(workspace.is_suspended):
                return None
        session_row.last_seen_at = datetime.now(UTC).replace(tzinfo=None)
        db.add(session_row)
        db.commit()
        return user
    except Exception:
        return None


@app.get("/admin/chats/updates", response_class=JSONResponse)
def admin_chats_updates(
    conversation_id: int | None = None,
    q: str = "",
    folder_id: int | None = None,
    auto_mark_read: int = 0,
    last_message_id: int = 0,
    threads_sig: str = "",
    messages_sig: str = "",
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> JSONResponse:
    payload = _build_chat_updates_payload(
        db=db,
        workspace_id=DEFAULT_WORKSPACE_ID,
        service_user_id=0,
        query=q,
        folder_id=folder_id,
        conversation_id=conversation_id,
        mark_read=(int(auto_mark_read or 0) == 1),
        last_message_id=last_message_id,
        threads_signature=threads_sig,
        messages_signature=messages_sig,
    )
    return JSONResponse({"ok": True, **payload}, status_code=200)


@app.get("/app/chats/updates", response_class=JSONResponse)
def app_chats_updates(
    request: Request,
    conversation_id: int | None = None,
    q: str = "",
    folder_id: int | None = None,
    auto_mark_read: int = 0,
    workspace_id: int | None = None,
    last_message_id: int = 0,
    threads_sig: str = "",
    messages_sig: str = "",
    current_user: ServiceUser = Depends(require_service_user),
    db: Session = Depends(get_db),
) -> JSONResponse:
    if current_user.role == "superadmin":
        raise HTTPException(status_code=403, detail="Для superadmin доступна только панель /app/superadmin")
    # Polling endpoint must have its own bucket, otherwise it can throttle page opens.
    _check_rate_limit_or_raise(
        request,
        scope="app_view_updates",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 120),
    )
    workspace_id, _scoped_workspace, _is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    payload = _build_chat_updates_payload(
        db=db,
        workspace_id=workspace_id,
        service_user_id=int(current_user.id or 0),
        query=q,
        folder_id=folder_id,
        conversation_id=conversation_id,
        mark_read=(int(auto_mark_read or 0) == 1),
        last_message_id=last_message_id,
        threads_signature=threads_sig,
        messages_signature=messages_sig,
    )
    return JSONResponse({"ok": True, **payload}, status_code=200)


@app.get("/mini/manager/chats/updates", response_class=JSONResponse)
def manager_mini_updates(
    request: Request,
    token: str,
    conversation_id: int | None = None,
    q: str = "",
    folder_id: int | None = None,
    auto_mark_read: int = 0,
    last_message_id: int = 0,
    threads_sig: str = "",
    messages_sig: str = "",
    db: Session = Depends(get_db),
) -> JSONResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 6),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    manager_user_id = int(claims.get("service_user_id") or 0)
    payload = _build_chat_updates_payload(
        db=db,
        workspace_id=workspace_id,
        service_user_id=manager_user_id,
        query=q,
        folder_id=folder_id,
        conversation_id=conversation_id,
        mark_read=(int(auto_mark_read or 0) == 1),
        last_message_id=last_message_id,
        threads_signature=threads_sig,
        messages_signature=messages_sig,
    )
    return JSONResponse({"ok": True, **payload}, status_code=200)


@app.websocket("/admin/chats/ws")
async def admin_chats_ws(
    websocket: WebSocket,
    db: Session = Depends(get_db),
) -> None:
    current_admin = _resolve_ws_admin_username(websocket, db)
    if not current_admin:
        await websocket.close(code=1008)
        return
    workspace_id = DEFAULT_WORKSPACE_ID
    await chat_realtime_hub.connect(workspace_id=workspace_id, websocket=websocket)
    try:
        while True:
            try:
                payload = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except RuntimeError:
                break
            if str(payload or "").strip().lower() == "ping":
                await websocket.send_json({"type": "pong", "workspace_id": workspace_id})
    finally:
        await chat_realtime_hub.disconnect(websocket)


@app.websocket("/app/chats/ws")
async def app_chats_ws(
    websocket: WebSocket,
    workspace_id: int | None = None,
    db: Session = Depends(get_db),
) -> None:
    current_user = _resolve_service_user_from_ws(websocket, db)
    if current_user is None or current_user.role == "superadmin":
        await websocket.close(code=1008)
        return
    resolved_workspace_id, _scoped_workspace, _is_superadmin_scoped = _resolve_app_workspace_scope(
        db=db,
        current_user=current_user,
        workspace_id=workspace_id,
    )
    await chat_realtime_hub.connect(workspace_id=resolved_workspace_id, websocket=websocket)
    try:
        while True:
            try:
                payload = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except RuntimeError:
                break
            if str(payload or "").strip().lower() == "ping":
                await websocket.send_json({"type": "pong", "workspace_id": resolved_workspace_id})
    finally:
        await chat_realtime_hub.disconnect(websocket)


@app.websocket("/mini/manager/chats/ws")
async def manager_mini_ws(
    websocket: WebSocket,
    token: str,
    db: Session = Depends(get_db),
) -> None:
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    await chat_realtime_hub.connect(workspace_id=workspace_id, websocket=websocket)
    try:
        while True:
            try:
                payload = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except RuntimeError:
                break
            if str(payload or "").strip().lower() == "ping":
                await websocket.send_json({"type": "pong", "workspace_id": workspace_id})
    finally:
        await chat_realtime_hub.disconnect(websocket)


async def _render_chat_workspace(
    *,
    request: Request,
    db: Session,
    conversation_id: int | None,
    q: str,
    view: str,
    folder_id: int | None,
    ui: dict[str, str | bool | ServiceUser | dict],
    include_removed: bool,
    workspace_id: int,
) -> HTMLResponse:
    # Do not process outbox inline on page render: under load this can block
    # request thread and escalate into 504 on /app/chats page loads.
    # Background outbox worker remains responsible for queue draining.
    op_message, op_error = _chat_op_messages(request)

    service_user_id = _chat_scope_service_user_id(
        current_user=ui.get("current_user") if isinstance(ui.get("current_user"), ServiceUser) else None,
        manager_claims=ui.get("manager_claims") if isinstance(ui.get("manager_claims"), dict) else None,
    )
    all_threads = load_chat_threads(
        db,
        query=q,
        workspace_id=workspace_id,
        service_user_id=service_user_id,
    )
    threads = all_threads
    threads = _filter_threads_by_folder(threads, folder_id)

    active_thread = _select_active_thread(
        threads=threads,
        conversation_id=conversation_id,
    )

    messages = []
    mark_read_requested = request.query_params.get("mark_read") == "1"
    opened_unread_same = request.query_params.get("opened_same_unread") == "1"
    if active_thread:
        if mark_read_requested and not opened_unread_same:
            thread_chat_id = str(getattr(active_thread, "chat_id", "") or "").strip()
            thread_sender_id = str(getattr(active_thread, "customer_account_id", "") or "").strip()
            mark_thread_read(db, active_thread.conversation_id, workspace_id=workspace_id)
            with suppress(Exception):
                await _try_mark_chat_seen_from_manager_read(
                    db=db,
                    workspace_id=workspace_id,
                    conversation_id=int(active_thread.conversation_id),
                    chat_id=thread_chat_id,
                    sender_id=thread_sender_id,
                )
            try:
                setattr(active_thread, "is_unread", False)
            except Exception:
                pass
            for item in all_threads:
                if int(getattr(item, "conversation_id", 0) or 0) == int(active_thread.conversation_id):
                    try:
                        setattr(item, "is_unread", False)
                    except Exception:
                        pass
                    break
        messages = load_chat_messages(db, active_thread.conversation_id, workspace_id=workspace_id)
        _hydrate_message_media_urls(db, messages)
    folder_unread_counts = _folder_unread_counts(all_threads)
    unread_total_count, unread_without_folder_count = _unread_chat_totals(all_threads)
    message_summaries = [_message_summary_dict(item) for item in messages]
    mobile_chat_view = view.strip().lower() == "chat"
    threads_signature = _threads_signature(threads)
    chat_state = {
        "active_conversation_id": (active_thread.conversation_id if active_thread else 0),
        "active_last_message_id": (message_summaries[-1]["id"] if message_summaries else 0),
        # Keep signature basis identical to /updates payload to avoid
        # perpetual messages_changed loops on long histories.
        "active_messages_signature": _messages_signature(
            _messages_signature_window(messages, limit=_CHAT_UPDATES_MESSAGES_LIMIT)
        ),
        "threads_signature": threads_signature,
        "threads_count": len(threads),
        "folder_unread_counts": {int(key): int(value) for key, value in folder_unread_counts.items()},
        "unread_total_count": int(unread_total_count),
        "unread_without_folder_count": int(unread_without_folder_count),
    }

    quick_reply_owner_id = _chat_scope_quick_reply_owner_id(
        current_user=ui.get("current_user") if isinstance(ui.get("current_user"), ServiceUser) else None,
        manager_claims=ui.get("manager_claims") if isinstance(ui.get("manager_claims"), dict) else None,
    )
    context: dict = {
        "request": request,
        "threads": threads,
        "active_thread": active_thread,
        "messages": message_summaries,
        "now_utc": datetime.utcnow(),
        "query": q,
        "folder_filter": folder_id,
        "message": op_message,
        "error": op_error,
        "mobile_chat_view": mobile_chat_view,
        "admin_quick_options": _build_quick_options_for_compose(
            db,
            workspace_id=workspace_id,
            owner_user_id=quick_reply_owner_id,
        ),
        "chat_folders": [
            {
                "id": folder.id,
                "name": folder.name,
                "unread_count": int(folder_unread_counts.get(int(folder.id), 0)),
            }
            for folder in list_chat_folders(db, workspace_id=workspace_id)
        ],
        "unread_total_count": int(unread_total_count),
        "unread_without_folder_count": int(unread_without_folder_count),
        "pinned_limit": _pinned_chats_limit_for_workspace(db, workspace_id=workspace_id),
        "pinned_used": len([item for item in threads if bool(getattr(item, "is_pinned", False))]),
        "ui": ui,
        "chat_state": chat_state,
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
    ui = _manager_mini_ui(token)
    ui["manager_claims"] = claims
    return await _render_chat_workspace(
        request=request,
        db=db,
        conversation_id=conversation_id,
        q=q,
        view=view,
        folder_id=folder_id,
        ui=ui,
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
        if not _is_unlimited_plan(db, workspace_id=workspace_id):
            folders_count = db.query(ChatFolder).filter(ChatFolder.workspace_id == workspace_id).count()
            folders_limit = _folder_limit_for_workspace(db, workspace_id=workspace_id)
            if folders_count >= folders_limit:
                return RedirectResponse(
                    url=_manager_mini_url(
                        token=token,
                        conversation_id=conversation_id,
                        q=q,
                        view=view,
                        folder_id=folder_id,
                        extra="folder_limit=1",
                    ),
                    status_code=302,
                )
        created = create_chat_folder(db, folder_name=folder_name, workspace_id=workspace_id)
        if conversation_id is not None:
            _assign_new_folder_to_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
                new_folder_id=created.id,
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


@app.post("/mini/manager/chats/folders/delete", response_class=RedirectResponse)
def manager_mini_delete_folders(
    request: Request,
    token: str,
    folder_ids: str = Form(""),
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
    target_ids = _resolve_folder_ids_from_form(folder_ids_csv=folder_ids, folder_id=folder_id)
    removed_ids = delete_chat_folders(
        db,
        folder_ids=target_ids,
        workspace_id=workspace_id,
    )
    removed_set = {int(value) for value in removed_ids}
    next_folder_filter = (
        current_folder_id
        if current_folder_id is not None and int(current_folder_id) not in removed_set
        else None
    )
    removed_suffix = "1" if removed_ids else "0"
    removed_count_suffix = f"&folders_removed_count={len(removed_ids)}" if removed_ids else ""
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            q=q,
            view=view,
            folder_id=next_folder_filter,
            extra=f"folders_removed={removed_suffix}{removed_count_suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def manager_mini_mark_unread(
    request: Request,
    conversation_id: int,
    token: str,
    current_conversation_id: int | None = Form(default=None),
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
    target_conversation = (
        current_conversation_id
        if current_conversation_id is not None
        else conversation_id
    )
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=target_conversation,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"mark_read=0&unread={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def manager_mini_move_folder(
    request: Request,
    conversation_id: int,
    token: str,
    folder_id: int = Form(0),
    folder_ids: str = Form(""),
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
    next_folder_ids = _resolve_folder_ids_from_form(
        folder_ids_csv=folder_ids,
        folder_id=folder_id,
    )
    ok = replace_conversation_folder_links(
        db=db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=workspace_id,
    )
    suffix = "1" if ok else "0"
    multi_suffix = "1" if ok and len(next_folder_ids) > 1 else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=current_folder_id,
            extra=f"foldered={suffix}&foldered_multi={multi_suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/bulk-action", response_class=RedirectResponse)
async def manager_mini_bulk_action(
    request: Request,
    token: str,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    manager_user_id = int(claims.get("service_user_id") or 0)
    if manager_user_id <= 0:
        raise HTTPException(status_code=403, detail="Недостаточно прав")

    payload = await _parse_bulk_action_request_payload(request)
    action = str(payload.get("action") or "").strip().lower()
    conversation_ids = _parse_conversation_ids_from_payload(payload)
    q_value = str(payload.get("q") or "").strip()
    view_value = str(payload.get("view") or "").strip()
    current_folder_id = _parse_optional_int(payload.get("current_folder_id"))

    moved_total = 0
    deleted_total = 0
    moved_multi = False

    if action == "move":
        folder_ids = _parse_folder_ids_from_payload(payload)
        moved_multi = len(folder_ids) > 1
        for conversation_id in conversation_ids:
            moved = replace_conversation_folder_links(
                db=db,
                conversation_id=conversation_id,
                folder_ids=folder_ids,
                workspace_id=workspace_id,
            )
            if moved:
                moved_total += 1
    elif action == "delete":
        _ensure_manager_can_delete_chats(
            db,
            workspace_id=workspace_id,
            manager_user_id=manager_user_id,
        )
        for conversation_id in conversation_ids:
            deleted = delete_conversation(
                db,
                conversation_id=conversation_id,
                workspace_id=workspace_id,
            )
            if not deleted:
                continue
            deleted_total += 1
            db.add(
                AuditLog(
                    workspace_id=workspace_id,
                    actor_user_id=manager_user_id,
                    action="customer_deleted",
                    object_type="conversation",
                    object_id=str(conversation_id),
                    details_json=safe_json_dumps({"source": "mini_manager_chats_bulk"}),
                )
            )
            db.commit()
    else:
        raise HTTPException(status_code=400, detail="invalid_bulk_action")

    redirect_url = _bulk_operation_redirect_url(
        base_path="/mini/manager",
        q=q_value,
        view=view_value,
        folder_id=current_folder_id,
        deleted_total=deleted_total,
        moved_total=moved_total,
        moved_multi=moved_multi,
    )
    token_qs = f"token={quote_plus(token)}"
    joiner = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{joiner}{token_qs}", status_code=302)


@app.post("/mini/manager/chats/{conversation_id}/delete-user", response_class=RedirectResponse)
def manager_mini_delete_conversation(
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
    manager_user_id = int(claims.get("service_user_id") or 0)
    if manager_user_id <= 0:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    _ensure_manager_can_delete_chats(
        db,
        workspace_id=workspace_id,
        manager_user_id=manager_user_id,
    )
    deleted = delete_conversation(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )
    if deleted:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=manager_user_id,
                action="customer_deleted",
                object_type="conversation",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"source": "mini_manager_chats"}),
            )
        )
        db.commit()
    suffix = "1" if deleted else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"removed={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/pin", response_class=RedirectResponse)
def manager_mini_pin_chat(
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
    manager_user_id = int(claims.get("service_user_id") or 0)
    if manager_user_id <= 0:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    pinned, reason = pin_conversation_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=manager_user_id,
        conversation_id=conversation_id,
    )
    if pinned:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=manager_user_id,
                action="chat_pinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "mini_manager"}),
            )
        )
        db.commit()
    suffix = "1" if pinned else "0"
    limit_suffix = "&pin_limit=1" if reason == "pinned_chats_limit_exceeded" else ""
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"pinned={suffix}{limit_suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/{conversation_id}/unpin", response_class=RedirectResponse)
def manager_mini_unpin_chat(
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
    manager_user_id = int(claims.get("service_user_id") or 0)
    if manager_user_id <= 0:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    unpinned = unpin_conversation_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=manager_user_id,
        conversation_id=conversation_id,
    )
    if unpinned:
        db.add(
            AuditLog(
                workspace_id=workspace_id,
                actor_user_id=manager_user_id,
                action="chat_unpinned",
                object_type="conversation_pin",
                object_id=str(conversation_id),
                details_json=safe_json_dumps({"scope": "mini_manager"}),
            )
        )
        db.commit()
    suffix = "1" if unpinned else "0"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view,
            folder_id=folder_id,
            extra=f"unpinned={suffix}",
        ),
        status_code=302,
    )


@app.post("/mini/manager/chats/pins/reorder", response_class=JSONResponse)
async def manager_mini_reorder_pins(
    request: Request,
    token: str,
    db: Session = Depends(get_db),
) -> JSONResponse:
    _check_rate_limit_or_raise(
        request,
        scope="mini_ops",
        limit=max(1, int(settings.rate_limit_login_per_minute) * 4),
    )
    claims = _require_manager_mini_access(token=token, db=db)
    workspace_id = int(claims.get("workspace_id") or DEFAULT_WORKSPACE_ID)
    manager_user_id = int(claims.get("service_user_id") or 0)
    if manager_user_id <= 0:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    payload = await request.json()
    ids_raw = payload.get("conversation_ids") if isinstance(payload, dict) else None
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ordered_ids: list[int] = []
    seen: set[int] = set()
    for item in ids_raw:
        try:
            conv_id = int(item)
        except (TypeError, ValueError):
            continue
        if conv_id <= 0 or conv_id in seen:
            continue
        seen.add(conv_id)
        ordered_ids.append(conv_id)
    if not ordered_ids:
        raise HTTPException(status_code=400, detail="conversation_ids_required")
    ok = reorder_pins_for_user(
        db,
        workspace_id=workspace_id,
        service_user_id=manager_user_id,
        ordered_conversation_ids=ordered_ids,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="invalid_pin_order")
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=manager_user_id,
            action="chat_pins_reordered",
            object_type="conversation_pin",
            object_id=str(manager_user_id),
            details_json=safe_json_dumps({"conversation_ids": ordered_ids, "scope": "mini_manager"}),
        )
    )
    db.commit()
    return JSONResponse({"ok": True, "ordered_count": len(ordered_ids)}, status_code=200)


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
    photos: list[UploadFile] = File(default=[]),
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
    uploads_to_validate = [item for item in photos if item and item.filename]
    if not uploads_to_validate:
        form_data = await request.form()
        legacy_photo = form_data.get("photo")
        if isinstance(legacy_photo, UploadFile) and legacy_photo.filename:
            uploads_to_validate = [legacy_photo]
    validated_uploads = await _read_and_validate_uploads(uploads_to_validate)
    image_paths = _store_uploaded_images(validated_uploads)

    if text_value.startswith("/") and not image_paths:
        quick_reply_owner_id = _chat_scope_quick_reply_owner_id(manager_claims=claims)
        sent_ok, quick_error = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
            workspace_id=workspace_id,
            owner_user_id=quick_reply_owner_id,
        )
        suffix = "1" if sent_ok else "0"
        extra = f"quick={suffix}"
        if not sent_ok and quick_error:
            extra += f"&quick_error={_encode_chat_error(quick_error)}"
        return RedirectResponse(
            url=_manager_mini_url(
                token=token,
                conversation_id=conversation_id,
                q=q,
                view=view_value,
                folder_id=folder_id,
                extra=extra,
            ),
            status_code=302,
        )

    schedule_at_value = await _resolve_schedule_at_value(request, schedule_at)
    sent_ok = False
    send_error_reason = ""
    try:
        sent_ok, send_error_reason = await send_admin_chat_message(
            db=db,
            conversation_id=conversation_id,
            text=text_value,
            image_paths=image_paths,
            workspace_id=workspace_id,
            schedule_at_iso=schedule_at_value,
        )
    finally:
        if not sent_ok and image_paths:
            _cleanup_uploaded_images(image_paths)
    scheduled_at_clean = schedule_at_value
    is_scheduled = bool(scheduled_at_clean)
    suffix = "1" if sent_ok else "0"
    flag_name = "scheduled" if is_scheduled else "sent"
    extra = f"{flag_name}={suffix}"
    if not sent_ok and send_error_reason:
        extra += f"&send_error={_encode_chat_error(send_error_reason)}"
    return RedirectResponse(
        url=_manager_mini_url(
            token=token,
            conversation_id=conversation_id,
            q=q,
            view=view_value,
            folder_id=folder_id,
            extra=extra,
        ),
        status_code=302,
    )


@app.post(f"{webhook_path}/{{webhook_key}}")
async def max_webhook(
    webhook_key: str,
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

    import logging as _logging
    _wh_log = _logging.getLogger("webhook.debug")
    if not event.image_urls and event.update_type == "message_created":
        _message_node = payload.get("message") or {}
        _link_node = _message_node.get("link") or {}
        if _link_node:
            _wh_log.warning(
                "[FORWARDED_NO_IMAGE] link keys=%s link_message_keys=%s full_payload=%s",
                list(_link_node.keys()),
                list((_link_node.get("message") or {}).keys()),
                payload,
            )

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
        "message_read",
        "read",
        "seen",
        "opened",
        "message_seen",
        "bot_started",
        "bot_start",
    }
    if event.update_type and event.update_type not in accepted_update_types:
        return {"ok": True, "ignored": event.update_type}

    workspace_id = _resolve_workspace_by_webhook_key(db, webhook_key) or _resolve_workspace_id_from_event(db, event)
    settings_db = get_or_create_settings(db, workspace_id=workspace_id)
    max_client, client_error = _workspace_client_or_error(settings_db)
    if client_error or max_client is None:
        return {"ok": True, "ignored": "bot_token_not_configured"}

    sender_id = (event.sender_id or "").strip()
    if settings_db.admin_account_id and sender_id == (settings_db.admin_account_id or "").strip():
        return {"ok": True, "ignored": "admin"}

    if sender_id in _manager_ids_from_settings_row(settings_db):
        return await handle_manager_message(
            db=db,
            client=max_client,
            settings=settings_db,
            event=event,
        )

    update_type = (event.update_type or "").strip().lower()
    if update_type == "message_read":
        _read_flow_log(
            "webhook_message_read_received",
            update_type=update_type,
            chat_id=event.chat_id,
            sender_id=event.sender_id,
            read_message_mid=event.read_message_mid,
            event_uid=event_uid,
        )
        changed = mark_conversation_messages_read_by_customer(
            db,
            workspace_id=workspace_id,
            chat_id=event.chat_id,
            customer_id=event.sender_id,
            read_up_to_mid=event.read_message_mid,
        )
        _read_flow_log(
            "webhook_message_read_applied",
            chat_id=event.chat_id,
            sender_id=event.sender_id,
            read_message_mid=event.read_message_mid,
            marked_read_count=int(changed),
        )
        return {
            "ok": True,
            "flow": "message_read",
            "marked_read_count": int(changed),
            "read_message_mid": str(event.read_message_mid or ""),
        }

    result = await handle_customer_event(
        db=db,
        client=max_client,
        settings=settings_db,
        event=event,
    )
    # This branch handles non-admin/non-manager, non-read events,
    # i.e. customer-originated message flow. Always emit WS hint.
    with suppress(Exception):
        conversation_hint_id = _extract_conversation_id_from_result(result)
        if conversation_hint_id is None or int(conversation_hint_id) <= 0:
            conversation_hint_id = _resolve_conversation_hint_id(
                db,
                workspace_id=workspace_id,
                chat_id=event.chat_id,
                sender_id=event.sender_id,
            )
        _ws_hint_log(
            "scheduled_from_webhook",
            workspace_id=workspace_id,
            conversation_id=conversation_hint_id,
            update_type=event.update_type,
            chat_id=event.chat_id,
            sender_id=event.sender_id,
        )
        asyncio.create_task(
            _broadcast_workspace_chat_update(
                workspace_id=workspace_id,
                conversation_id=conversation_hint_id,
                source="incoming_customer_message",
            )
        )
    return result
