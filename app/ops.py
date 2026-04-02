from __future__ import annotations

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.models import BillingEvent, Conversation, ServiceUser, Subscription, TenantAlert, Workspace
from app.services import DEFAULT_WORKSPACE_ID


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def get_or_create_subscription(db: Session, *, workspace_id: int) -> Subscription:
    sub = db.query(Subscription).filter(Subscription.workspace_id == workspace_id).first()
    if sub:
        return sub
    now = _now()
    sub = Subscription(
        workspace_id=workspace_id,
        plan_code="trial",
        status="active",
        manager_limit=3,
        dialogs_limit=500,
        messages_per_month_limit=5000,
        current_period_start=now,
        current_period_end=now + timedelta(days=30),
        grace_until=now + timedelta(days=settings.default_grace_days),
    )
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return sub


def ensure_workspace_active_by_billing(db: Session, *, workspace_id: int) -> bool:
    workspace = db.query(Workspace).filter(Workspace.id == workspace_id).first()
    if workspace is None:
        return False
    if workspace.is_manually_suspended:
        return False
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    now = _now()
    should_suspend = sub.status in {"past_due", "paused", "cancelled"} and (
        sub.grace_until is None or sub.grace_until < now
    )
    changed = False
    if should_suspend and not workspace.is_suspended:
        workspace.is_suspended = True
        changed = True
    if not should_suspend and workspace.is_suspended and sub.status in {"active", "trial"}:
        workspace.is_suspended = False
        changed = True
    if changed:
        db.add(workspace)
        db.commit()
    return not workspace.is_suspended


def can_add_manager(db: Session, *, workspace_id: int) -> tuple[bool, str]:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    count = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.is_active.is_(True),
        )
        .count()
    )
    if count >= int(sub.manager_limit or 0):
        return False, "manager_limit_exceeded"
    return True, ""


def can_create_dialog(db: Session, *, workspace_id: int) -> tuple[bool, str]:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    dialogs = db.query(Conversation).filter(Conversation.workspace_id == workspace_id).count()
    if dialogs >= int(sub.dialogs_limit or 0):
        return False, "dialogs_limit_exceeded"
    return True, ""


def can_send_message_this_month(db: Session, *, workspace_id: int) -> tuple[bool, str]:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    now = _now()
    period_start = sub.current_period_start or (now - timedelta(days=30))
    sent_count = (
        db.query(BillingEvent)
        .filter(
            BillingEvent.workspace_id == workspace_id,
            BillingEvent.event_type == "message_sent",
            BillingEvent.created_at >= period_start,
        )
        .count()
    )
    if sent_count >= int(sub.messages_per_month_limit or 0):
        return False, "messages_limit_exceeded"
    return True, ""


def track_message_sent(db: Session, *, workspace_id: int, external_id: str = "") -> None:
    db.add(
        BillingEvent(
            workspace_id=workspace_id,
            event_type="message_sent",
            external_id=(external_id or "")[:255],
            payload_json="{}",
            created_at=_now(),
        )
    )
    db.commit()


def apply_billing_hook(
    db: Session,
    *,
    workspace_id: int,
    event_type: str,
    external_id: str,
    payload_json: str,
) -> Subscription:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    now = _now()
    evt = (event_type or "").strip().lower()
    if evt in {"payment_succeeded", "subscription_resumed"}:
        sub.status = "active"
        sub.grace_until = now + timedelta(days=settings.default_grace_days)
    elif evt in {"payment_failed", "invoice_failed"}:
        sub.status = "past_due"
        sub.grace_until = now + timedelta(days=settings.default_grace_days)
    elif evt in {"subscription_cancelled"}:
        sub.status = "cancelled"
    elif evt in {"subscription_paused"}:
        sub.status = "paused"
    elif evt in {"subscription_trial"}:
        sub.status = "trial"
        sub.grace_until = now + timedelta(days=settings.default_grace_days)
    db.add(sub)
    db.add(
        BillingEvent(
            workspace_id=workspace_id,
            event_type=evt or "unknown",
            external_id=(external_id or "")[:255],
            payload_json=payload_json or "{}",
            created_at=now,
        )
    )
    db.commit()
    ensure_workspace_active_by_billing(db, workspace_id=workspace_id)
    db.refresh(sub)
    return sub


def collect_tenant_metrics(db: Session, *, workspace_id: int) -> dict[str, int]:
    managers = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.is_active.is_(True),
        )
        .count()
    )
    dialogs = db.query(Conversation).filter(Conversation.workspace_id == workspace_id).count()
    month_start = _now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    messages = (
        db.query(BillingEvent)
        .filter(
            BillingEvent.workspace_id == workspace_id,
            BillingEvent.event_type == "message_sent",
            BillingEvent.created_at >= month_start,
        )
        .count()
    )
    return {
        "managers_active": managers,
        "dialogs_total": dialogs,
        "messages_month": messages,
    }


def refresh_tenant_alerts(db: Session, *, workspace_id: int) -> list[TenantAlert]:
    sub = get_or_create_subscription(db, workspace_id=workspace_id)
    metrics = collect_tenant_metrics(db, workspace_id=workspace_id)
    thresholds = {
        "managers_limit": (metrics["managers_active"], int(sub.manager_limit or 0)),
        "dialogs_limit": (metrics["dialogs_total"], int(sub.dialogs_limit or 0)),
        "messages_month_limit": (metrics["messages_month"], int(sub.messages_per_month_limit or 0)),
    }
    created: list[TenantAlert] = []
    for key, (value, limit) in thresholds.items():
        if limit <= 0:
            continue
        ratio = value / limit if limit else 0.0
        severity = ""
        if ratio >= 1.0:
            severity = "critical"
        elif ratio >= 0.8:
            severity = "warning"
        existing = (
            db.query(TenantAlert)
            .filter(
                TenantAlert.workspace_id == workspace_id,
                TenantAlert.alert_key == key,
                TenantAlert.is_resolved.is_(False),
            )
            .first()
        )
        if severity:
            msg = f"{key}: {value}/{limit}"
            if existing is None:
                alert = TenantAlert(
                    workspace_id=workspace_id,
                    alert_key=key,
                    severity=severity,
                    message=msg,
                    metric_value=float(value),
                    is_resolved=False,
                    created_at=_now(),
                )
                db.add(alert)
                created.append(alert)
            else:
                existing.severity = severity
                existing.message = msg
                existing.metric_value = float(value)
                db.add(existing)
        elif existing is not None:
            existing.is_resolved = True
            existing.resolved_at = _now()
            db.add(existing)
    db.commit()
    return created


def create_sqlite_backup() -> Path:
    db_url = settings.database_url
    if not db_url.startswith("sqlite:///"):
        raise ValueError("backup_supported_only_for_sqlite")
    source = Path(db_url.replace("sqlite:///", "", 1))
    if not source.is_absolute():
        source = Path.cwd() / source
    source = source.resolve()
    if not source.exists():
        raise FileNotFoundError(str(source))
    backup_dir = Path(settings.backups_dir)
    if not backup_dir.is_absolute():
        backup_dir = (Path.cwd() / backup_dir).resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%d-%H%M%S")
    target = backup_dir / f"backup-{stamp}.sqlite3"
    shutil.copy2(source, target)
    return target


def restore_sqlite_backup(backup_name: str) -> Path:
    db_url = settings.database_url
    if not db_url.startswith("sqlite:///"):
        raise ValueError("restore_supported_only_for_sqlite")
    backup_dir = Path(settings.backups_dir)
    if not backup_dir.is_absolute():
        backup_dir = (Path.cwd() / backup_dir).resolve()
    source = (backup_dir / backup_name).resolve()
    if not source.exists():
        raise FileNotFoundError(str(source))
    db_path = Path(db_url.replace("sqlite:///", "", 1))
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    shutil.copy2(source, db_path)
    return db_path


def list_backups(limit: int = 20) -> list[str]:
    backup_dir = Path(settings.backups_dir)
    if not backup_dir.is_absolute():
        backup_dir = (Path.cwd() / backup_dir).resolve()
    if not backup_dir.exists():
        return []
    files = sorted(
        [p for p in backup_dir.iterdir() if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return [p.name for p in files[: max(limit, 1)]]


def ensure_workspace_limits_and_state(db: Session, *, workspace_id: int) -> tuple[bool, str]:
    if workspace_id <= 0:
        workspace_id = DEFAULT_WORKSPACE_ID
    active = ensure_workspace_active_by_billing(db, workspace_id=workspace_id)
    if not active:
        return False, "workspace_suspended_by_billing"
    return True, ""
