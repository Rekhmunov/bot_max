from __future__ import annotations

import re
import secrets
from typing import cast

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.auth import hash_password
from app.config import settings
from app.max_client import MaxClient
from app.models import BotSettings, Conversation, MessageLog, QuickReply, ServiceUser, Workspace

DEFAULT_WORKSPACE_ID = 1
_USERNAME_RE = re.compile(r"^[a-z0-9_.\-]{3,64}$")


def normalize_username(value: str) -> str:
    return (value or "").strip().lower()


def validate_username(value: str) -> bool:
    return bool(_USERNAME_RE.fullmatch(normalize_username(value)))


def ensure_default_workspace(db: Session) -> Workspace:
    workspace = db.query(Workspace).filter(Workspace.id == DEFAULT_WORKSPACE_ID).first()
    if workspace:
        return workspace
    workspace = Workspace(
        id=DEFAULT_WORKSPACE_ID,
        name="Default Workspace",
        tenant_code="default",
        is_active=True,
        is_suspended=False,
    )
    db.add(workspace)
    db.commit()
    db.refresh(workspace)
    return workspace


def get_workspace_by_tenant_code(db: Session, tenant_code: str) -> Workspace | None:
    value = (tenant_code or "").strip().lower()
    if not value:
        return None
    return db.query(Workspace).filter(func.lower(Workspace.tenant_code) == value).first()


def generate_tenant_code(db: Session, seed: str | None = None) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (seed or "").strip().lower()).strip("-")
    base = base[:24] if base else "tenant"
    while True:
        suffix = secrets.token_hex(2)
        code = f"{base}-{suffix}"
        exists = db.query(Workspace.id).filter(Workspace.tenant_code == code).first()
        if not exists:
            return code


def create_workspace(db: Session, *, name: str, tenant_code: str | None = None) -> Workspace:
    normalized_name = (name or "").strip() or "Workspace"
    code = (tenant_code or "").strip().lower() or generate_tenant_code(db, seed=normalized_name)
    workspace = Workspace(name=normalized_name, tenant_code=code, is_active=True, is_suspended=False)
    db.add(workspace)
    db.commit()
    db.refresh(workspace)
    return workspace


def create_service_user(
    db: Session,
    *,
    username: str,
    password: str,
    role: str,
    workspace_id: int | None,
    display_name: str = "",
    max_account_id: str = "",
) -> ServiceUser:
    normalized = normalize_username(username)
    if not validate_username(normalized):
        raise ValueError("invalid_username")
    if len((password or "").strip()) < 8:
        raise ValueError("password_too_short")
    existing = db.query(ServiceUser.id).filter(ServiceUser.username == normalized).first()
    if existing:
        raise ValueError("username_exists")
    user = ServiceUser(
        workspace_id=workspace_id,
        role=(role or "owner").strip().lower(),
        username=normalized,
        password_hash=hash_password(password.strip()),
        display_name=(display_name or "").strip(),
        max_account_id=(max_account_id or "").strip(),
        is_active=True,
        is_blocked=False,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def create_workspace_with_owner(
    db: Session,
    *,
    workspace_name: str,
    username: str,
    password: str,
    display_name: str = "",
) -> tuple[Workspace, ServiceUser]:
    workspace = create_workspace(db, name=workspace_name)
    owner = create_service_user(
        db,
        username=username,
        password=password,
        role="owner",
        workspace_id=workspace.id,
        display_name=display_name,
    )
    return workspace, owner


def list_workspace_managers(db: Session, *, workspace_id: int) -> list[ServiceUser]:
    return (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.is_active.is_(True),
        )
        .order_by(ServiceUser.id.asc())
        .all()
    )


def get_or_create_settings(db: Session, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> BotSettings:
    ensure_default_workspace(db)
    bot_settings = db.query(BotSettings).filter(BotSettings.workspace_id == workspace_id).first()
    if bot_settings:
        return bot_settings

    bot_settings = BotSettings(workspace_id=workspace_id)
    db.add(bot_settings)
    db.commit()
    db.refresh(bot_settings)
    return bot_settings


def _get_or_create_conversation(
    db: Session,
    *,
    workspace_id: int,
    chat_id: str,
    customer_id: str,
) -> Conversation:
    conversation = (
        db.query(Conversation)
        .filter(
            Conversation.workspace_id == workspace_id,
            Conversation.chat_id == chat_id,
        )
        .first()
    )
    if conversation:
        return conversation

    conversation = Conversation(
        workspace_id=workspace_id,
        chat_id=chat_id,
        customer_account_id=customer_id,
        manager_added=False,
    )
    db.add(conversation)
    db.commit()
    db.refresh(conversation)
    return conversation


async def process_incoming_customer_message(
    db: Session,
    client: MaxClient,
    customer_id: str,
    chat_id: str,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> None:
    bot_settings = get_or_create_settings(db, workspace_id=workspace_id)
    conversation = _get_or_create_conversation(
        db=db,
        workspace_id=workspace_id,
        chat_id=chat_id,
        customer_id=customer_id,
    )

    message_log = MessageLog(
        workspace_id=workspace_id,
        conversation_id=conversation.id,
        sender_account_id=customer_id,
        message_text="customer_message",
        message_type="event",
    )
    db.add(message_log)

    if not conversation.manager_added:
        await client.send_text(chat_id=chat_id, text=bot_settings.greeting_text)
        if bot_settings.manager_account_id:
            add_result = await client.add_member_to_chat(
                chat_id=chat_id,
                account_id=bot_settings.manager_account_id,
            )
            add_ok = bool(add_result.get("success", True))
            if add_ok:
                if bot_settings.manager_added_notice_text:
                    await client.send_text(chat_id=chat_id, text=bot_settings.manager_added_notice_text)
                conversation.manager_added = True
            else:
                error_message = f"manager_add_failed: {add_result}"
                db.add(
                    MessageLog(
                        workspace_id=workspace_id,
                        conversation_id=conversation.id,
                        sender_account_id="bot",
                        message_text=error_message,
                        message_type="error",
                    ),
                )

    db.add(conversation)
    db.commit()


async def handle_manager_command(
    db: Session,
    client: MaxClient,
    chat_id: str,
    command_text: str,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> bool:
    command = command_text.strip().lstrip("/").strip().lower()
    if not command:
        return False

    quick_reply = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.command == command,
            QuickReply.is_active.is_(True),
        )
        .first()
    )
    if not quick_reply:
        return False

    if quick_reply.text:
        send_result = await client.send_text(chat_id=chat_id, text=quick_reply.text)
        if not send_result.get("success", True):
            return False

    if quick_reply.image_path:
        image_url = f"{settings.public_base_url.rstrip('/')}{quick_reply.image_path}"
        image_result = await client.send_photo(chat_id=chat_id, photo_url=image_url)
        if not image_result.get("success", True):
            return False

    return True
