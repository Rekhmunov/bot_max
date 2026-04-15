from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), default="Workspace")
    tenant_code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_suspended: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    suspended_by_admin: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ServiceUser(Base):
    __tablename__ = "service_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int | None] = mapped_column(ForeignKey("workspaces.id"), nullable=True, index=True)
    role: Mapped[str] = mapped_column(String(32), default="user", index=True)
    username: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(512), default="")
    display_name: Mapped[str] = mapped_column(String(255), default="")
    max_account_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    can_delete_chats: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    email_verification_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    totp_secret: Mapped[str] = mapped_column(String(255), default="")
    totp_enabled: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class ManagerInvite(Base):
    __tablename__ = "manager_invites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    max_account_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    invite_token_hash: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="")
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    is_used: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    used_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("service_users.id"), nullable=True)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class IntroStep(Base):
    __tablename__ = "intro_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    step_order: Mapped[int] = mapped_column(Integer, default=1, index=True)
    delay_seconds: Mapped[int] = mapped_column(Integer, default=0)
    text: Mapped[str] = mapped_column(Text, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    plan_code: Mapped[str] = mapped_column(String(64), default="basic")
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    manager_limit: Mapped[int] = mapped_column(Integer, default=3)
    dialogs_limit: Mapped[int] = mapped_column(Integer, default=500)
    messages_per_month_limit: Mapped[int] = mapped_column(Integer, default=5000)
    quick_replies_limit: Mapped[int] = mapped_column(Integer, default=10)
    folders_limit: Mapped[int] = mapped_column(Integer, default=10)
    pinned_chats_limit: Mapped[int] = mapped_column(Integer, default=5)
    grace_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    current_period_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TariffPlan(Base):
    __tablename__ = "tariff_plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    manager_limit: Mapped[int] = mapped_column(Integer, default=3)
    dialogs_limit: Mapped[int] = mapped_column(Integer, default=500)
    messages_per_month_limit: Mapped[int] = mapped_column(Integer, default=5000)
    quick_replies_limit: Mapped[int] = mapped_column(Integer, default=10)
    folders_limit: Mapped[int] = mapped_column(Integer, default=10)
    pinned_chats_limit: Mapped[int] = mapped_column(Integer, default=5)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    billing_product_code: Mapped[str] = mapped_column(String(128), default="")
    billing_price_code: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UserSession(Base):
    __tablename__ = "user_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("service_users.id"), index=True)
    session_token_hash: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    is_revoked: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ip_address: Mapped[str] = mapped_column(String(128), default="")
    user_agent: Mapped[str] = mapped_column(String(512), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int | None] = mapped_column(ForeignKey("workspaces.id"), nullable=True, index=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("service_users.id"), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(120), index=True)
    object_type: Mapped[str] = mapped_column(String(120), default="")
    object_id: Mapped[str] = mapped_column(String(120), default="")
    details_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class PlatformSettings(Base):
    __tablename__ = "platform_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    technical_support_email: Mapped[str] = mapped_column(String(255), default="")
    billing_support_email: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BillingEvent(Base):
    __tablename__ = "billing_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(120), index=True)
    external_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class BotSettings(Base):
    __tablename__ = "bot_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    greeting_text: Mapped[str] = mapped_column(
        Text,
        default="Здравствуйте! Сейчас подключу менеджера к диалогу.",
    )
    manager_account_id: Mapped[str] = mapped_column(String(255), default="")
    admin_account_id: Mapped[str] = mapped_column(String(255), default="")
    manager_added_notice_text: Mapped[str] = mapped_column(
        Text,
        default="Менеджер подключен к диалогу.",
    )
    bot_token: Mapped[str] = mapped_column(Text, default="")
    bot_link: Mapped[str] = mapped_column(String(255), default="")
    webhook_key: Mapped[str] = mapped_column(String(96), default="", index=True)
    request_customer_phone: Mapped[bool] = mapped_column(Boolean, default=True)
    routing_mode: Mapped[str] = mapped_column(String(20), default="round_robin")
    routing_rr_cursor: Mapped[int] = mapped_column(Integer, default=0)


class QuickReply(Base):
    __tablename__ = "quick_replies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    owner_user_id: Mapped[int] = mapped_column(Integer, default=0, index=True)
    command: Mapped[str] = mapped_column(String(100), index=True)
    title: Mapped[str] = mapped_column(String(255))
    text: Mapped[str] = mapped_column(Text, default="")
    image_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class QuickReplyMedia(Base):
    __tablename__ = "quick_reply_media"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    quick_reply_id: Mapped[int] = mapped_column(ForeignKey("quick_replies.id"), index=True)
    media_path: Mapped[str] = mapped_column(String(1000))
    sort_order: Mapped[int] = mapped_column(Integer, default=0, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class MediaAsset(Base):
    __tablename__ = "media_assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    storage_provider: Mapped[str] = mapped_column(String(32), default="local", index=True)
    storage_key: Mapped[str] = mapped_column(String(1024))
    public_url: Mapped[str] = mapped_column(String(2048), default="")
    mime_type: Mapped[str] = mapped_column(String(128), default="")
    byte_size: Mapped[int] = mapped_column(Integer, default=0)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sha256: Mapped[str] = mapped_column(String(64), default="", index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ref_count: Mapped[int] = mapped_column(Integer, default=0)
    last_accessed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    storage_tier: Mapped[str] = mapped_column(String(32), default="hot")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ChatMessageMedia(Base):
    __tablename__ = "chat_message_media"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    chat_message_id: Mapped[int] = mapped_column(ForeignKey("chat_messages.id"), index=True)
    media_asset_id: Mapped[int] = mapped_column(ForeignKey("media_assets.id"), index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, index=True)
    role: Mapped[str] = mapped_column(String(32), default="image")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class QuickReplyMediaAssetLink(Base):
    __tablename__ = "quick_reply_media_asset_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    quick_reply_id: Mapped[int] = mapped_column(ForeignKey("quick_replies.id"), index=True)
    media_asset_id: Mapped[int] = mapped_column(ForeignKey("media_assets.id"), index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    chat_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    customer_account_id: Mapped[str] = mapped_column(String(255))
    manager_added: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    folder_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    messages: Mapped[list["MessageLog"]] = relationship(
        back_populates="conversation",
        cascade="all, delete-orphan",
    )
    chat_messages: Mapped[list["ChatMessage"]] = relationship(
        back_populates="conversation",
        cascade="all, delete-orphan",
    )


class MessageLog(Base):
    __tablename__ = "message_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    sender_account_id: Mapped[str] = mapped_column(String(255))
    message_text: Mapped[str] = mapped_column(Text, default="")
    message_type: Mapped[str] = mapped_column(String(50), default="text")

    conversation: Mapped[Conversation] = relationship(back_populates="messages")


class ChatMessage(Base):
    __tablename__ = "chat_messages"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    direction: Mapped[str] = mapped_column(String(50), default="customer")  # customer | bot
    source: Mapped[str] = mapped_column(String(50), default="customer")  # customer | manager | bot_system
    text: Mapped[str] = mapped_column(Text, default="")
    image_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    image_urls_json: Mapped[str] = mapped_column(Text, default="[]")
    max_message_mid: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    link_mid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    delivery_state: Mapped[str] = mapped_column(String(20), default="sent", index=True)  # sent | queued | failed
    is_scheduled_message: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    is_read_by_customer: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    delivery_error: Mapped[str] = mapped_column(Text, default="")
    delivery_retry_count: Mapped[int] = mapped_column(Integer, default=0)
    delivery_next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    conversation: Mapped[Conversation] = relationship(back_populates="chat_messages")


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int | None] = mapped_column(ForeignKey("conversations.id"), index=True, nullable=True)
    chat_message_id: Mapped[int | None] = mapped_column(ForeignKey("chat_messages.id"), index=True, nullable=True)
    target_chat_id: Mapped[str] = mapped_column(String(255), index=True)
    target_user_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    operation: Mapped[str] = mapped_column(String(50), default="send_text")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    state: Mapped[str] = mapped_column(String(20), default="queued", index=True)  # queued | sent | failed
    is_permanent_failure: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str] = mapped_column(Text, default="")
    external_message_mid: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), default="", index=True)
    idempotency_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class MessageTemplate(Base):
    __tablename__ = "message_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    template_key: Mapped[str] = mapped_column(String(100), index=True)
    template_text: Mapped[str] = mapped_column(Text, default="")


class ConversationMeta(Base):
    __tablename__ = "conversation_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), unique=True, index=True)
    ticket_no: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(50), default="new")
    manager_owner_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    is_unread: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    manual_unread_mark: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    phone_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    start_prompt_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    intro_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    last_start_intent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_start_intent_stage: Mapped[str] = mapped_column(String(32), default="")
    phone_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    unread_errors_count: Mapped[int] = mapped_column(Integer, default=0)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    blocked_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    blocked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    blocked_prev_folder_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    blocked_notice_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    offhours_notice_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class WorkspaceBusinessHours(Base):
    __tablename__ = "workspace_business_hours"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    offhours_message: Mapped[str] = mapped_column(
        Text,
        default="Сейчас мы вне рабочего времени. Мы ответим в рабочие часы.",
    )
    cooldown_seconds: Mapped[int] = mapped_column(Integer, default=21600)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class WorkspaceBusinessSlot(Base):
    __tablename__ = "workspace_business_slots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    weekday: Mapped[int] = mapped_column(Integer, default=0, index=True)  # 0=Mon .. 6=Sun
    start_minute: Mapped[int] = mapped_column(Integer, default=540)  # 09:00
    end_minute: Mapped[int] = mapped_column(Integer, default=1080)  # 18:00
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class WorkspaceBusinessException(Base):
    __tablename__ = "workspace_business_exceptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    date_from: Mapped[date] = mapped_column(Date, index=True)
    date_to: Mapped[date] = mapped_column(Date, index=True)
    mode: Mapped[str] = mapped_column(String(24), default="closed_all_day")
    start_minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    note: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class TenantAlert(Base):
    __tablename__ = "tenant_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    alert_key: Mapped[str] = mapped_column(String(120), index=True)
    severity: Mapped[str] = mapped_column(String(20), default="warning", index=True)
    message: Mapped[str] = mapped_column(Text, default="")
    metric_value: Mapped[float] = mapped_column(Float, default=0.0)
    is_resolved: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class WorkspaceRetentionPolicy(Base):
    __tablename__ = "workspace_retention_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True)
    outbox_sent_ttl_days: Mapped[int] = mapped_column(Integer, default=30)
    outbox_failed_ttl_days: Mapped[int] = mapped_column(Integer, default=90)
    message_logs_ttl_days: Mapped[int] = mapped_column(Integer, default=180)
    chat_messages_ttl_days: Mapped[int] = mapped_column(Integer, default=0)
    deleted_media_grace_days: Mapped[int] = mapped_column(Integer, default=7)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StorageCleanupRun(Base):
    __tablename__ = "storage_cleanup_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="running", index=True)
    details_json: Mapped[str] = mapped_column(Text, default="{}")


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    customer_account_id: Mapped[str] = mapped_column(String(255), index=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone_number: Mapped[str] = mapped_column(String(64), default="")
    source_chat_id: Mapped[str] = mapped_column(String(255), default="")


class ChatFolder(Base):
    __tablename__ = "chat_folders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    name: Mapped[str] = mapped_column(String(120), index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, index=True)


class ConversationFolderLink(Base):
    __tablename__ = "conversation_folder_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    folder_id: Mapped[int] = mapped_column(ForeignKey("chat_folders.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class ConversationPin(Base):
    __tablename__ = "conversation_pins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    service_user_id: Mapped[int] = mapped_column(ForeignKey("service_users.id"), index=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ManagerDispatch(Base):
    __tablename__ = "manager_dispatches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), index=True, default=1)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    manager_message_mid: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    dispatch_type: Mapped[str] = mapped_column(String(50), default="customer_to_manager")


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_uid: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    update_type: Mapped[str] = mapped_column(String(100), default="")
    received_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


Index(
    "ix_quick_replies_workspace_owner_command",
    QuickReply.workspace_id,
    QuickReply.owner_user_id,
    QuickReply.command,
    unique=True,
)
Index("ix_message_templates_workspace_key", MessageTemplate.workspace_id, MessageTemplate.template_key, unique=True)
Index("ix_chat_folders_workspace_name", ChatFolder.workspace_id, ChatFolder.name, unique=True)
Index(
    "ix_workspace_business_slots_workspace_weekday_start",
    WorkspaceBusinessSlot.workspace_id,
    WorkspaceBusinessSlot.weekday,
    WorkspaceBusinessSlot.start_minute,
)
Index(
    "ix_workspace_business_exceptions_workspace_date",
    WorkspaceBusinessException.workspace_id,
    WorkspaceBusinessException.date_from,
    WorkspaceBusinessException.date_to,
)
Index(
    "ix_conversation_folder_links_workspace_conversation_folder",
    ConversationFolderLink.workspace_id,
    ConversationFolderLink.conversation_id,
    ConversationFolderLink.folder_id,
    unique=True,
)
Index(
    "ix_customer_profiles_workspace_customer",
    CustomerProfile.workspace_id,
    CustomerProfile.customer_account_id,
    unique=True,
)
Index(
    "ix_conversation_pins_workspace_user_conversation",
    ConversationPin.workspace_id,
    ConversationPin.service_user_id,
    ConversationPin.conversation_id,
    unique=True,
)
Index(
    "ix_conversation_pins_workspace_user_sort",
    ConversationPin.workspace_id,
    ConversationPin.service_user_id,
    ConversationPin.sort_order,
)
Index(
    "ux_workspace_retention_policies_workspace",
    WorkspaceRetentionPolicy.workspace_id,
    unique=True,
)
Index(
    "ux_media_assets_workspace_storage",
    MediaAsset.workspace_id,
    MediaAsset.storage_provider,
    MediaAsset.storage_key,
    unique=True,
)
Index(
    "ix_media_assets_workspace_deleted",
    MediaAsset.workspace_id,
    MediaAsset.is_deleted,
    MediaAsset.deleted_at,
)
Index(
    "ux_chat_message_media_message_asset_role",
    ChatMessageMedia.chat_message_id,
    ChatMessageMedia.media_asset_id,
    ChatMessageMedia.role,
    unique=True,
)
Index(
    "ux_quick_reply_media_asset_links_reply_asset",
    QuickReplyMediaAssetLink.quick_reply_id,
    QuickReplyMediaAssetLink.media_asset_id,
    unique=True,
)
