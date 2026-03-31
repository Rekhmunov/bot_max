from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from app import models

    models.Base.metadata.create_all(bind=engine)
    _ensure_lightweight_migrations()


def _ensure_lightweight_migrations() -> None:
    """
    Apply additive schema updates for SQLite deployments without Alembic.
    """
    inspector = inspect(engine)
    with engine.begin() as conn:
        table_names = set(inspector.get_table_names())

        # SaaS baseline tables are created by metadata.create_all.
        # Ensure there is always a default workspace for legacy data.
        if "workspaces" in table_names:
            conn.execute(
                text(
                    "INSERT OR IGNORE INTO workspaces (id, name, tenant_code, is_active, is_suspended, created_at, updated_at) "
                    "VALUES (1, 'Default Workspace', 'default', 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
            )

        if "chat_messages" in table_names:
            chat_columns = {col["name"] for col in inspector.get_columns("chat_messages")}
            if "workspace_id" not in chat_columns:
                conn.execute(
                    text("ALTER TABLE chat_messages ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )
            if "delivery_state" not in chat_columns:
                conn.execute(
                    text("ALTER TABLE chat_messages ADD COLUMN delivery_state VARCHAR(20) DEFAULT 'sent'")
                )
            if "delivery_error" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN delivery_error TEXT DEFAULT ''"))
            if "delivery_retry_count" not in chat_columns:
                conn.execute(
                    text("ALTER TABLE chat_messages ADD COLUMN delivery_retry_count INTEGER DEFAULT 0")
                )
            if "delivery_next_retry_at" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN delivery_next_retry_at DATETIME"))

        if "conversation_meta" in table_names:
            meta_columns = {col["name"] for col in inspector.get_columns("conversation_meta")}
            if "workspace_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )
            if "unread_errors_count" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN unread_errors_count INTEGER DEFAULT 0")
                )
            if "manager_owner_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN manager_owner_id VARCHAR(255) DEFAULT ''")
                )
            if "is_unread" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN is_unread INTEGER DEFAULT 1")
                )
            if "assigned_manager_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN assigned_manager_id VARCHAR(255)")
                )

        if "conversations" in table_names:
            conversation_columns = {col["name"] for col in inspector.get_columns("conversations")}
            if "workspace_id" not in conversation_columns:
                conn.execute(text("ALTER TABLE conversations ADD COLUMN workspace_id INTEGER DEFAULT 1"))
            if "folder_id" not in conversation_columns:
                conn.execute(text("ALTER TABLE conversations ADD COLUMN folder_id INTEGER"))

        if "outbox_messages" in table_names:
            outbox_columns = {col["name"] for col in inspector.get_columns("outbox_messages")}
            if "workspace_id" not in outbox_columns:
                conn.execute(
                    text("ALTER TABLE outbox_messages ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )
            if "is_permanent_failure" not in outbox_columns:
                conn.execute(
                    text("ALTER TABLE outbox_messages ADD COLUMN is_permanent_failure INTEGER DEFAULT 0")
                )
            if "target_user_id" not in outbox_columns:
                conn.execute(
                    text("ALTER TABLE outbox_messages ADD COLUMN target_user_id VARCHAR(255) DEFAULT ''")
                )

        if "chat_folders" in table_names:
            folder_columns = {col["name"] for col in inspector.get_columns("chat_folders")}
            if "workspace_id" not in folder_columns:
                conn.execute(text("ALTER TABLE chat_folders ADD COLUMN workspace_id INTEGER DEFAULT 1"))
            if "sort_order" not in folder_columns:
                conn.execute(text("ALTER TABLE chat_folders ADD COLUMN sort_order INTEGER DEFAULT 0"))

        if "bot_settings" in table_names:
            settings_columns = {col["name"] for col in inspector.get_columns("bot_settings")}
            if "workspace_id" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN workspace_id INTEGER DEFAULT 1"))

        if "quick_replies" in table_names:
            quick_columns = {col["name"] for col in inspector.get_columns("quick_replies")}
            if "workspace_id" not in quick_columns:
                conn.execute(text("ALTER TABLE quick_replies ADD COLUMN workspace_id INTEGER DEFAULT 1"))

        if "message_templates" in table_names:
            template_columns = {col["name"] for col in inspector.get_columns("message_templates")}
            if "workspace_id" not in template_columns:
                conn.execute(
                    text("ALTER TABLE message_templates ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )

        if "message_logs" in table_names:
            log_columns = {col["name"] for col in inspector.get_columns("message_logs")}
            if "workspace_id" not in log_columns:
                conn.execute(text("ALTER TABLE message_logs ADD COLUMN workspace_id INTEGER DEFAULT 1"))

        if "customer_profiles" in table_names:
            profile_columns = {col["name"] for col in inspector.get_columns("customer_profiles")}
            if "workspace_id" not in profile_columns:
                conn.execute(
                    text("ALTER TABLE customer_profiles ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )

        if "manager_dispatches" in table_names:
            dispatch_columns = {col["name"] for col in inspector.get_columns("manager_dispatches")}
            if "workspace_id" not in dispatch_columns:
                conn.execute(
                    text("ALTER TABLE manager_dispatches ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )

        if "webhook_events" in table_names:
            webhook_columns = {col["name"] for col in inspector.get_columns("webhook_events")}
            if "event_uid" not in webhook_columns and "event_key" in webhook_columns:
                conn.execute(text("ALTER TABLE webhook_events RENAME COLUMN event_key TO event_uid"))

        # Workspace-scoped uniqueness for fresh deployments.
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_quick_replies_workspace_command "
                "ON quick_replies (workspace_id, command)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_message_templates_workspace_key "
                "ON message_templates (workspace_id, template_key)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_chat_folders_workspace_name "
                "ON chat_folders (workspace_id, name)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_customer_profiles_workspace_customer "
                "ON customer_profiles (workspace_id, customer_account_id)"
            )
        )
