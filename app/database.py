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
            workspace_columns = {col["name"] for col in inspector.get_columns("workspaces")}
            if "suspended_by_admin" not in workspace_columns:
                conn.execute(
                    text("ALTER TABLE workspaces ADD COLUMN suspended_by_admin INTEGER DEFAULT 0")
                )
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
            if "is_read_by_customer" not in chat_columns:
                conn.execute(
                    text("ALTER TABLE chat_messages ADD COLUMN is_read_by_customer INTEGER DEFAULT 0")
                )
            if "read_at" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN read_at DATETIME"))

        if "conversation_meta" in table_names:
            meta_columns = {col["name"] for col in inspector.get_columns("conversation_meta")}
            if "workspace_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN workspace_id INTEGER DEFAULT 1")
                )
            if "intro_sent" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN intro_sent INTEGER DEFAULT 0")
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
            if "manual_unread_mark" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN manual_unread_mark INTEGER DEFAULT 0")
                )
            if "assigned_manager_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN assigned_manager_id VARCHAR(255)")
                )
            if "is_blocked" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN is_blocked INTEGER DEFAULT 0")
                )
            if "blocked_at" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_at DATETIME")
                )
            if "blocked_by_user_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_by_user_id INTEGER")
                )
            if "blocked_prev_folder_id" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_prev_folder_id INTEGER")
                )
            if "blocked_reason" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_reason TEXT DEFAULT ''")
                )
            if "blocked_notice_sent_at" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_notice_sent_at DATETIME")
                )
            if "blocked_notice_last_sent_at" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN blocked_notice_last_sent_at DATETIME")
                )
            if "offhours_notice_sent_at" not in meta_columns:
                conn.execute(
                    text("ALTER TABLE conversation_meta ADD COLUMN offhours_notice_sent_at DATETIME")
                )

        if "workspace_business_hours" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE workspace_business_hours ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER DEFAULT 1, "
                    "enabled INTEGER DEFAULT 0, "
                    "timezone VARCHAR(64) DEFAULT 'UTC', "
                    "offhours_message TEXT DEFAULT 'Сейчас мы вне рабочего времени. Мы ответим в рабочие часы.', "
                    "cooldown_seconds INTEGER DEFAULT 21600, "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
                    "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                )
            )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_workspace_business_hours_workspace_id "
                "ON workspace_business_hours (workspace_id)"
            )
        )
        conn.execute(
            text(
                "UPDATE workspace_business_hours "
                "SET timezone = 'UTC' "
                "WHERE timezone IS NULL OR TRIM(timezone) = ''"
            )
        )

        if "workspace_business_slots" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE workspace_business_slots ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER DEFAULT 1, "
                    "weekday INTEGER DEFAULT 0, "
                    "start_minute INTEGER DEFAULT 540, "
                    "end_minute INTEGER DEFAULT 1080, "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                )
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_slots_workspace_id "
                "ON workspace_business_slots (workspace_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_slots_weekday "
                "ON workspace_business_slots (weekday)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_slots_workspace_weekday_start "
                "ON workspace_business_slots (workspace_id, weekday, start_minute)"
            )
        )

        if "workspace_business_exceptions" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE workspace_business_exceptions ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER DEFAULT 1, "
                    "date_from DATE NOT NULL, "
                    "date_to DATE NOT NULL, "
                    "mode VARCHAR(24) DEFAULT 'closed_all_day', "
                    "start_minute INTEGER, "
                    "end_minute INTEGER, "
                    "note VARCHAR(255) DEFAULT '', "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                )
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_exceptions_workspace_id "
                "ON workspace_business_exceptions (workspace_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_exceptions_date_from "
                "ON workspace_business_exceptions (date_from)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_exceptions_date_to "
                "ON workspace_business_exceptions (date_to)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workspace_business_exceptions_workspace_date "
                "ON workspace_business_exceptions (workspace_id, date_from, date_to)"
            )
        )

        if "conversations" in table_names:
            conversation_columns = {col["name"] for col in inspector.get_columns("conversations")}
            if "workspace_id" not in conversation_columns:
                conn.execute(text("ALTER TABLE conversations ADD COLUMN workspace_id INTEGER DEFAULT 1"))
            if "folder_id" not in conversation_columns:
                conn.execute(text("ALTER TABLE conversations ADD COLUMN folder_id INTEGER"))
            if "created_at" not in conversation_columns:
                # SQLite does not allow non-constant defaults in ALTER TABLE ADD COLUMN.
                conn.execute(text("ALTER TABLE conversations ADD COLUMN created_at DATETIME"))
                conn.execute(
                    text(
                        "UPDATE conversations SET created_at = CURRENT_TIMESTAMP "
                        "WHERE created_at IS NULL"
                    )
                )

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

        if "conversation_folder_links" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE conversation_folder_links ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER DEFAULT 1, "
                    "conversation_id INTEGER NOT NULL, "
                    "folder_id INTEGER NOT NULL, "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                )
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_conversation_folder_links_workspace_id "
                "ON conversation_folder_links (workspace_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_conversation_folder_links_conversation_id "
                "ON conversation_folder_links (conversation_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_conversation_folder_links_folder_id "
                "ON conversation_folder_links (folder_id)"
            )
        )
        if "conversations" in table_names and "chat_folders" in table_names:
            # Backfill legacy one-folder relation into many-to-many links.
            conn.execute(
                text(
                    "INSERT OR IGNORE INTO conversation_folder_links "
                    "(workspace_id, conversation_id, folder_id, created_at) "
                    "SELECT c.workspace_id, c.id, c.folder_id, CURRENT_TIMESTAMP "
                    "FROM conversations c "
                    "JOIN chat_folders f ON f.id = c.folder_id AND f.workspace_id = c.workspace_id "
                    "WHERE c.folder_id IS NOT NULL AND c.folder_id > 0"
                )
            )

        if "bot_settings" in table_names:
            settings_columns = {col["name"] for col in inspector.get_columns("bot_settings")}
            if "workspace_id" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN workspace_id INTEGER DEFAULT 1"))
            if "bot_token" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN bot_token TEXT DEFAULT ''"))
            if "bot_link" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN bot_link VARCHAR(255) DEFAULT ''"))
            if "webhook_key" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN webhook_key VARCHAR(96) DEFAULT ''"))
            # Backfill missing keys and normalize duplicates to keep unique index creation safe.
            rows = conn.execute(text("SELECT id, webhook_key FROM bot_settings ORDER BY id ASC")).fetchall()
            seen_webhook_keys: set[str] = set()
            for row in rows:
                row_id = int(row[0])
                raw_key = str(row[1] or "").strip()
                key_value = raw_key
                if not key_value:
                    key_value = f"wk_{row_id:08d}"
                while key_value in seen_webhook_keys:
                    key_value = f"{key_value}_{row_id}"
                seen_webhook_keys.add(key_value)
                if key_value != raw_key:
                    conn.execute(
                        text("UPDATE bot_settings SET webhook_key = :key WHERE id = :row_id"),
                        {"key": key_value, "row_id": row_id},
                    )
            if "routing_mode" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN routing_mode VARCHAR(20) DEFAULT 'round_robin'"))
            if "routing_rr_cursor" not in settings_columns:
                conn.execute(text("ALTER TABLE bot_settings ADD COLUMN routing_rr_cursor INTEGER DEFAULT 0"))
            if "request_customer_phone" not in settings_columns:
                conn.execute(
                    text("ALTER TABLE bot_settings ADD COLUMN request_customer_phone INTEGER DEFAULT 1")
                )

        if "quick_replies" in table_names:
            quick_columns = {col["name"] for col in inspector.get_columns("quick_replies")}
            if "workspace_id" not in quick_columns:
                conn.execute(text("ALTER TABLE quick_replies ADD COLUMN workspace_id INTEGER DEFAULT 1"))
            if "image_path" not in quick_columns:
                conn.execute(text("ALTER TABLE quick_replies ADD COLUMN image_path VARCHAR(500)"))

        if "quick_reply_media" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE quick_reply_media ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER DEFAULT 1, "
                    "quick_reply_id INTEGER NOT NULL, "
                    "media_path VARCHAR(1000) NOT NULL, "
                    "sort_order INTEGER DEFAULT 0, "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_quick_reply_media_workspace_id "
                    "ON quick_reply_media (workspace_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_quick_reply_media_quick_reply_id "
                    "ON quick_reply_media (quick_reply_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_quick_reply_media_sort_order "
                    "ON quick_reply_media (sort_order)"
                )
            )

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

        if "service_users" in table_names:
            service_user_columns = {col["name"] for col in inspector.get_columns("service_users")}
            if "totp_secret" not in service_user_columns:
                conn.execute(text("ALTER TABLE service_users ADD COLUMN totp_secret VARCHAR(255) DEFAULT ''"))
            if "totp_enabled" not in service_user_columns:
                conn.execute(text("ALTER TABLE service_users ADD COLUMN totp_enabled INTEGER DEFAULT 0"))
            if "email_verified" not in service_user_columns:
                conn.execute(text("ALTER TABLE service_users ADD COLUMN email_verified INTEGER DEFAULT 0"))
            if "email_verified_at" not in service_user_columns:
                conn.execute(text("ALTER TABLE service_users ADD COLUMN email_verified_at DATETIME"))
            if "email_verification_sent_at" not in service_user_columns:
                conn.execute(text("ALTER TABLE service_users ADD COLUMN email_verification_sent_at DATETIME"))
            # Role model migration:
            # - keep only one owner (superadmin with no workspace)
            # - all workspace-bound owners become admins
            conn.execute(
                text(
                    "UPDATE service_users "
                    "SET role = 'admin' "
                    "WHERE role = 'owner' AND workspace_id IS NOT NULL"
                )
            )

        if "subscriptions" in table_names:
            sub_cols = {col["name"] for col in inspector.get_columns("subscriptions")}
            if "quick_replies_limit" not in sub_cols:
                conn.execute(
                    text("ALTER TABLE subscriptions ADD COLUMN quick_replies_limit INTEGER DEFAULT 10")
                )
            if "folders_limit" not in sub_cols:
                conn.execute(
                    text("ALTER TABLE subscriptions ADD COLUMN folders_limit INTEGER DEFAULT 10")
                )
        if "quick_replies" in table_names:
            qr_cols = {col["name"] for col in inspector.get_columns("quick_replies")}
            if "owner_user_id" not in qr_cols:
                conn.execute(
                    text("ALTER TABLE quick_replies ADD COLUMN owner_user_id INTEGER DEFAULT 0")
                )
            conn.execute(text("UPDATE quick_replies SET owner_user_id = 0 WHERE owner_user_id IS NULL"))

        if "platform_settings" in table_names:
            platform_cols = {col["name"] for col in inspector.get_columns("platform_settings")}
            if "settings_key" in platform_cols and "settings_value" in platform_cols:
                # Legacy key/value schema is incompatible with the new dedicated columns.
                conn.execute(text("DROP TABLE platform_settings"))
                conn.execute(
                    text(
                        "CREATE TABLE platform_settings ("
                        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "technical_support_email VARCHAR(255) DEFAULT '', "
                        "billing_support_email VARCHAR(255) DEFAULT '', "
                        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
                        "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
                    )
                )
                platform_cols = {col["name"] for col in inspector.get_columns("platform_settings")}
            if "technical_support_email" not in platform_cols:
                conn.execute(
                    text("ALTER TABLE platform_settings ADD COLUMN technical_support_email VARCHAR(255) DEFAULT ''")
                )
            if "billing_support_email" not in platform_cols:
                conn.execute(
                    text("ALTER TABLE platform_settings ADD COLUMN billing_support_email VARCHAR(255) DEFAULT ''")
                )

        if "intro_steps" in table_names:
            intro_columns = {col["name"] for col in inspector.get_columns("intro_steps")}
            if "created_at" not in intro_columns:
                conn.execute(
                    text("ALTER TABLE intro_steps ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                )

        if "tenant_alerts" not in table_names:
            conn.execute(
                text(
                    "CREATE TABLE tenant_alerts ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "workspace_id INTEGER NOT NULL, "
                    "alert_key VARCHAR(120), "
                    "severity VARCHAR(20) DEFAULT 'warning', "
                    "message TEXT DEFAULT '', "
                    "metric_value FLOAT DEFAULT 0, "
                    "is_resolved INTEGER DEFAULT 0, "
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
                    "resolved_at DATETIME)"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tenant_alerts_workspace_id ON tenant_alerts (workspace_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tenant_alerts_alert_key ON tenant_alerts (alert_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tenant_alerts_severity ON tenant_alerts (severity)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tenant_alerts_is_resolved ON tenant_alerts (is_resolved)"))

        # Drop legacy global unique indexes left from single-tenant schema.
        # They conflict with new workspace-scoped unique constraints.
        conn.execute(text("DROP INDEX IF EXISTS ix_quick_replies_command"))
        conn.execute(text("DROP INDEX IF EXISTS ix_quick_replies_workspace_command"))
        conn.execute(text("DROP INDEX IF EXISTS ix_message_templates_template_key"))
        conn.execute(text("DROP INDEX IF EXISTS ix_chat_folders_name"))
        conn.execute(text("DROP INDEX IF EXISTS ix_customer_profiles_customer_account_id"))

        # Workspace-scoped uniqueness for fresh deployments.
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_quick_replies_workspace_owner_command "
                "ON quick_replies (workspace_id, owner_user_id, command)"
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
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_conversation_folder_links_workspace_conversation_folder "
                "ON conversation_folder_links (workspace_id, conversation_id, folder_id)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_customer_profiles_workspace_customer "
                "ON customer_profiles (workspace_id, customer_account_id)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_bot_settings_workspace_webhook_key "
                "ON bot_settings (workspace_id, webhook_key)"
            )
        )
