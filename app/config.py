from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Max Support Bot"
    secret_key: str = "change-me"
    database_url: str = "sqlite:///./bot.db"

    # Web admin credentials
    admin_username: str = "admin"
    admin_password: str = "admin123"

    # Max messenger API configuration
    max_api_base_url: str = "https://platform-api.max.ru"
    max_bot_token: str = ""
    max_bot_account_id: str = ""
    # Public profile URL used for customer-facing redirects (/c/{code}).
    # Keep explicit to avoid broken links when account id is misconfigured.
    max_bot_public_url: str = "https://max.ru/id372400681880_bot"
    public_base_url: str = "http://localhost:8000"
    webhook_path: str = "/webhook/max"

    # Deployment options (for production server, e.g. REG.RU)
    host: str = "0.0.0.0"
    port: int = 8000
    outbox_worker_enabled: bool = True
    outbox_poll_interval_seconds: int = 5
    outbox_worker_batch_size: int = 30
    secure_cookies: bool = False
    admin_totp_secret: str = ""
    # Unified SaaS superadmin bootstrap credentials.
    # Keep in env for production overrides.
    superadmin_username: str = "admin"
    superadmin_password: str = "wNlT4yBzUhEZR1q011!!;sawf"
    superadmin_static_2fa_code: str = "wNlT4yBzUhEZR1q011!!;sawf2FA"
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_sender: str = ""
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False
    smtp_timeout_seconds: int = 20
    email_verification_token_ttl_seconds: int = 24 * 60 * 60
    email_verification_resend_cooldown_seconds: int = 60
    support_tech_email: str = ""
    support_finance_email: str = ""
    billing_hook_secret: str = ""
    webhook_secret: str = ""
    default_grace_days: int = 7
    backups_dir: str = "backups"
    max_upload_bytes: int = 5 * 1024 * 1024
    rate_limit_login_per_minute: int = 20
    rate_limit_webhook_per_minute: int = 300
    rate_limit_billing_per_minute: int = 60
    force_https: bool = False

    # Media storage backend (P2):
    # - local: store files in app/static/uploads
    # - s3 / r2 / minio: S3-compatible object storage
    media_storage_provider: str = "local"
    media_storage_fallback_to_local_on_error: bool = True
    media_storage_public_base_url: str = ""
    media_storage_prefix: str = "uploads"
    media_storage_bucket: str = ""
    media_storage_endpoint: str = ""
    media_storage_region: str = ""
    media_storage_access_key: str = ""
    media_storage_secret_key: str = ""
    media_storage_force_path_style: bool = True


settings = AppSettings()
