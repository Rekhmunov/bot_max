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
    max_api_base_url: str = "https://api.max.ru"
    max_bot_token: str = ""
    max_bot_account_id: str = ""
    public_base_url: str = "http://localhost:8000"
    webhook_path: str = "/webhook/max"

    # Deployment options (for production server, e.g. REG.RU)
    host: str = "0.0.0.0"
    port: int = 8000


settings = AppSettings()
