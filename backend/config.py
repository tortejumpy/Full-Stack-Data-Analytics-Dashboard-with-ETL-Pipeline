"""
config.py — Centralised settings via Pydantic BaseSettings.

All values are read from environment variables with sensible defaults,
making the service environment-agnostic (local, Docker, CI).
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables.

    Attributes:
        processed_dir: Absolute or relative path to the processed CSV directory.
        cors_origins: Comma-separated list of allowed CORS origins.
        cache_ttl_seconds: How long to cache loaded DataFrames in memory.
        api_prefix: URL prefix for all API routes.
        app_title: Displayed in the OpenAPI docs.
        app_version: Semantic version string.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Data paths — override via PROCESSED_DIR env var
    processed_dir: Path = Field(
        default=Path("data/processed"),
        description="Directory containing cleaned CSV files.",
    )

    # CORS — comma-separated string in env, converted to list by validator
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080", "null", "*"],
        description="Allowed CORS origins.",
    )

    # Cache
    cache_ttl_seconds: int = Field(
        default=300,
        ge=0,
        description="Seconds to cache DataFrames before reloading from disk.",
    )

    # API meta
    api_prefix: str = "/api"
    app_title: str = "Data Analytics Platform"
    app_version: str = "1.0.0"
    debug: bool = False


# Module-level singleton
settings = Settings()
