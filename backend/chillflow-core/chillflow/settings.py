"""Global settings for ChillFlow."""

import os

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Allow extra fields from .env without validation errors
    )

    # Database
    DATABASE_URL: str = "postgresql+psycopg://dev:dev@localhost:5432/chillflow"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:19092"

    # Security / Hashing
    HASH_SALT: str = os.getenv("HASH_SALT", "chillflow-dev-salt-2025")


settings = Settings()
