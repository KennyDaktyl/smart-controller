from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class CommonSettings(BaseSettings):
    LOG_DIR: str = Field("logs", env="LOG_DIR")

    NATS_URL: str = Field("nats://localhost:4222", env="NATS_URL")

    REDIS_HOST: str = Field("redis", env="REDIS_HOST")
    REDIS_PORT: int = Field(6379, env="REDIS_PORT")

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        env_file_encoding="utf-8",
        validate_default=True
    )

settings = CommonSettings()
