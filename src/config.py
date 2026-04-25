from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres_url: str = Field(
        default="postgresql://platform_user:platform_pass@localhost:5432/feature_platform",
        alias="POSTGRES_URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    kafka_bootstrap_servers: str = Field(default="localhost:19092", alias="KAFKA_BOOTSTRAP_SERVERS")
    raw_events_topic: str = Field(default="raw-events", alias="RAW_EVENTS_TOPIC")
    online_feature_prefix: str = Field(default="features:", alias="ONLINE_FEATURE_PREFIX")
    supported_schema_versions: str = Field(default="1.0.0", alias="SUPPORTED_SCHEMA_VERSIONS")
    freshness_warning_lag_seconds: int = Field(default=300, alias="FRESHNESS_WARNING_LAG_SECONDS")
    freshness_error_lag_seconds: int = Field(default=1800, alias="FRESHNESS_ERROR_LAG_SECONDS")


settings = Settings()
