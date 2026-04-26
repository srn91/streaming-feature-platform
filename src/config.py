from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    hosted_demo: bool = Field(default=False, alias="HOSTED_DEMO")
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
    demo_seed_users: int = Field(default=12, alias="DEMO_SEED_USERS")
    demo_events_per_user: int = Field(default=16, alias="DEMO_EVENTS_PER_USER")
    demo_sample_events_path: str = Field(
        default="data/generated/sample_events.jsonl",
        alias="DEMO_SAMPLE_EVENTS_PATH",
    )
    gcp_project_id: str = Field(default="streaming-feature-platform", alias="GCP_PROJECT_ID")
    gcp_pubsub_topic: str = Field(default="feature-events", alias="GCP_PUBSUB_TOPIC")
    gcp_bigquery_dataset: str = Field(default="feature_platform", alias="GCP_BIGQUERY_DATASET")
    gcp_bigquery_raw_events_table: str = Field(default="raw_events", alias="GCP_BIGQUERY_RAW_EVENTS_TABLE")
    gcp_bigquery_feature_snapshots_table: str = Field(
        default="feature_snapshots",
        alias="GCP_BIGQUERY_FEATURE_SNAPSHOTS_TABLE",
    )
    gcp_asset_output_dir: str = Field(default="data/generated/gcp", alias="GCP_ASSET_OUTPUT_DIR")


settings = Settings()
