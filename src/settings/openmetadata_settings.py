from pydantic_settings import BaseSettings, SettingsConfigDict


class OpenMetadataSettings(BaseSettings):
    api_base_url: str
    jwt_token: str
    default_domain_type: str = "Aggregate"
    default_storage_service_name: str = "generic"
    default_storage_service_type: str = "CustomStorage"

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="openmetadata_", extra="ignore"
    )
