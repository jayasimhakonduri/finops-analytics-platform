"""FinOps Platform Configuration"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # AWS
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_DEFAULT_REGION: str = "us-east-1"

    # Azure
    AZURE_SUBSCRIPTION_ID: str
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str

    # Database (PostgreSQL)
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/finops"

    # Redis (caching)
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECONDS: int = 3600

    # API
    API_KEY: str
    ALLOWED_ORIGINS: List[str] = ["*"]

    # ETL
    ETL_BATCH_SIZE: int = 10000
    ETL_SCHEDULE_CRON: str = "0 2 * * *"   # nightly at 2AM

    class Config:
        env_file = ".env"


settings = Settings()
