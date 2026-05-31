"""Pydantic models for FinOps Billing API"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from enum import Enum


class CloudProvider(str, Enum):
    AWS = "aws"
    AZURE = "azure"
    ALL = "all"


class Granularity(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


# ── Request models ─────────────────────────────────────────────

class BillingQueryRequest(BaseModel):
    provider: CloudProvider = CloudProvider.ALL
    start_date: date
    end_date: date
    granularity: Granularity = Granularity.DAILY
    resource_types: Optional[List[str]] = None   # e.g. ["EC2", "S3", "RDS"]
    tags: Optional[dict] = None

    class Config:
        json_schema_extra = {
            "example": {
                "provider": "aws",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "granularity": "daily",
                "resource_types": ["EC2", "S3", "RDS", "Lambda"],
            }
        }


class CostForecastRequest(BaseModel):
    provider: CloudProvider = CloudProvider.ALL
    months_ahead: int = Field(default=3, ge=1, le=12)


class ResourceTagRequest(BaseModel):
    resource_id: str
    tags: dict


# ── Response models ────────────────────────────────────────────

class CostDataPoint(BaseModel):
    date: str
    provider: str
    resource_type: str
    service: str
    region: str
    cost_usd: float
    usage_quantity: float
    usage_unit: str
    account_id: str
    tags: dict = {}


class BillingSummary(BaseModel):
    total_cost_usd: float
    provider_breakdown: dict        # {"aws": 120000.0, "azure": 45000.0}
    resource_type_breakdown: dict   # {"EC2": 60000.0, "S3": 12000.0, ...}
    top_services: List[dict]
    cost_trend_pct: float           # % change vs previous period
    data_points: int


class BillingResponse(BaseModel):
    summary: BillingSummary
    records: List[CostDataPoint]
    query_time_ms: float


class ForecastResponse(BaseModel):
    provider: str
    forecast_months: List[dict]     # [{"month": "2025-02", "predicted_cost": 165000.0}]
    confidence_pct: float


class ResourceInventory(BaseModel):
    provider: str
    resource_id: str
    resource_type: str              # EC2, S3, RDS, Lambda, EKS, Azure VM, Blob, etc.
    service: str
    region: str
    status: str
    monthly_cost_usd: float
    tags: dict = {}
    metadata: dict = {}
