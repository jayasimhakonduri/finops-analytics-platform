"""
FinOps Platform API Routes
Endpoints: billing costs, resource inventory, cost forecast, anomaly detection
"""
import time
from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from api.models import (
    BillingQueryRequest, BillingResponse, ForecastResponse,
    ResourceInventory, CostForecastRequest, CloudProvider,
)
from api.billing_api import AWSBillingAPI, AzureBillingAPI, BillingAggregator
from api.auth import verify_api_key

router = APIRouter()


def get_aggregator() -> BillingAggregator:
    return BillingAggregator(
        aws=AWSBillingAPI(),
        azure=AzureBillingAPI(),
    )


# ── Billing costs ──────────────────────────────────────────────

@router.post("/billing/costs", response_model=BillingResponse)
async def get_billing_costs(
    request: BillingQueryRequest,
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    Fetch and normalize billing costs from AWS Cost Explorer and/or Azure Cost Management.
    Supports filtering by resource type (EC2, S3, RDS, Lambda, EKS, Azure VM, Blob, etc.)
    and custom tags. Returns daily or monthly granularity.
    """
    t0 = time.perf_counter()
    try:
        result = await aggregator.get_costs(request)
        elapsed = (time.perf_counter() - t0) * 1000
        result.query_time_ms = round(elapsed, 2)
        return result
    except Exception as e:
        logger.error(f"Billing costs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/billing/summary")
async def get_billing_summary(
    provider: CloudProvider = CloudProvider.ALL,
    year: int = Query(..., ge=2020, le=2030),
    month: int = Query(..., ge=1, le=12),
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    Monthly billing summary: total cost, provider/resource breakdown,
    top 10 services, month-over-month trend.
    """
    try:
        return await aggregator.get_monthly_summary(provider, year, month)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Resource inventory ─────────────────────────────────────────

@router.get("/resources", response_model=list[ResourceInventory])
async def get_resource_inventory(
    provider: CloudProvider = CloudProvider.ALL,
    resource_type: str | None = None,
    region: str | None = None,
    min_monthly_cost: float = 0.0,
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    List all cloud resources with their monthly costs.
    Covers: EC2, S3, RDS, Lambda, EKS, CloudFront (AWS)
    and VMs, Blob Storage, SQL DB, AKS, Functions (Azure).
    Filter by resource type, region, or minimum cost.
    """
    try:
        return await aggregator.get_resource_inventory(
            provider=provider,
            resource_type=resource_type,
            region=region,
            min_monthly_cost=min_monthly_cost,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resources/{resource_id}")
async def get_resource_detail(
    resource_id: str,
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """Get detailed cost history and metadata for a specific resource."""
    try:
        return await aggregator.get_resource_detail(resource_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Cost forecast ──────────────────────────────────────────────

@router.post("/billing/forecast", response_model=ForecastResponse)
async def forecast_costs(
    request: CostForecastRequest,
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    Predict future cloud costs using AWS Cost Explorer forecasting
    and Azure Consumption budget projections.
    """
    try:
        return await aggregator.forecast(request.provider, request.months_ahead)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Anomaly detection ──────────────────────────────────────────

@router.get("/billing/anomalies")
async def get_cost_anomalies(
    provider: CloudProvider = CloudProvider.ALL,
    threshold_pct: float = Query(default=20.0, ge=5.0, le=100.0),
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    Detect resources/services with cost spikes exceeding threshold_pct
    compared to 7-day rolling average. Uses AWS Cost Anomaly Detection
    and custom Azure spike detection logic.
    """
    try:
        return await aggregator.detect_anomalies(provider, threshold_pct)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Cost by tag ────────────────────────────────────────────────

@router.get("/billing/by-tag")
async def get_cost_by_tag(
    tag_key: str,
    provider: CloudProvider = CloudProvider.ALL,
    year: int = Query(...),
    month: int = Query(..., ge=1, le=12),
    api_key: str = Depends(verify_api_key),
    aggregator: BillingAggregator = Depends(get_aggregator),
):
    """
    Group and aggregate costs by a specific resource tag (e.g. 'team', 'project', 'environment').
    Useful for chargeback and showback reporting.
    """
    try:
        return await aggregator.get_cost_by_tag(tag_key, provider, year, month)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
