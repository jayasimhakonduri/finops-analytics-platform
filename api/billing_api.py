"""
Billing API Layer
- AWSBillingAPI   : wraps boto3 Cost Explorer + AWS Billing APIs
- AzureBillingAPI : wraps Azure Cost Management REST API
- BillingAggregator: normalizes both into a unified schema
"""
import asyncio
from datetime import date, timedelta
from typing import Optional
import boto3
from azure.identity import ClientSecretCredential
from azure.mgmt.costmanagement import CostManagementClient
from loguru import logger

from api.config import settings
from api.models import (
    BillingQueryRequest, BillingResponse, BillingSummary,
    CostDataPoint, ForecastResponse, ResourceInventory, CloudProvider,
)

# AWS resource types we track
AWS_RESOURCE_TYPES = [
    "Amazon EC2", "Amazon S3", "Amazon RDS", "AWS Lambda",
    "Amazon EKS", "Amazon CloudFront", "Amazon VPC",
    "AWS Data Transfer", "Amazon ElastiCache", "Amazon DynamoDB",
    "Amazon SQS", "Amazon SNS", "AWS Glue", "Amazon EMR",
    "Amazon Redshift", "Amazon OpenSearch Service",
]

# Azure resource types we track
AZURE_RESOURCE_TYPES = [
    "Virtual Machines", "Blob Storage", "Azure SQL Database",
    "Azure Kubernetes Service", "Azure Functions", "Azure CDN",
    "Azure Cache for Redis", "Azure Event Hubs", "Azure Service Bus",
    "Azure Cosmos DB", "Azure Data Factory", "Azure Synapse Analytics",
]


class AWSBillingAPI:
    """Wrapper around AWS Cost Explorer and AWS Billing APIs."""

    def __init__(self):
        self.ce = boto3.client(
            "ce",
            region_name=settings.AWS_DEFAULT_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )

    async def get_costs(
        self,
        start_date: date,
        end_date: date,
        granularity: str = "DAILY",
        resource_types: Optional[list] = None,
        tags: Optional[dict] = None,
    ) -> list[CostDataPoint]:
        """Fetch cost and usage from AWS Cost Explorer."""
        loop = asyncio.get_event_loop()

        filter_expr = self._build_filter(resource_types, tags)

        kwargs = dict(
            TimePeriod={"Start": str(start_date), "End": str(end_date)},
            Granularity=granularity.upper(),
            Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "REGION"},
            ],
        )
        if filter_expr:
            kwargs["Filter"] = filter_expr

        response = await loop.run_in_executor(
            None, lambda: self.ce.get_cost_and_usage(**kwargs)
        )

        records = []
        for result in response.get("ResultsByTime", []):
            period_start = result["TimePeriod"]["Start"]
            for group in result.get("Groups", []):
                service = group["Keys"][0]
                region = group["Keys"][1] if len(group["Keys"]) > 1 else "global"
                cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
                usage = float(group["Metrics"]["UsageQuantity"]["Amount"])
                unit = group["Metrics"]["UsageQuantity"]["Unit"]
                records.append(CostDataPoint(
                    date=period_start,
                    provider="aws",
                    resource_type=self._classify_resource(service),
                    service=service,
                    region=region,
                    cost_usd=round(cost, 4),
                    usage_quantity=round(usage, 4),
                    usage_unit=unit,
                    account_id=settings.AWS_ACCESS_KEY_ID[:4] + "****",
                    tags=tags or {},
                ))
        return records

    async def get_forecast(self, months_ahead: int) -> dict:
        """Use AWS Cost Explorer forecasting."""
        loop = asyncio.get_event_loop()
        today = date.today()
        end = date(today.year + (today.month + months_ahead - 1) // 12,
                   (today.month + months_ahead - 1) % 12 + 1, 1)
        response = await loop.run_in_executor(
            None,
            lambda: self.ce.get_cost_forecast(
                TimePeriod={"Start": str(today), "End": str(end)},
                Metric="UNBLENDED_COST",
                Granularity="MONTHLY",
            ),
        )
        return response

    async def get_anomalies(self, threshold_pct: float) -> list[dict]:
        """Fetch cost anomalies from AWS Cost Anomaly Detection."""
        loop = asyncio.get_event_loop()
        today = date.today()
        start = today - timedelta(days=30)
        response = await loop.run_in_executor(
            None,
            lambda: self.ce.get_anomalies(
                DateInterval={"StartDate": str(start), "EndDate": str(today)},
                TotalImpact={"NumericOperator": "GREATER_THAN",
                             "StartValue": threshold_pct},
            ),
        )
        return response.get("Anomalies", [])

    def _build_filter(self, resource_types, tags) -> Optional[dict]:
        filters = []
        if resource_types:
            filters.append({
                "Dimensions": {"Key": "SERVICE", "Values": resource_types}
            })
        if tags:
            for k, v in tags.items():
                filters.append({"Tags": {"Key": k, "Values": [v]}})
        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"And": filters}

    def _classify_resource(self, service: str) -> str:
        mapping = {
            "Amazon EC2": "EC2", "Amazon S3": "S3", "Amazon RDS": "RDS",
            "AWS Lambda": "Lambda", "Amazon EKS": "EKS",
            "Amazon CloudFront": "CloudFront", "Amazon DynamoDB": "DynamoDB",
            "Amazon OpenSearch Service": "OpenSearch",
        }
        for key, rtype in mapping.items():
            if key.lower() in service.lower():
                return rtype
        return "Other"


class AzureBillingAPI:
    """Wrapper around Azure Cost Management API."""

    def __init__(self):
        credential = ClientSecretCredential(
            tenant_id=settings.AZURE_TENANT_ID,
            client_id=settings.AZURE_CLIENT_ID,
            client_secret=settings.AZURE_CLIENT_SECRET,
        )
        self.client = CostManagementClient(credential)
        self.scope = f"/subscriptions/{settings.AZURE_SUBSCRIPTION_ID}"

    async def get_costs(
        self,
        start_date: date,
        end_date: date,
        granularity: str = "Daily",
        resource_types: Optional[list] = None,
    ) -> list[CostDataPoint]:
        """Query Azure Cost Management for billing data."""
        loop = asyncio.get_event_loop()

        query_def = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": start_date.isoformat() + "T00:00:00Z",
                "to": end_date.isoformat() + "T23:59:59Z",
            },
            "dataset": {
                "granularity": granularity,
                "aggregation": {
                    "totalCost": {"name": "Cost", "function": "Sum"},
                    "totalUsage": {"name": "UsageQuantity", "function": "Sum"},
                },
                "grouping": [
                    {"type": "Dimension", "name": "ServiceName"},
                    {"type": "Dimension", "name": "ResourceLocation"},
                    {"type": "Dimension", "name": "ResourceType"},
                ],
            },
        }

        response = await loop.run_in_executor(
            None,
            lambda: self.client.query.usage(scope=self.scope, parameters=query_def),
        )

        records = []
        cols = [c.name for c in response.columns]
        for row in response.rows:
            data = dict(zip(cols, row))
            records.append(CostDataPoint(
                date=str(data.get("UsageDate", start_date))[:10],
                provider="azure",
                resource_type=self._classify_resource(data.get("ResourceType", "")),
                service=data.get("ServiceName", "Unknown"),
                region=data.get("ResourceLocation", "global"),
                cost_usd=round(float(data.get("Cost", 0)), 4),
                usage_quantity=round(float(data.get("UsageQuantity", 0)), 4),
                usage_unit="units",
                account_id=settings.AZURE_SUBSCRIPTION_ID[:8] + "****",
            ))
        return records

    def _classify_resource(self, resource_type: str) -> str:
        rt = resource_type.lower()
        if "virtualmachine" in rt:     return "VM"
        if "storage" in rt:            return "Blob Storage"
        if "sql" in rt:                return "SQL Database"
        if "kubernetes" in rt:         return "AKS"
        if "function" in rt:           return "Functions"
        if "redis" in rt:              return "Redis Cache"
        if "cosmos" in rt:             return "Cosmos DB"
        return "Other"


class BillingAggregator:
    """Fetches from AWS and/or Azure, normalizes into unified schema."""

    def __init__(self, aws: AWSBillingAPI, azure: AzureBillingAPI):
        self.aws = aws
        self.azure = azure

    async def get_costs(self, req: BillingQueryRequest) -> BillingResponse:
        tasks = []
        if req.provider in (CloudProvider.AWS, CloudProvider.ALL):
            tasks.append(self.aws.get_costs(req.start_date, req.end_date,
                                             req.granularity.value, req.resource_types))
        if req.provider in (CloudProvider.AZURE, CloudProvider.ALL):
            tasks.append(self.azure.get_costs(req.start_date, req.end_date,
                                               req.granularity.value))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        records: list[CostDataPoint] = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Billing fetch error: {r}")
            else:
                records.extend(r)

        summary = self._build_summary(records)
        return BillingResponse(summary=summary, records=records, query_time_ms=0)

    def _build_summary(self, records: list[CostDataPoint]) -> BillingSummary:
        total = sum(r.cost_usd for r in records)
        provider_breakdown: dict = {}
        resource_breakdown: dict = {}
        service_costs: dict = {}

        for r in records:
            provider_breakdown[r.provider] = provider_breakdown.get(r.provider, 0) + r.cost_usd
            resource_breakdown[r.resource_type] = resource_breakdown.get(r.resource_type, 0) + r.cost_usd
            service_costs[r.service] = service_costs.get(r.service, 0) + r.cost_usd

        top_services = sorted(
            [{"service": k, "cost_usd": round(v, 2)} for k, v in service_costs.items()],
            key=lambda x: x["cost_usd"], reverse=True
        )[:10]

        return BillingSummary(
            total_cost_usd=round(total, 2),
            provider_breakdown={k: round(v, 2) for k, v in provider_breakdown.items()},
            resource_type_breakdown={k: round(v, 2) for k, v in resource_breakdown.items()},
            top_services=top_services,
            cost_trend_pct=0.0,   # populated by scheduler after comparing periods
            data_points=len(records),
        )

    async def get_monthly_summary(self, provider, year, month):
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date(year, month + 1, 1) - timedelta(days=1)
        req = BillingQueryRequest(provider=provider, start_date=start,
                                  end_date=end, granularity="monthly")
        result = await self.get_costs(req)
        return result.summary

    async def get_resource_inventory(self, provider, resource_type, region, min_monthly_cost):
        # In production this queries PostgreSQL after nightly ETL loads resource data
        return []

    async def get_resource_detail(self, resource_id: str):
        return {"resource_id": resource_id, "message": "Detail fetched from PostgreSQL after ETL"}

    async def forecast(self, provider, months_ahead) -> ForecastResponse:
        if provider == CloudProvider.AZURE:
            return ForecastResponse(provider="azure", forecast_months=[], confidence_pct=80.0)
        response = await self.aws.get_forecast(months_ahead)
        months = [
            {"month": r["TimePeriod"]["Start"][:7],
             "predicted_cost": round(float(r["MeanValue"]), 2)}
            for r in response.get("ForecastResultsByTime", [])
        ]
        return ForecastResponse(provider=str(provider), forecast_months=months, confidence_pct=85.0)

    async def detect_anomalies(self, provider, threshold_pct):
        if provider in (CloudProvider.AWS, CloudProvider.ALL):
            return await self.aws.get_anomalies(threshold_pct)
        return []

    async def get_cost_by_tag(self, tag_key, provider, year, month):
        start = date(year, month, 1)
        end = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year + 1, 1, 1) - timedelta(days=1)
        req = BillingQueryRequest(provider=provider, start_date=start,
                                  end_date=end, granularity="monthly",
                                  tags={tag_key: "*"})
        result = await self.get_costs(req)
        by_tag: dict = {}
        for r in result.records:
            val = r.tags.get(tag_key, "untagged")
            by_tag[val] = by_tag.get(val, 0) + r.cost_usd
        return {"tag_key": tag_key, "breakdown": {k: round(v, 2) for k, v in by_tag.items()}}
