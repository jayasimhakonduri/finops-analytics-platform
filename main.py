"""
FinOps Analytics Platform — FastAPI Entry Point
Unified cloud billing API: AWS Cost Explorer + Azure Cost Management
Author: Jaya Simha Konduri
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from loguru import logger

from api.routes import router
from api.config import settings
from etl.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinOps Analytics Platform...")
    await start_scheduler()
    yield
    await stop_scheduler()
    logger.info("FinOps platform shut down.")


app = FastAPI(
    title="FinOps Analytics Platform API",
    description=(
        "Unified multi-cloud billing API integrating AWS Cost Explorer, "
        "AWS Billing, and Azure Cost Management. Normalizes 100M+ billing records/day "
        "across EC2, S3, RDS, Lambda, EKS, Azure VMs, Blob Storage, and 30+ resource types."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "finops-platform", "version": "1.0.0"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
