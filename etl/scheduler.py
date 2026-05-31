"""Nightly ETL job scheduler using APScheduler."""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
import subprocess, os

scheduler = AsyncIOScheduler()

async def run_nightly_etl():
    logger.info("Triggering nightly FinOps ETL...")
    aws_path   = os.getenv("AWS_CUR_S3_PATH", "s3://finops-bucket/cur/latest/")
    azure_path = os.getenv("AZURE_BILLING_BLOB_PATH", "wasbs://billing@account.blob.core.windows.net/")
    subprocess.Popen([
        "spark-submit", "--master", "local[*]",
        "etl/spark_pipeline.py", aws_path, azure_path
    ])

async def start_scheduler():
    scheduler.add_job(run_nightly_etl, "cron", hour=2, minute=0)
    scheduler.start()
    logger.info("ETL scheduler started (nightly 02:00)")

async def stop_scheduler():
    scheduler.shutdown()
