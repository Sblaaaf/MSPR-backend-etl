import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.routes import router
from etl_pipeline import run_pipeline

logger = logging.getLogger("healthai_etl")

# Heure de déclenchement automatique (configurable via env)
ETL_CRON_HOUR   = int(os.getenv("ETL_CRON_HOUR",   "3"))
ETL_CRON_MINUTE = int(os.getenv("ETL_CRON_MINUTE", "0"))
ETL_SCHEDULER_ENABLED = os.getenv("ETL_SCHEDULER_ENABLED", "true").lower() == "true"

scheduler = AsyncIOScheduler()


async def _scheduled_run():
    logger.info("Déclenchement automatique du pipeline ETL (scheduler).")
    run_pipeline(declencheur="scheduler")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ETL_SCHEDULER_ENABLED:
        scheduler.add_job(
            _scheduled_run,
            trigger=CronTrigger(hour=ETL_CRON_HOUR, minute=ETL_CRON_MINUTE),
            id="etl_daily",
            name="ETL quotidien HealthAI",
            replace_existing=True,
        )
        scheduler.start()
        logger.info(
            f"Scheduler ETL démarré — exécution quotidienne à {ETL_CRON_HOUR:02d}:{ETL_CRON_MINUTE:02d}"
        )
    yield
    if ETL_SCHEDULER_ENABLED:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="HealthAI ETL Service",
    description=(
        "Service d'ingestion et de transformation des données HealthAI Coach. "
        "Déclenche et monitore le pipeline ETL, expose les rapports de qualité."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
