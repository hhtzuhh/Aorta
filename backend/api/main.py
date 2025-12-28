"""
Aorta Backend API - Main Application

FastAPI application with Server-Sent Events (SSE) for real-time
hospital admission monitoring.
"""

import sys
from pathlib import Path

# Add parent directory to Python path so Aorta package can be imported
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

# Pre-import anyio backends to avoid lazy import race condition
import anyio._backends._asyncio
import anyio._core._eventloop
import anyio._core._synchronization

from .config import settings
from .unified_consumer import UnifiedConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global unified consumer instance
unified_consumer: UnifiedConsumer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""

    global unified_consumer
    logger.info("Starting Aorta Backend API...")

    try:
        # Initialize single unified Kafka consumer for all topics
        logger.info("Initializing unified Kafka consumer...")
        unified_consumer = UnifiedConsumer(settings)
        asyncio.create_task(unified_consumer.start())

        logger.info("✅ Unified Kafka consumer started")
        logger.info(f"📡 Listening to topics: {settings.kafka_topic}, patient-labs, icu-admissions, patient-vitals")
        logger.info(f"🌐 CORS origins: {settings.cors_origins}")

        yield

    finally:
        logger.info("Shutting down Aorta Backend API...")

        if unified_consumer:
            await unified_consumer.stop()

        logger.info("✅ Shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Aorta - Hospital Admission Monitoring API",
    description="Real-time hospital admission monitoring with SSE support",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "Aorta API",
        "version": "0.1.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "admissions": "/api/admissions",
            "labs": "/api/labs",
            "icu_admissions": "/api/icu-admissions",
            "vitals": "/api/vitals",
            "sepsis_alerts": "/api/sepsis-alerts",
            "stream_admissions": "/stream/admissions",
            "stream_labs": "/stream/labs",
            "stream_icu_admissions": "/stream/icu-admissions",
            "stream_vitals": "/stream/vitals",
            "stream_sepsis_alerts": "/stream/sepsis-alerts",
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "consumer_running": unified_consumer.running if unified_consumer else False,
        "admission_sse_clients": len(unified_consumer.admission_sse_queues) if unified_consumer else 0,
        "lab_sse_clients": len(unified_consumer.lab_sse_queues) if unified_consumer else 0,
        "icu_sse_clients": len(unified_consumer.icu_sse_queues) if unified_consumer else 0,
        "vitals_sse_clients": len(unified_consumer.vitals_sse_queues) if unified_consumer else 0,
        "sepsis_sse_clients": len(unified_consumer.sepsis_sse_queues) if unified_consumer else 0,
        "recent_admissions": len(unified_consumer.recent_admissions) if unified_consumer else 0,
        "recent_labs": len(unified_consumer.recent_labs) if unified_consumer else 0,
        "recent_icu_admissions": len(unified_consumer.recent_icu_admissions) if unified_consumer else 0,
        "recent_chartevents": len(unified_consumer.recent_chartevents) if unified_consumer else 0,
        "recent_sepsis_alerts": len(unified_consumer.recent_sepsis_alerts) if unified_consumer else 0,
        "ml_module_stats": unified_consumer.ml_module.get_stats() if (unified_consumer and unified_consumer.ml_module) else {},
    }


@app.get("/api/admissions")
async def get_recent_admissions() -> List[dict]:
    """Get recent admissions"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_admissions()


@app.get("/stream/admissions")
async def stream_admissions(request: Request):
    """SSE endpoint for admission stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_admissions()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to admission stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "admission", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_admissions(queue)

    return EventSourceResponse(event_generator())


@app.get("/api/labs")
async def get_recent_labs() -> List[dict]:
    """Get recent labs"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_labs()


@app.get("/stream/labs")
async def stream_labs(request: Request):
    """SSE endpoint for lab stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_labs()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to lab stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "lab", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("Lab SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_labs(queue)

    return EventSourceResponse(event_generator())


@app.get("/api/icu-admissions")
async def get_recent_icu_admissions() -> List[dict]:
    """Get recent ICU admissions"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_icu_admissions()


@app.get("/stream/icu-admissions")
async def stream_icu_admissions(request: Request):
    """SSE endpoint for ICU admission stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_icu()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to ICU stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "icu", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("ICU SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_icu(queue)

    return EventSourceResponse(event_generator())


@app.get("/api/vitals")
async def get_recent_vitals() -> List[dict]:
    """Get recent vitals/chartevents"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_chartevents()


@app.get("/stream/vitals")
async def stream_vitals(request: Request):
    """SSE endpoint for vitals stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_vitals()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to vitals stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "chartevent", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("Vitals SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_vitals(queue)

    return EventSourceResponse(event_generator())


@app.get("/api/sepsis-alerts")
async def get_recent_sepsis_alerts() -> List[dict]:
    """Get recent sepsis prediction alerts"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_sepsis_alerts()


@app.get("/stream/sepsis-alerts")
async def stream_sepsis_alerts(request: Request):
    """SSE endpoint for sepsis alert stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_sepsis_alerts()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to sepsis alerts stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "sepsis-alert", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("Sepsis alerts SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_sepsis_alerts(queue)

    return EventSourceResponse(event_generator())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=8000, reload=True)
