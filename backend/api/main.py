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
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

# Pre-import anyio backends to avoid lazy import race condition
import anyio._backends._asyncio
import anyio._core._eventloop
import anyio._core._synchronization

from .config import settings
from .unified_consumer import UnifiedConsumer

# Producer service client
import requests
import os

PRODUCER_SERVICE_URL = os.getenv("PRODUCER_SERVICE_URL", "http://localhost:9001")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global unified consumer instance
unified_consumer: UnifiedConsumer = None


# Pydantic model for simulation configuration
class SimulationConfig(BaseModel):
    """Configuration for starting a simulation"""
    subject_ids: List[int]  # Patient IDs to filter by
    start_time: str  # Simulation start time (YYYY-MM-DD HH:MM:SS)
    tick_minutes: int = 60  # Minutes per tick window
    tick_interval: float = 2.0  # Seconds between clock ticks
    max_ticks: Optional[int] = None  # Maximum number of ticks (None = unlimited)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""
    global unified_consumer
    logger.info("Starting Aorta Backend API (Consumer Service)...")

    try:
        # Initialize single unified Kafka consumer for all topics
        logger.info("Initializing unified Kafka consumer...")
        unified_consumer = UnifiedConsumer(settings)
        asyncio.create_task(unified_consumer.start())

        logger.info("✅ Unified Kafka consumer started")
        logger.info(f"📡 Listening to topics: {settings.kafka_topic}, patient-labs, icu-admissions, patient-vitals")
        logger.info(f"🌐 CORS origins: {settings.cors_origins}")
        logger.info(f"🔗 Producer service URL: {PRODUCER_SERVICE_URL}")

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
            "clinical_recommendations": "/api/clinical-recommendations",
            "stream_admissions": "/stream/admissions",
            "stream_labs": "/stream/labs",
            "stream_icu_admissions": "/stream/icu-admissions",
            "stream_vitals": "/stream/vitals",
            "stream_sepsis_alerts": "/stream/sepsis-alerts",
            "stream_clinical_recommendations": "/stream/clinical-recommendations",
            "clock_status": "/clock/status",
            "clock_current": "/clock/current",
            "clock_tick": "/clock/tick",
            "clock_start": "/clock/start",
            "clock_stop": "/clock/stop",
            "clock_reset": "/clock/reset",
            "simulation_start": "/api/simulation/start",
            "simulation_stop": "/api/simulation/stop",
            "simulation_status": "/api/simulation/status",
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
        "recommendation_sse_clients": len(unified_consumer.recommendation_sse_queues) if unified_consumer else 0,
        "recent_admissions": len(unified_consumer.recent_admissions) if unified_consumer else 0,
        "recent_labs": len(unified_consumer.recent_labs) if unified_consumer else 0,
        "recent_icu_admissions": len(unified_consumer.recent_icu_admissions) if unified_consumer else 0,
        "recent_chartevents": len(unified_consumer.recent_chartevents) if unified_consumer else 0,
        "recent_sepsis_alerts": len(unified_consumer.recent_sepsis_alerts) if unified_consumer else 0,
        "recent_recommendations": len(unified_consumer.recent_recommendations) if unified_consumer else 0,
        "ml_module_stats": unified_consumer.ml_module.get_stats() if (unified_consumer and unified_consumer.ml_module) else {},
        "rag_module_enabled": unified_consumer.rag_module is not None if unified_consumer else False,
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


@app.get("/api/clinical-recommendations")
async def get_recent_recommendations() -> List[dict]:
    """Get recent RAG-generated clinical recommendations"""
    if not unified_consumer:
        return []
    return unified_consumer.get_recent_recommendations()


@app.get("/stream/clinical-recommendations")
async def stream_clinical_recommendations(request: Request):
    """SSE endpoint for clinical recommendations stream"""
    if not unified_consumer:
        return {"error": "Consumer not available"}

    queue = unified_consumer.subscribe_recommendations()

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to clinical recommendations stream"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "recommendation", "data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}

        except asyncio.CancelledError:
            logger.info("Clinical recommendations SSE stream cancelled")
        finally:
            unified_consumer.unsubscribe_recommendations(queue)

    return EventSourceResponse(event_generator())


# ==================== Clock Proxy Endpoints ====================
# Proxy clock requests to producer service for frontend compatibility


@app.get("/clock/status")
async def get_clock_status():
    """Proxy to producer service clock status"""
    try:
        response = requests.get(f"{PRODUCER_SERVICE_URL}/clock/status", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Producer service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/clock/tick")
async def clock_tick():
    """Proxy to producer service clock tick"""
    try:
        response = requests.post(f"{PRODUCER_SERVICE_URL}/clock/tick", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Producer service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Simulation Control Endpoints ====================
# Calls producer service to start/stop data streaming


@app.post("/api/simulation/start")
async def start_simulation(config: SimulationConfig):
    """
    Start a new simulation by calling the producer service.

    Args:
        config: Simulation configuration (subject_ids, start_time, etc.)

    Returns:
        Status message from producer service
    """
    try:
        logger.info(f"Calling producer service to start simulation: {PRODUCER_SERVICE_URL}/start")

        response = requests.post(
            f"{PRODUCER_SERVICE_URL}/start",
            json=config.dict(),
            timeout=10
        )
        response.raise_for_status()

        logger.info("✅ Simulation started via producer service")
        return response.json()

    except requests.exceptions.ConnectionError:
        logger.error(f"Failed to connect to producer service at {PRODUCER_SERVICE_URL}")
        raise HTTPException(
            status_code=503,
            detail=f"Producer service unavailable at {PRODUCER_SERVICE_URL}. Is it running?"
        )
    except requests.exceptions.Timeout:
        logger.error(f"Producer service timeout")
        raise HTTPException(
            status_code=504,
            detail="Producer service timeout"
        )
    except requests.exceptions.HTTPError as e:
        logger.error(f"Producer service error: {e.response.text}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.json().get("detail", str(e))
        )
    except Exception as e:
        logger.error(f"Unexpected error calling producer service: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/simulation/stop")
async def stop_simulation():
    """
    Stop the running simulation by calling the producer service.

    Returns:
        Status message from producer service
    """
    try:
        logger.info(f"Calling producer service to stop simulation: {PRODUCER_SERVICE_URL}/stop")

        response = requests.post(
            f"{PRODUCER_SERVICE_URL}/stop",
            timeout=10
        )
        response.raise_for_status()

        logger.info("✅ Simulation stopped via producer service")
        return response.json()

    except requests.exceptions.ConnectionError:
        logger.error(f"Failed to connect to producer service at {PRODUCER_SERVICE_URL}")
        raise HTTPException(
            status_code=503,
            detail=f"Producer service unavailable at {PRODUCER_SERVICE_URL}"
        )
    except Exception as e:
        logger.error(f"Error calling producer service: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/simulation/status")
async def get_simulation_status():
    """
    Get the current status of the simulation from the producer service.

    Returns:
        Simulation status from producer service
    """
    try:
        response = requests.get(
            f"{PRODUCER_SERVICE_URL}/status",
            timeout=5
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.ConnectionError:
        # If producer service is down, return stopped status
        return {
            "status": "stopped",
            "running": False,
            "config": None,
            "tick_count": 0,
            "totals": {}
        }
    except Exception as e:
        logger.error(f"Error getting producer status: {e}")
        return {
            "status": "unknown",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=8000, reload=True)
