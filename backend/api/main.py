"""
Aorta Backend API - Main Application

FastAPI application with Server-Sent Events (SSE) for real-time
hospital admission monitoring.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

from .config import settings
from .kafka_consumer import AdmissionConsumer
from .lab_consumer import LabConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global consumer instances
consumer: AdmissionConsumer = None
lab_consumer: LabConsumer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""

    # Startup
    global consumer, lab_consumer
    logger.info("Starting Aorta Backend API...")

    try:
        # Initialize Kafka consumers
        consumer = AdmissionConsumer(settings)
        lab_consumer = LabConsumer(settings)

        # Start consuming in background tasks
        asyncio.create_task(consumer.start())
        asyncio.create_task(lab_consumer.start())

        logger.info("✅ Kafka consumers started successfully")
        logger.info(f"📡 Listening to topics: {settings.kafka_topic}, patient-labs")
        logger.info(f"🌐 CORS origins: {settings.cors_origins}")

        yield

    finally:
        # Shutdown
        logger.info("Shutting down Aorta Backend API...")

        if consumer:
            await consumer.stop()

        if lab_consumer:
            await lab_consumer.stop()

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
            "stream_admissions": "/stream/admissions",
            "stream_labs": "/stream/labs",
        }
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint

    Returns service health status
    """
    return {
        "status": "healthy",
        "admission_consumer_running": consumer.running if consumer else False,
        "lab_consumer_running": lab_consumer.running if lab_consumer else False,
        "admission_sse_clients": len(consumer.sse_queues) if consumer else 0,
        "lab_sse_clients": len(lab_consumer.sse_queues) if lab_consumer else 0,
        "recent_admissions": len(consumer.recent_admissions) if consumer else 0,
        "recent_labs": len(lab_consumer.recent_labs) if lab_consumer else 0,
    }


@app.get("/api/admissions")
async def get_recent_admissions() -> List[dict]:
    """
    Get recent admissions (REST endpoint)

    Returns the last 50 admissions from the circular buffer.
    """
    if not consumer:
        return []

    return consumer.get_recent_admissions()


@app.get("/stream/admissions")
async def stream_admissions(request: Request):
    """
    Server-Sent Events endpoint for real-time admission stream

    Clients connect to this endpoint to receive admission events
    as they arrive from Kafka.

    Returns:
        EventSourceResponse: SSE stream
    """

    if not consumer:
        logger.error("Consumer not initialized")
        return {"error": "Consumer not available"}

    # Subscribe to SSE updates
    queue = consumer.subscribe_sse()

    async def event_generator():
        """Generate SSE events from queue"""

        try:
            # Send initial connection message
            yield {
                "event": "connected",
                "data": json.dumps({
                    "message": "Connected to admission stream",
                    "timestamp": str(asyncio.get_event_loop().time())
                })
            }

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info("Client disconnected from SSE stream")
                    break

                try:
                    # Wait for event from queue (with timeout for heartbeat)
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)

                    # Send admission event
                    yield {
                        "event": "admission",
                        "data": json.dumps(event)
                    }

                except asyncio.TimeoutError:
                    # Send heartbeat/keep-alive comment
                    yield {
                        "comment": "keepalive"
                    }

        except asyncio.CancelledError:
            logger.info("SSE stream cancelled")

        finally:
            # Clean up: unsubscribe from consumer
            consumer.unsubscribe_sse(queue)
            logger.info("SSE client cleanup complete")

    return EventSourceResponse(event_generator())


@app.get("/api/labs")
async def get_recent_labs() -> List[dict]:
    """
    Get recent labs (REST endpoint)

    Returns the last 200 labs from the circular buffer.
    """
    if not lab_consumer:
        return []

    return lab_consumer.get_recent_labs()


@app.get("/stream/labs")
async def stream_labs(request: Request):
    """
    Server-Sent Events endpoint for real-time lab stream

    Clients connect to this endpoint to receive lab events
    as they arrive from Kafka.

    Returns:
        EventSourceResponse: SSE stream
    """

    if not lab_consumer:
        logger.error("Lab consumer not initialized")
        return {"error": "Lab consumer not available"}

    # Subscribe to SSE updates
    queue = lab_consumer.subscribe_sse()

    async def event_generator():
        """Generate SSE events from queue"""

        try:
            # Send initial connection message
            yield {
                "event": "connected",
                "data": json.dumps({
                    "message": "Connected to lab stream",
                    "timestamp": str(asyncio.get_event_loop().time())
                })
            }

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info("Client disconnected from lab SSE stream")
                    break

                try:
                    # Wait for event from queue (with timeout for heartbeat)
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)

                    # Send lab event
                    yield {
                        "event": "lab",
                        "data": json.dumps(event)
                    }

                except asyncio.TimeoutError:
                    # Send heartbeat/keep-alive comment
                    yield {
                        "comment": "keepalive"
                    }

        except asyncio.CancelledError:
            logger.info("Lab SSE stream cancelled")

        finally:
            # Clean up: unsubscribe from consumer
            lab_consumer.unsubscribe_sse(queue)
            logger.info("Lab SSE client cleanup complete")

    return EventSourceResponse(event_generator())


# Development server
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
