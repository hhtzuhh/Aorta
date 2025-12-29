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
import subprocess
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

# Import clock service
from coordinator.clock_service import SimulationClock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global unified consumer instance
unified_consumer: UnifiedConsumer = None

# Global clock instance (embedded from coordinator)
clock: Optional[SimulationClock] = None
tick_task: Optional[asyncio.Task] = None
tick_interval_seconds: float = 2.0

# Global orchestrator process management
orchestrator_process: Optional[subprocess.Popen] = None
orchestrator_config: Optional[dict] = None


# Pydantic models for clock configuration
class ClockConfig(BaseModel):
    """Configuration for initializing the clock"""
    start_time: str
    tick_minutes: int = 10
    tick_interval_seconds: Optional[float] = None


class TickInterval(BaseModel):
    """Configuration for auto-tick interval"""
    interval_seconds: float = 2.0


# Pydantic model for simulation configuration
class SimulationConfig(BaseModel):
    """Configuration for starting a simulation"""
    subject_ids: List[int]  # Patient IDs to filter by
    start_time: Optional[str] = None  # Simulation start time (YYYY-MM-DD HH:MM:SS)
    tick_minutes: int = 10  # Minutes per tick window
    tick_interval: float = 2.0  # Seconds between clock ticks
    max_ticks: Optional[int] = None  # Maximum number of ticks (None = unlimited)


async def auto_tick_loop(interval_seconds: float):
    """Background task that automatically advances the clock"""
    global clock
    try:
        while clock.is_running:
            await asyncio.sleep(interval_seconds)
            if clock.is_running:
                window_start, window_end = clock.tick()
                logger.info(f"⏰ Clock tick: {window_start.strftime('%Y-%m-%d %H:%M:%S')} → {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
    except asyncio.CancelledError:
        logger.info("🛑 Auto-tick stopped")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""

    global unified_consumer, clock, tick_task
    logger.info("Starting Aorta Backend API...")

    try:
        # Initialize simulation clock
        logger.info("Initializing simulation clock...")
        clock = SimulationClock(start_time="2134-06-05 22:00:00", tick_minutes=10)
        logger.info("✅ Simulation clock initialized")

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

        # Stop orchestrator if running
        global orchestrator_process
        if orchestrator_process and orchestrator_process.poll() is None:
            logger.info("Stopping orchestrator process...")
            orchestrator_process.terminate()
            try:
                orchestrator_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Orchestrator didn't terminate, killing...")
                orchestrator_process.kill()

        # Stop clock auto-tick if running
        if tick_task and not tick_task.done():
            tick_task.cancel()
            try:
                await tick_task
            except asyncio.CancelledError:
                pass

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


# ==================== Clock Service Endpoints ====================
# Embedded from coordinator/main.py


@app.get("/clock/status")
async def get_clock_status():
    """
    Get the current status of the clock.

    Returns:
        Clock status including current time and running state
    """
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    status = clock.get_status()

    return {
        "current_time": status["current_time"],
        "is_running": status["is_running"],
        "tick_size_minutes": status["tick_size_minutes"],
        "tick_interval_seconds": tick_interval_seconds
    }


@app.get("/clock/current")
async def get_current_window():
    """
    Get the current time window.

    Returns:
        Current simulation time window (start and end)
    """
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    window_start, window_end = clock.get_current_window()

    return {
        "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
        "window_size_minutes": int(clock.tick_size.total_seconds() / 60)
    }


@app.post("/clock/tick")
async def advance_tick():
    """
    Manually advance the clock by one tick.

    Returns:
        New time window after the tick
    """
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    window_start, window_end = clock.tick()

    return {
        "message": "Clock advanced",
        "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S")
    }


@app.post("/clock/start")
async def start_auto_tick(config: TickInterval = TickInterval()):
    """
    Start automatic clock ticking.

    Args:
        config: Tick interval configuration (default: 2 seconds)

    Returns:
        Status message
    """
    global tick_task

    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    if clock.is_running:
        return {"message": "Clock is already running", "status": "running"}

    clock.set_running(True)
    tick_task = asyncio.create_task(auto_tick_loop(config.interval_seconds))

    return {
        "message": "Auto-tick started",
        "interval_seconds": config.interval_seconds,
        "status": "running"
    }


@app.post("/clock/stop")
async def stop_auto_tick():
    """
    Stop automatic clock ticking.

    Returns:
        Status message
    """
    global tick_task

    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    if not clock.is_running:
        return {"message": "Clock is not running", "status": "stopped"}

    clock.set_running(False)

    if tick_task and not tick_task.done():
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass

    return {
        "message": "Auto-tick stopped",
        "status": "stopped"
    }


@app.post("/clock/reset")
async def reset_clock(config: ClockConfig):
    """
    Reset the clock to a new starting time.

    Args:
        config: Clock configuration with new start time

    Returns:
        Status message with new configuration
    """
    global clock, tick_task, tick_interval_seconds

    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    # Stop auto-tick if running
    if clock.is_running:
        clock.set_running(False)
        if tick_task and not tick_task.done():
            tick_task.cancel()
            try:
                await tick_task
            except asyncio.CancelledError:
                pass

    # Store tick interval if provided
    if config.tick_interval_seconds is not None:
        tick_interval_seconds = config.tick_interval_seconds

    # Create new clock with new configuration
    clock = SimulationClock(
        start_time=config.start_time,
        tick_minutes=config.tick_minutes
    )

    return {
        "message": "Clock reset successfully",
        "start_time": config.start_time,
        "tick_minutes": config.tick_minutes,
        "tick_interval_seconds": tick_interval_seconds,
        "status": "stopped"
    }


# ==================== Simulation Control Endpoints ====================
# Manage orchestrator subprocess for data streaming


@app.post("/api/simulation/start")
async def start_simulation(config: SimulationConfig):
    """
    Start a new simulation by spawning the orchestrator subprocess.

    Args:
        config: Simulation configuration (subject_ids, start_time, etc.)

    Returns:
        Status message with simulation details
    """
    global orchestrator_process, orchestrator_config

    # Check if simulation is already running
    if orchestrator_process and orchestrator_process.poll() is None:
        raise HTTPException(
            status_code=400,
            detail="Simulation already running. Stop it first before starting a new one."
        )

    # Reset clock if start_time is provided
    if config.start_time:
        logger.info(f"Resetting clock to {config.start_time}")
        await reset_clock(ClockConfig(
            start_time=config.start_time,
            tick_minutes=config.tick_minutes,
            tick_interval_seconds=config.tick_interval
        ))

    # Start clock auto-tick
    logger.info(f"Starting clock auto-tick (interval: {config.tick_interval}s)")
    await start_auto_tick(TickInterval(interval_seconds=config.tick_interval))

    # Build orchestrator command
    orchestrator_path = Path(__file__).parent.parent.parent / "coordinator" / "orchestrator.py"

    cmd = [
        sys.executable,  # Use same Python interpreter
        str(orchestrator_path),
        "--subject-ids", *[str(sid) for sid in config.subject_ids],
        "--tick-interval", str(config.tick_interval),
        "--clock-url", "http://localhost:8000/clock",  # Use embedded clock
        "--tick-minutes", str(config.tick_minutes),
    ]

    if config.max_ticks:
        cmd.extend(["--max-ticks", str(config.max_ticks)])

    if config.start_time:
        cmd.extend(["--start-time", config.start_time])

    # Start orchestrator subprocess
    try:
        logger.info(f"Starting orchestrator: {' '.join(cmd)}")
        orchestrator_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Store config
        orchestrator_config = {
            "subject_ids": config.subject_ids,
            "start_time": config.start_time,
            "tick_minutes": config.tick_minutes,
            "tick_interval": config.tick_interval,
            "max_ticks": config.max_ticks,
            "started_at": datetime.now().isoformat()
        }

        logger.info(f"✅ Orchestrator started (PID: {orchestrator_process.pid})")

        return {
            "message": "Simulation started",
            "status": "running",
            "pid": orchestrator_process.pid,
            "config": orchestrator_config
        }

    except Exception as e:
        logger.error(f"Failed to start orchestrator: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start simulation: {str(e)}")


@app.post("/api/simulation/stop")
async def stop_simulation():
    """
    Stop the running simulation by terminating the orchestrator subprocess.

    Returns:
        Status message
    """
    global orchestrator_process, orchestrator_config

    if not orchestrator_process:
        raise HTTPException(status_code=400, detail="No simulation is running")

    if orchestrator_process.poll() is not None:
        # Process already terminated
        orchestrator_process = None
        orchestrator_config = None
        return {"message": "Simulation was not running", "status": "stopped"}

    # Terminate the process
    logger.info(f"Stopping orchestrator (PID: {orchestrator_process.pid})...")
    orchestrator_process.terminate()

    try:
        orchestrator_process.wait(timeout=5)
        logger.info("✅ Orchestrator stopped gracefully")
    except subprocess.TimeoutExpired:
        logger.warning("Orchestrator didn't terminate, killing...")
        orchestrator_process.kill()
        orchestrator_process.wait()
        logger.info("✅ Orchestrator killed")

    # Stop clock auto-tick
    await stop_auto_tick()

    orchestrator_process = None
    orchestrator_config = None

    return {
        "message": "Simulation stopped",
        "status": "stopped"
    }


@app.get("/api/simulation/status")
async def get_simulation_status():
    """
    Get the current status of the simulation.

    Returns:
        Simulation status including running state and configuration
    """
    global orchestrator_process, orchestrator_config

    if not orchestrator_process:
        return {
            "status": "stopped",
            "running": False,
            "config": None
        }

    # Check if process is still alive
    if orchestrator_process.poll() is not None:
        # Process has terminated
        return_code = orchestrator_process.returncode
        orchestrator_process = None
        orchestrator_config = None
        return {
            "status": "stopped",
            "running": False,
            "exit_code": return_code,
            "config": None
        }

    return {
        "status": "running",
        "running": True,
        "pid": orchestrator_process.pid,
        "config": orchestrator_config
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=8000, reload=True)
