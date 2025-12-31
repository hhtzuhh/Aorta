"""
Producer Service - Standalone Kafka Producer with Embedded Clock

Runs on port 9001, completely isolated from consumer service.
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from coordinator.clock_service import SimulationClock
from backend.api.threaded_producer import ThreadedProducerOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
clock: Optional[SimulationClock] = None
tick_task: Optional[asyncio.Task] = None
producer_orchestrator: Optional[ThreadedProducerOrchestrator] = None
producer_config: Optional[dict] = None


# Pydantic models
class ClockConfig(BaseModel):
    start_time: str
    tick_minutes: int = 60


class TickInterval(BaseModel):
    interval_seconds: float = 2.0


class SimulationConfig(BaseModel):
    subject_ids: list[int]
    start_time: str
    tick_interval: float = 2.0
    tick_minutes: int = 60
    max_ticks: Optional[int] = None


# Auto-tick function
async def auto_tick_loop(interval_seconds: float):
    """Background task that auto-ticks the clock"""
    global clock
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            if clock and clock.is_running:
                clock.tick()
        except asyncio.CancelledError:
            logger.info("Auto-tick loop cancelled")
            break
        except Exception as e:
            logger.error(f"Error in auto-tick loop: {e}", exc_info=True)


async def start_auto_tick(interval: TickInterval):
    """Start auto-tick background task"""
    global tick_task, clock

    # Stop existing task
    if tick_task and not tick_task.done():
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass

    # Start new task
    if clock:
        clock.set_running(True)
        tick_task = asyncio.create_task(auto_tick_loop(interval.interval_seconds))
        logger.info(f"Auto-tick started (interval: {interval.interval_seconds}s)")


async def stop_auto_tick():
    """Stop auto-tick background task"""
    global tick_task, clock

    if clock:
        clock.set_running(False)

    if tick_task and not tick_task.done():
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass
        logger.info("Auto-tick stopped")


# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic"""
    global clock

    # Startup
    logger.info("🚀 Starting Producer Service...")

    # Initialize clock with default time
    clock = SimulationClock(
        start_time="2116-06-26 18:00:00",
        tick_minutes=60
    )
    logger.info(f"✅ Clock initialized: {clock.get_status()['current_time']}")

    yield

    # Shutdown
    logger.info("🛑 Shutting down Producer Service...")

    # Stop producer if running
    if producer_orchestrator and producer_orchestrator.running:
        producer_orchestrator.stop()
        await asyncio.to_thread(producer_orchestrator.join, timeout=5.0)

    # Stop auto-tick
    await stop_auto_tick()

    logger.info("✅ Producer Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Aorta Producer Service",
    description="Standalone Kafka producer with embedded simulation clock",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# CLOCK ENDPOINTS
# ============================================================================

@app.get("/clock/status")
async def get_clock_status():
    """Get current clock status"""
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")
    return clock.get_status()


@app.post("/clock/reset")
async def reset_clock(config: ClockConfig):
    """Reset clock to a new time"""
    global clock
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    clock.reset(config.start_time)
    logger.info(f"Clock reset to {config.start_time}")
    return clock.get_status()


@app.post("/clock/tick")
async def manual_tick():
    """Manually advance the clock by one tick"""
    if not clock:
        raise HTTPException(status_code=500, detail="Clock not initialized")

    new_window = clock.tick()
    logger.info(f"Manual tick: {new_window[0]} → {new_window[1]}")
    return {
        "window_start": new_window[0].strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": new_window[1].strftime("%Y-%m-%d %H:%M:%S")
    }


@app.post("/clock/start")
async def start_clock(interval: TickInterval):
    """Start clock auto-tick"""
    await start_auto_tick(interval)
    return {"message": "Clock auto-tick started", "interval_seconds": interval.interval_seconds}


@app.post("/clock/stop")
async def stop_clock():
    """Stop clock auto-tick"""
    await stop_auto_tick()
    return {"message": "Clock auto-tick stopped"}


# ============================================================================
# PRODUCER ENDPOINTS
# ============================================================================

@app.post("/start")
async def start_producer(config: SimulationConfig):
    """
    Start the producer with the given configuration.
    This resets the clock, starts auto-tick, and begins producing to Kafka.
    """
    global producer_orchestrator, producer_config, clock

    # Check if already running
    if producer_orchestrator and producer_orchestrator.running:
        raise HTTPException(
            status_code=400,
            detail="Producer already running. Stop it first."
        )

    try:
        # Reset clock
        logger.info(f"Resetting clock to {config.start_time}")
        clock.reset(config.start_time)

        # Start auto-tick
        logger.info(f"Starting clock auto-tick (interval: {config.tick_interval}s)")
        await start_auto_tick(TickInterval(interval_seconds=config.tick_interval))

        # Create producer orchestrator
        logger.info("Creating producer orchestrator...")
        producer_orchestrator = ThreadedProducerOrchestrator(
            clock=clock,
            subject_ids=config.subject_ids,
            tick_interval=config.tick_interval,
            max_ticks=config.max_ticks
        )

        # Start producer thread
        logger.info("Starting producer thread...")
        producer_orchestrator.start()

        # Store config
        producer_config = config.dict()

        logger.info("✅ Producer started successfully")

        return {
            "status": "running",
            "config": producer_config,
            "clock_status": clock.get_status()
        }

    except Exception as e:
        logger.error(f"Failed to start producer: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to start producer: {str(e)}")


@app.post("/stop")
async def stop_producer():
    """Stop the producer"""
    global producer_orchestrator, producer_config

    if not producer_orchestrator or not producer_orchestrator.running:
        return {"message": "Producer is not running", "status": "stopped"}

    # Signal stop
    logger.info("Stopping producer thread...")
    producer_orchestrator.stop()

    # Wait for thread to finish
    await asyncio.to_thread(producer_orchestrator.join, timeout=5.0)

    # Stop clock auto-tick
    await stop_auto_tick()

    producer_orchestrator = None
    producer_config = None

    logger.info("✅ Producer stopped")

    return {
        "message": "Producer stopped",
        "status": "stopped"
    }


@app.get("/status")
async def get_producer_status():
    """Get current producer status"""
    if not producer_orchestrator:
        return {
            "status": "stopped",
            "running": False,
            "config": None,
            "tick_count": 0,
            "totals": {}
        }

    return {
        "status": "running" if producer_orchestrator.running else "stopped",
        "running": producer_orchestrator.running,
        "config": producer_config,
        "tick_count": producer_orchestrator.tick_count,
        "totals": producer_orchestrator.totals
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "producer"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9001)
