"""
FastAPI Clock Service
HTTP server providing clock coordination endpoints for producers
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
from typing import Optional

from coordinator.clock_service import SimulationClock


# Global clock instance
clock: Optional[SimulationClock] = None
tick_task: Optional[asyncio.Task] = None
tick_interval_seconds: float = 2.0  # Default tick interval


app = FastAPI(
    title="Simulation Clock Service",
    description="Central time coordination service for multi-producer streaming",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ClockConfig(BaseModel):
    """Configuration for initializing the clock"""
    start_time: str
    tick_minutes: int = 10
    tick_interval_seconds: Optional[float] = None  # Optional: tick interval for auto-tick


class TickInterval(BaseModel):
    """Configuration for auto-tick interval"""
    interval_seconds: float = 2.0


@app.on_event("startup")
async def startup_event():
    """Initialize clock on startup"""
    global clock
    # Default start time for MIMIC-IV data
    clock = SimulationClock(start_time="2134-06-05 22:00:00", tick_minutes=10)
    print("🕐 Simulation clock initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Stop auto-tick on shutdown"""
    global tick_task
    if tick_task and not tick_task.done():
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass


async def auto_tick_loop(interval_seconds: float):
    """Background task that automatically advances the clock"""
    global clock
    try:
        while clock.is_running:
            await asyncio.sleep(interval_seconds)
            if clock.is_running:
                window_start, window_end = clock.tick()
                print(f"⏰ Clock tick: {window_start.strftime('%Y-%m-%d %H:%M:%S')} → {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
    except asyncio.CancelledError:
        print("🛑 Auto-tick stopped")
        raise


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Simulation Clock",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/current")
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


@app.post("/tick")
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


@app.post("/start")
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


@app.post("/stop")
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


@app.get("/status")
async def get_status():
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
        "tick_interval_seconds": tick_interval_seconds  # Add tick interval to status
    }


@app.post("/reset")
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
