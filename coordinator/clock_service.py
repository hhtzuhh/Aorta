"""
Simulation Clock Service
Manages temporal coordination for multi-producer streaming system
"""

from datetime import datetime, timedelta
from typing import Tuple
import threading


class SimulationClock:
    """
    Central simulation clock that orchestrates temporal event streaming.

    Manages time windows for coordinated data streaming from multiple producers,
    ensuring all events are streamed in chronological order.
    """

    def __init__(self, start_time: str, tick_minutes: int = 10):
        """
        Initialize the simulation clock.

        Args:
            start_time: Starting simulation time in format "YYYY-MM-DD HH:MM:SS"
            tick_minutes: Size of each time window in minutes (default: 10)
        """
        self.current_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        self.tick_size = timedelta(minutes=tick_minutes)
        self.is_running = False
        self._lock = threading.RLock()  # Use RLock for reentrant locking

    def get_current_window(self) -> Tuple[datetime, datetime]:
        """
        Get the current time window.

        Returns:
            Tuple of (window_start, window_end) as datetime objects
        """
        with self._lock:
            return (self.current_time, self.current_time + self.tick_size)

    def tick(self) -> Tuple[datetime, datetime]:
        """
        Advance the clock by one tick.

        Returns:
            Tuple of (new_window_start, new_window_end) after the tick
        """
        with self._lock:
            self.current_time += self.tick_size
            return self.get_current_window()

    def set_running(self, running: bool):
        """
        Set the running state of the clock.

        Args:
            running: True to mark as running, False to stop
        """
        with self._lock:
            self.is_running = running

    def get_status(self) -> dict:
        """
        Get the current status of the clock.

        Returns:
            Dictionary with current_time, is_running, and tick_size
        """
        with self._lock:
            return {
                "current_time": self.current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "is_running": self.is_running,
                "tick_size_minutes": int(self.tick_size.total_seconds() / 60)
            }

    def reset(self, start_time: str):
        """
        Reset the clock to a new starting time.

        Args:
            start_time: New starting time in format "YYYY-MM-DD HH:MM:SS"
        """
        with self._lock:
            self.current_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            self.is_running = False
