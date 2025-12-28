"""
Prediction Throttler - Tick-based throttling for simulation

Prevents excessive predictions by limiting to once every N ticks.
"""

from datetime import datetime
from typing import Dict


class PredictionThrottler:
    """Tick-based throttling for simulation - predict once every N ticks"""

    def __init__(self, tick_interval: int = 3):
        """
        Initialize throttler

        Args:
            tick_interval: Number of ticks between predictions (default: 3)
        """
        self.tick_interval = tick_interval
        self.last_prediction_tick: Dict[str, int] = {}  # hadm_id -> tick number

    def should_predict(
        self,
        hadm_id: str,
        event_time: str,
        tick_duration_minutes: int = 60,
        is_critical_event: bool = False,
    ) -> bool:
        """
        Decide if we should predict based on tick count

        Args:
            hadm_id: Patient admission ID
            event_time: Event timestamp from event (simulated time)
            tick_duration_minutes: Minutes per tick (default 60)
            is_critical_event: Override throttle for critical events

        Returns:
            True if should predict, False otherwise

        Logic:
            1. First prediction for this patient → True
            2. Critical event (abnormal lab, ICU admission) → True (override)
            3. N ticks have passed since last prediction → True
            4. Otherwise → False (throttled)
        """
        # Calculate current tick number from event timestamp
        dt = datetime.fromisoformat(event_time)
        current_tick = int(dt.timestamp() / (tick_duration_minutes * 60))

        last_tick = self.last_prediction_tick.get(hadm_id)

        # First prediction for this patient
        if last_tick is None:
            self.last_prediction_tick[hadm_id] = current_tick
            return True

        # Critical events override throttle
        if is_critical_event:
            self.last_prediction_tick[hadm_id] = current_tick
            return True

        # Check if enough ticks have passed
        ticks_since_last = current_tick - last_tick
        if ticks_since_last >= self.tick_interval:
            self.last_prediction_tick[hadm_id] = current_tick
            return True

        return False

    def reset(self, hadm_id: str):
        """Reset throttle state for a patient (e.g., on discharge)"""
        if hadm_id in self.last_prediction_tick:
            del self.last_prediction_tick[hadm_id]

    def get_stats(self) -> Dict[str, int]:
        """Get throttling statistics"""
        return {
            "total_patients": len(self.last_prediction_tick),
            "tick_interval": self.tick_interval,
        }
