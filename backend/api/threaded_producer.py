"""
Threaded Producer Orchestrator

Runs the sync UnifiedProducer in a dedicated background thread,
completely isolated from FastAPI's async event loop.
"""

import threading
import logging
import time
from typing import Optional
from datetime import datetime

# Import the WORKING sync producer
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from producers.unified_producer import UnifiedProducer

logger = logging.getLogger(__name__)


class ThreadedProducerOrchestrator:
    """
    Runs producer in a dedicated thread to avoid async/Kafka conflicts.
    """

    def __init__(
        self,
        clock,  # SimulationClock instance
        subject_ids: list[int],
        tick_interval: float = 2.0,
        max_ticks: Optional[int] = None,
    ):
        self.clock = clock
        self.subject_ids = subject_ids
        self.tick_interval = tick_interval
        self.max_ticks = max_ticks

        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.producer: Optional[UnifiedProducer] = None
        self.tick_count = 0
        self.totals = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

    def start(self):
        """Start the producer thread"""
        if self.thread and self.thread.is_alive():
            logger.warning("Producer thread already running")
            return

        self.running = True
        self.tick_count = 0
        self.totals = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        logger.info("✅ Producer thread started")

    def stop(self):
        """Signal the producer thread to stop (non-blocking)"""
        logger.info("🛑 Signaling producer thread to stop...")
        self.running = False

    def join(self, timeout: float = 10.0):
        """Wait for the producer thread to finish (blocking)"""
        if self.thread:
            logger.info(f"⏳ Waiting for producer thread to finish (timeout={timeout}s)...")
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                logger.warning("Producer thread did not stop cleanly")
            else:
                logger.info("✅ Producer thread terminated cleanly")

    def _run_loop(self):
        """Main loop running in dedicated thread"""
        try:
            # Initialize producer in this thread
            logger.info("🔧 Initializing UnifiedProducer in thread...")
            self.producer = UnifiedProducer(
                clock_url="unused",  # We use direct clock access
                subject_ids=self.subject_ids,
            )
            logger.info("✅ Producer initialized in thread")

            logger.info("🚀 Starting producer loop...")
            logger.info(f"   Patient IDs: {self.subject_ids}")
            logger.info(f"   Tick interval: {self.tick_interval}s")
            logger.info(f"   Max ticks: {self.max_ticks or '∞'}")

            while self.running and self.clock.is_running:
                # Check max ticks
                if self.max_ticks and self.tick_count >= self.max_ticks:
                    logger.info(f"✅ Max ticks ({self.max_ticks}) reached")
                    break

                # Get current window directly from clock (no HTTP!)
                window_start, window_end = self.clock.get_current_window()

                # Process tick
                try:
                    counts = self._process_tick(window_start, window_end)
                    self.tick_count += 1

                    # Update totals
                    for key in self.totals:
                        self.totals[key] += counts[key]

                    # Log summary
                    total_events = sum(counts.values())
                    if total_events > 0:
                        logger.info(
                            f"⏰ Tick #{self.tick_count:04d}: "
                            f"{window_start.strftime('%Y-%m-%d %H:%M:%S')} → "
                            f"{window_end.strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"🏥 Admissions: {counts['admissions']}, "
                            f"🔬 Labs: {counts['labs']}, "
                            f"❤️  Vitals: {counts['vitals']}, "
                            f"🚨 ICU: {counts['icu']}"
                        )
                    else:
                        logger.info(
                            f"⏰ Tick #{self.tick_count:04d}: "
                            f"{window_start.strftime('%Y-%m-%d %H:%M:%S')} → "
                            f"{window_end.strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"📭 No events"
                        )

                except Exception as e:
                    logger.error(f"❌ Error processing tick: {e}", exc_info=True)

                # Wait for next tick
                time.sleep(self.tick_interval)

        except Exception as e:
            logger.error(f"❌ Producer thread error: {e}", exc_info=True)

        finally:
            self._cleanup()

    def _process_tick(self, window_start: datetime, window_end: datetime) -> dict:
        """Process one tick - queries DB and sends to Kafka"""
        # Format times for SQL queries
        start_str = window_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = window_end.strftime('%Y-%m-%d %H:%M:%S')

        counts = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

        # Use the sync producer's methods directly
        # Add small delays between topics to avoid DNS throttling

        # Admissions
        for row in self.producer._get_admissions(start_str, end_str):
            self.producer._send(self.producer.topics['admissions'],
                               self.producer._format_admission(row))
            counts['admissions'] += 1

        # Flush admissions before moving to next topic
        if counts['admissions'] > 0:
            self.producer.producer.flush(5)
            time.sleep(0.1)

        # Labs
        for row in self.producer._get_labs(start_str, end_str):
            self.producer._send(self.producer.topics['labs'],
                               self.producer._format_lab(row))
            counts['labs'] += 1

        if counts['labs'] > 0:
            self.producer.producer.flush(5)
            time.sleep(0.1)

        # ICU
        for row in self.producer._get_icu_stays(start_str, end_str):
            self.producer._send(self.producer.topics['icu'],
                               self.producer._format_icu(row))
            counts['icu'] += 1

        if counts['icu'] > 0:
            self.producer.producer.flush(5)
            time.sleep(0.1)

        # Vitals
        for row in self.producer._get_vitals(start_str, end_str):
            self.producer._send(self.producer.topics['vitals'],
                               self.producer._format_vitals(row))
            counts['vitals'] += 1

        if counts['vitals'] > 0:
            self.producer.producer.flush(5)

        return counts

    def _cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up producer thread...")

        if self.producer:
            try:
                logger.info("⏳ Flushing producer...")
                self.producer.flush(10)
                self.producer.close()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

        # Log final summary
        logger.info("=" * 60)
        logger.info("📊 FINAL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total ticks: {self.tick_count}")
        logger.info(f"Total admissions: {self.totals['admissions']}")
        logger.info(f"Total labs: {self.totals['labs']}")
        logger.info(f"Total vitals: {self.totals['vitals']}")
        logger.info(f"Total ICU: {self.totals['icu']}")
        logger.info(f"Total events: {sum(self.totals.values())}")
        logger.info("✅ Producer thread cleanup complete")
