"""
Producer Orchestrator - Async Task Version

Replaces the subprocess-based orchestrator.py with an async task
that runs within the FastAPI backend process.

Key benefits:
- Direct clock access (no HTTP timeouts)
- Better error handling
- Simpler debugging (single process)
- Cloud Run friendly
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime

from coordinator.clock_service import SimulationClock
from .async_producer import AsyncUnifiedProducer

logger = logging.getLogger(__name__)


class ProducerOrchestrator:
    """
    Async orchestrator for producer coordination.

    Manages the producer tick loop as an async task within
    the FastAPI backend, eliminating the need for subprocess
    communication and HTTP timeouts.
    """

    def __init__(
        self,
        clock: SimulationClock,
        subject_ids: list[int],
        tick_interval: float = 2.0,
        max_ticks: Optional[int] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            clock: SimulationClock instance (direct reference!)
            subject_ids: Patient IDs to filter
            tick_interval: Seconds between ticks
            max_ticks: Max ticks to process (default: None = unlimited)
        """
        self.clock = clock
        self.subject_ids = subject_ids
        self.tick_interval = tick_interval
        self.max_ticks = max_ticks
        self.running = False
        self.producer: Optional[AsyncUnifiedProducer] = None
        self.tick_count = 0
        self.totals = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

    async def initialize(self):
        """Initialize producer"""
        logger.info("🔧 Initializing AsyncUnifiedProducer...")
        self.producer = AsyncUnifiedProducer(
            subject_ids=self.subject_ids
        )
        await self.producer.warm_up()
        logger.info("✅ Producer initialized")

    async def run(self):
        """
        Main orchestration loop.

        Processes ticks until:
        - self.running = False
        - clock stops running
        - max_ticks reached
        - Task is cancelled
        """
        self.running = True
        logger.info("🚀 Starting producer orchestration loop...")
        logger.info(f"   Patient IDs: {self.subject_ids}")
        logger.info(f"   Tick interval: {self.tick_interval}s")
        logger.info(f"   Max ticks: {self.max_ticks or '∞'}")

        try:
            while self.running and self.clock.is_running:
                # Check max ticks limit
                if self.max_ticks and self.tick_count >= self.max_ticks:
                    logger.info(f"✅ Max ticks ({self.max_ticks}) reached")
                    break

                # Get current window for logging
                window_start, window_end = self.clock.get_current_window()

                # Process tick
                try:
                    counts = await self.producer.process_tick(self.clock)
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
                            f"📭 No events in this window"
                        )

                    # NOTE: Don't flush here! Only flush at shutdown to avoid DNS overload
                    # poll(0) in _send() is enough for message delivery

                except Exception as e:
                    logger.error(f"❌ Error processing tick: {e}", exc_info=True)
                    # Continue running despite errors

                # Wait for next tick
                await asyncio.sleep(self.tick_interval)

        except asyncio.CancelledError:
            logger.info("⚠️  Orchestrator cancelled")
            raise

        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up producer orchestrator...")

        if self.producer:
            try:
                logger.info("⏳ Flushing producer...")
                await self.producer.flush()
                await self.producer.close()
            except Exception as e:
                logger.error(f"Error during producer cleanup: {e}")

        # Log final summary
        logger.info("=" * 80)
        logger.info("📊 FINAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total ticks processed: {self.tick_count}")
        logger.info(f"Total admissions: {self.totals['admissions']}")
        logger.info(f"Total labs: {self.totals['labs']}")
        logger.info(f"Total vitals: {self.totals['vitals']}")
        logger.info(f"Total ICU admissions: {self.totals['icu']}")
        logger.info(f"Total events: {sum(self.totals.values())}")
        logger.info("✅ Orchestrator cleanup complete")

    def stop(self):
        """Signal orchestrator to stop"""
        logger.info("🛑 Stopping orchestrator...")
        self.running = False
