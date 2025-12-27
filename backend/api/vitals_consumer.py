"""
Kafka Consumer for Chartevent (Vitals) Events with SSE Broadcasting

Consumes patient chartevent (vital signs) from Kafka and broadcasts
to connected SSE clients in real-time.
"""

import asyncio
import json
import logging
from collections import deque
from typing import List, Set
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import Settings
from .models import CharteventEvent

logger = logging.getLogger(__name__)


class VitalsConsumer:
    """
    Kafka consumer for chartevent (vital signs) events with SSE broadcast capability

    Features:
    - Consumes from patient-vitals topic
    - Maintains circular buffer of recent chartevents
    - Broadcasts new events to all connected SSE clients
    - Thread-safe queue management for SSE clients
    """

    def __init__(self, settings: Settings):
        self.settings = settings

        # Kafka consumer configuration
        self.consumer_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'security.protocol': settings.kafka_security_protocol,
            'sasl.mechanism': settings.kafka_sasl_mechanism,
            'sasl.username': settings.kafka_sasl_username,
            'sasl.password': settings.kafka_sasl_password,
            'group.id': 'aorta-vitals-consumer-v1',
            'auto.offset.reset': 'earliest',  # Start from beginning for testing
            'enable.auto.commit': True,
            'log_level': 0,  # Suppress librdkafka debug logs
        }

        self.consumer = None
        self.running = False

        # Circular buffer for recent chartevents (very frequent events)
        self.recent_chartevents: deque = deque(maxlen=500)

        # Set of asyncio queues for SSE clients
        self.sse_queues: Set[asyncio.Queue] = set()

        logger.info("VitalsConsumer initialized")

    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe(["patient-vitals"])
            self.running = True

            logger.info(
                f"Kafka vitals consumer started. Topic: patient-vitals, "
                f"Group: aorta-vitals-consumer-v1"
            )

            # Start consumption loop
            await self._consume_loop()

        except KafkaException as e:
            logger.error(f"Kafka error in vitals consumer: {e}")
            raise

    async def _consume_loop(self):
        """Main consumption loop - runs in background task"""

        logger.info("Starting Kafka vitals consumption loop")

        while self.running:
            try:
                # Poll for messages (non-blocking with timeout)
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)

                if msg is None:
                    # No message, continue
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    elif msg.error().code() == KafkaError._RESOLVE:
                        # DNS resolution errors - common during startup, log at debug
                        logger.debug(f"Kafka DNS resolution error: {msg.error()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Parse and process message
                await self._process_message(msg)

            except Exception as e:
                logger.error(f"Error in vitals consumption loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause before retry

    async def _process_message(self, msg):
        """Process a single Kafka message"""

        try:
            # Parse JSON message
            event_data = json.loads(msg.value().decode('utf-8'))

            # Skip warm-up test messages from producers
            if event_data.get('test'):
                return

            # Validate and create CharteventEvent
            chartevent = CharteventEvent(**event_data)

            # Add to circular buffer
            self.recent_chartevents.append(chartevent)

            # Log chartevents (debug level to avoid spam, as these are very frequent)
            logger.debug(
                f"📊 Chartevent: {chartevent.chartevent.label} = "
                f"{chartevent.chartevent.value_numeric or chartevent.chartevent.value_text} "
                f"{chartevent.chartevent.unit or ''} - "
                f"Patient {chartevent.patient.subject_id}"
            )

            # Broadcast to all SSE clients
            await self._broadcast_to_sse(chartevent)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Failed to process chartevent message: {e}", exc_info=True)

    async def _broadcast_to_sse(self, chartevent: CharteventEvent):
        """Broadcast chartevent event to all connected SSE clients"""

        if not self.sse_queues:
            return  # No clients connected

        # Convert to dict for JSON serialization
        event_dict = chartevent.model_dump()

        # Send to all queues
        dead_queues = set()

        for queue in self.sse_queues:
            try:
                # Non-blocking put
                queue.put_nowait(event_dict)
            except asyncio.QueueFull:
                logger.warning("SSE queue full, dropping chartevent")
            except Exception as e:
                logger.error(f"Failed to broadcast chartevent to queue: {e}")
                dead_queues.add(queue)

        # Remove dead queues
        self.sse_queues -= dead_queues

        logger.debug(f"Broadcasted chartevent to {len(self.sse_queues)} SSE clients")

    def get_recent_chartevents(self) -> List[dict]:
        """
        Get recent chartevents from circular buffer

        Returns:
            List of chartevent events as dictionaries (newest first)
        """
        # Convert deque to list and reverse (newest first)
        return [chartevent.model_dump() for chartevent in reversed(self.recent_chartevents)]

    def subscribe_sse(self) -> asyncio.Queue:
        """
        Create a new queue for an SSE client

        Returns:
            asyncio.Queue for receiving chartevent events
        """
        queue = asyncio.Queue(maxsize=100)
        self.sse_queues.add(queue)

        logger.info(f"New SSE client subscribed to vitals. Total clients: {len(self.sse_queues)}")

        return queue

    def unsubscribe_sse(self, queue: asyncio.Queue):
        """Remove a queue when SSE client disconnects"""

        self.sse_queues.discard(queue)

        logger.info(f"SSE client unsubscribed from vitals. Total clients: {len(self.sse_queues)}")

    async def stop(self):
        """Stop the Kafka consumer gracefully"""

        logger.info("Stopping Kafka vitals consumer...")

        self.running = False

        if self.consumer:
            await asyncio.to_thread(self.consumer.close)

        # Clear all SSE queues
        self.sse_queues.clear()

        logger.info("Kafka vitals consumer stopped")
