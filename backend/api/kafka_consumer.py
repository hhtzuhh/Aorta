"""
Kafka Consumer with SSE Broadcasting

Consumes hospital admission events from Kafka and broadcasts
to connected SSE clients in real-time.
"""

import asyncio
import json
import logging
from collections import deque
from typing import List, Set
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import Settings
from .models import AdmissionEvent

logger = logging.getLogger(__name__)


class AdmissionConsumer:
    """
    Kafka consumer for hospital admissions with SSE broadcast capability

    Features:
    - Consumes from hospital-admissions topic
    - Maintains circular buffer of recent admissions
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
            'group.id': settings.kafka_group_id,
            'auto.offset.reset': 'earliest',  # Start from beginning for testing
            'enable.auto.commit': True,
        }

        self.consumer = None
        self.running = False

        # Circular buffer for recent admissions
        self.recent_admissions: deque = deque(maxlen=settings.max_recent_admissions)

        # Set of asyncio queues for SSE clients
        self.sse_queues: Set[asyncio.Queue] = set()

        logger.info("AdmissionConsumer initialized")

    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe([self.settings.kafka_topic])
            self.running = True

            logger.info(
                f"Kafka consumer started. Topic: {self.settings.kafka_topic}, "
                f"Group: {self.settings.kafka_group_id}"
            )

            # Start consumption loop
            await self._consume_loop()

        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            raise

    async def _consume_loop(self):
        """Main consumption loop - runs in background task"""

        logger.info("Starting Kafka consumption loop")

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
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Parse and process message
                await self._process_message(msg)

            except Exception as e:
                logger.error(f"Error in consumption loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause before retry

    async def _process_message(self, msg):
        """Process a single Kafka message"""

        try:
            # Parse JSON message
            event_data = json.loads(msg.value().decode('utf-8'))

            # Validate and create AdmissionEvent
            admission = AdmissionEvent(**event_data)

            # Add to circular buffer
            self.recent_admissions.append(admission)

            # Log high-priority admissions
            if admission.is_high_priority:
                logger.info(
                    f"🚨 High-priority admission: {admission.admission.type} - "
                    f"Patient {admission.patient.subject_id}"
                )
            else:
                logger.debug(
                    f"📋 Admission: {admission.admission.type} - "
                    f"Patient {admission.patient.subject_id}"
                )

            # Broadcast to all SSE clients
            await self._broadcast_to_sse(admission)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Failed to process message: {e}", exc_info=True)

    async def _broadcast_to_sse(self, admission: AdmissionEvent):
        """Broadcast admission event to all connected SSE clients"""

        if not self.sse_queues:
            return  # No clients connected

        # Convert to dict for JSON serialization
        event_dict = admission.model_dump()

        # Send to all queues
        dead_queues = set()

        for queue in self.sse_queues:
            try:
                # Non-blocking put
                queue.put_nowait(event_dict)
            except asyncio.QueueFull:
                logger.warning("SSE queue full, dropping event")
            except Exception as e:
                logger.error(f"Failed to broadcast to queue: {e}")
                dead_queues.add(queue)

        # Remove dead queues
        self.sse_queues -= dead_queues

        logger.debug(f"Broadcasted to {len(self.sse_queues)} SSE clients")

    def get_recent_admissions(self) -> List[dict]:
        """
        Get recent admissions from circular buffer

        Returns:
            List of admission events as dictionaries (newest first)
        """
        # Convert deque to list and reverse (newest first)
        return [admission.model_dump() for admission in reversed(self.recent_admissions)]

    def subscribe_sse(self) -> asyncio.Queue:
        """
        Create a new queue for an SSE client

        Returns:
            asyncio.Queue for receiving admission events
        """
        queue = asyncio.Queue(maxsize=100)
        self.sse_queues.add(queue)

        logger.info(f"New SSE client subscribed. Total clients: {len(self.sse_queues)}")

        return queue

    def unsubscribe_sse(self, queue: asyncio.Queue):
        """Remove a queue when SSE client disconnects"""

        self.sse_queues.discard(queue)

        logger.info(f"SSE client unsubscribed. Total clients: {len(self.sse_queues)}")

    async def stop(self):
        """Stop the Kafka consumer gracefully"""

        logger.info("Stopping Kafka consumer...")

        self.running = False

        if self.consumer:
            await asyncio.to_thread(self.consumer.close)

        # Clear all SSE queues
        self.sse_queues.clear()

        logger.info("Kafka consumer stopped")
