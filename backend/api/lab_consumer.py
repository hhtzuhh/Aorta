"""
Kafka Consumer for Lab Events with SSE Broadcasting

Consumes patient lab events from Kafka and broadcasts
to connected SSE clients in real-time.
"""

import asyncio
import json
import logging
from collections import deque
from typing import List, Set
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import Settings
from .models import LabEvent

logger = logging.getLogger(__name__)


class LabConsumer:
    """
    Kafka consumer for patient lab events with SSE broadcast capability

    Features:
    - Consumes from patient-labs topic
    - Maintains circular buffer of recent labs
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
            'group.id': 'aorta-lab-consumer-v1',
            'auto.offset.reset': 'earliest',  # Start from beginning for testing
            'enable.auto.commit': True,
            'log_level': 0,  # Suppress librdkafka debug logs
        }

        self.consumer = None
        self.running = False

        # Circular buffer for recent labs (more frequent than admissions)
        self.recent_labs: deque = deque(maxlen=200)

        # Set of asyncio queues for SSE clients
        self.sse_queues: Set[asyncio.Queue] = set()

        logger.info("LabConsumer initialized")

    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe(["patient-labs"])
            self.running = True

            logger.info(
                f"Kafka lab consumer started. Topic: patient-labs, "
                f"Group: aorta-lab-consumer-v1"
            )

            # Start consumption loop
            await self._consume_loop()

        except KafkaException as e:
            logger.error(f"Kafka error in lab consumer: {e}")
            raise

    async def _consume_loop(self):
        """Main consumption loop - runs in background task"""

        logger.info("Starting Kafka lab consumption loop")

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
                logger.error(f"Error in lab consumption loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause before retry

    async def _process_message(self, msg):
        """Process a single Kafka message"""

        try:
            # Parse JSON message
            event_data = json.loads(msg.value().decode('utf-8'))

            # Skip warm-up test messages from producers
            if event_data.get('test'):
                return

            # Validate and create LabEvent
            lab = LabEvent(**event_data)

            # Add to circular buffer
            self.recent_labs.append(lab)

            # Log abnormal lab results
            if lab.is_abnormal:
                logger.info(
                    f"🔬 Abnormal lab: {lab.lab.test_name} = {lab.lab.value_numeric} {lab.lab.unit} - "
                    f"Patient {lab.patient.subject_id}"
                )
            else:
                logger.debug(
                    f"🧪 Lab: {lab.lab.test_name} = {lab.lab.value_numeric} {lab.lab.unit} - "
                    f"Patient {lab.patient.subject_id}"
                )

            # Broadcast to all SSE clients
            await self._broadcast_to_sse(lab)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Failed to process lab message: {e}", exc_info=True)

    async def _broadcast_to_sse(self, lab: LabEvent):
        """Broadcast lab event to all connected SSE clients"""

        if not self.sse_queues:
            return  # No clients connected

        # Convert to dict for JSON serialization
        event_dict = lab.model_dump()

        # Send to all queues
        dead_queues = set()

        for queue in self.sse_queues:
            try:
                # Non-blocking put
                queue.put_nowait(event_dict)
            except asyncio.QueueFull:
                logger.warning("SSE queue full, dropping lab event")
            except Exception as e:
                logger.error(f"Failed to broadcast lab to queue: {e}")
                dead_queues.add(queue)

        # Remove dead queues
        self.sse_queues -= dead_queues

        logger.debug(f"Broadcasted lab to {len(self.sse_queues)} SSE clients")

    def get_recent_labs(self) -> List[dict]:
        """
        Get recent labs from circular buffer

        Returns:
            List of lab events as dictionaries (newest first)
        """
        # Convert deque to list and reverse (newest first)
        return [lab.model_dump() for lab in reversed(self.recent_labs)]

    def subscribe_sse(self) -> asyncio.Queue:
        """
        Create a new queue for an SSE client

        Returns:
            asyncio.Queue for receiving lab events
        """
        queue = asyncio.Queue(maxsize=100)
        self.sse_queues.add(queue)

        logger.info(f"New SSE client subscribed to labs. Total clients: {len(self.sse_queues)}")

        return queue

    def unsubscribe_sse(self, queue: asyncio.Queue):
        """Remove a queue when SSE client disconnects"""

        self.sse_queues.discard(queue)

        logger.info(f"SSE client unsubscribed from labs. Total clients: {len(self.sse_queues)}")

    async def stop(self):
        """Stop the Kafka consumer gracefully"""

        logger.info("Stopping Kafka lab consumer...")

        self.running = False

        if self.consumer:
            await asyncio.to_thread(self.consumer.close)

        # Clear all SSE queues
        self.sse_queues.clear()

        logger.info("Kafka lab consumer stopped")
