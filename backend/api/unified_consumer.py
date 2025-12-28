"""
Unified Kafka Consumer for All Topics

Single consumer that handles all 4 topics to avoid DNS overload
from multiple simultaneous connections.
"""

import asyncio
import json
import logging
from collections import deque
from typing import List, Set, Dict
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import Settings
from .models import AdmissionEvent, LabEvent, ICUAdmissionEvent, CharteventEvent, SepsisAlert
from Aorta.ml.streaming.ml_prediction_module import MLPredictionModule

logger = logging.getLogger(__name__)


class UnifiedConsumer:
    """
    Single Kafka consumer for all topics with SSE broadcast capability

    Subscribes to all 4 topics with one connection to avoid DNS overload.
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
            'group.id': 'aorta-unified-consumer-v1',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'log_level': 0,  # Suppress librdkafka logs
        }

        # All topics we consume from (including sepsis-alerts from ML module)
        self.topics = [
            settings.kafka_topic,  # hospital-admissions
            'patient-labs',
            'icu-admissions',
            'patient-vitals',
            'sepsis-alerts',  # NEW: ML prediction alerts
        ]

        self.consumer = None
        self.running = False

        # Circular buffers for each type
        self.recent_admissions: deque = deque(maxlen=settings.max_recent_admissions)
        self.recent_labs: deque = deque(maxlen=200)
        self.recent_icu_admissions: deque = deque(maxlen=100)
        self.recent_chartevents: deque = deque(maxlen=500)
        self.recent_sepsis_alerts: deque = deque(maxlen=100)  # NEW

        # SSE queues for each type
        self.admission_sse_queues: Set[asyncio.Queue] = set()
        self.lab_sse_queues: Set[asyncio.Queue] = set()
        self.icu_sse_queues: Set[asyncio.Queue] = set()
        self.vitals_sse_queues: Set[asyncio.Queue] = set()
        self.sepsis_sse_queues: Set[asyncio.Queue] = set()  # NEW

        # Initialize ML prediction module (optional - won't break if it fails)
        self.ml_module = None
        try:
            kafka_config = {
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'security.protocol': settings.kafka_security_protocol,
                'sasl.mechanism': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
                'log_level': 0
            }
            self.ml_module = MLPredictionModule(
                kafka_config=kafka_config,
                model_dir="ml/models/local",
                db_path="_data/mimic_demo.db",
                tick_interval=3,  # Predict every 3 ticks
                tick_duration_minutes=60  # Default tick duration
            )
            logger.info("✅ UnifiedConsumer initialized with ML prediction module")
        except Exception as e:
            logger.warning(f"⚠️  ML module initialization failed: {e}. Continuing without ML predictions.")
            logger.info("UnifiedConsumer initialized WITHOUT ML prediction module")

    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe(self.topics)
            self.running = True

            logger.info(f"Unified Kafka consumer started. Topics: {self.topics}")

            await self._consume_loop()

        except KafkaException as e:
            logger.error(f"Kafka error in unified consumer: {e}")
            raise

    async def _consume_loop(self):
        """Main consumption loop"""
        logger.info("Starting unified Kafka consumption loop")

        while self.running:
            try:
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    elif msg.error().code() == KafkaError._RESOLVE:
                        logger.debug(f"Kafka DNS resolution error: {msg.error()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Route message based on topic
                topic = msg.topic()
                await self._process_message(topic, msg)

            except Exception as e:
                logger.error(f"Error in unified consumption loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _process_message(self, topic: str, msg):
        """Process message and route to appropriate handler"""
        try:
            event_data = json.loads(msg.value().decode('utf-8'))

            # Skip warm-up test messages
            if event_data.get('test'):
                return

            if topic == self.topics[0]:  # hospital-admissions
                await self._handle_admission(event_data)
            elif topic == 'patient-labs':
                await self._handle_lab(event_data)
            elif topic == 'icu-admissions':
                await self._handle_icu(event_data)
            elif topic == 'patient-vitals':
                await self._handle_vitals(event_data)
            elif topic == 'sepsis-alerts':
                await self._handle_sepsis_alert(event_data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
        except Exception as e:
            logger.error(f"Failed to process message on {topic}: {e}", exc_info=True)

    async def _handle_admission(self, event_data: dict):
        """Handle admission event"""
        admission = AdmissionEvent(**event_data)
        self.recent_admissions.append(admission)

        if admission.is_high_priority:
            logger.info(f"🚨 High-priority admission: {admission.admission.type} - Patient {admission.patient.subject_id}")
        else:
            logger.debug(f"📋 Admission: {admission.admission.type} - Patient {admission.patient.subject_id}")

        await self._broadcast(admission.model_dump(), self.admission_sse_queues)

        # Trigger ML module
        if self.ml_module:
            await self.ml_module.on_admission(event_data)

    async def _handle_lab(self, event_data: dict):
        """Handle lab event"""
        lab = LabEvent(**event_data)
        self.recent_labs.append(lab)

        if lab.is_abnormal:
            logger.info(f"🔬 Abnormal lab: {lab.lab.test_name} = {lab.lab.value_numeric} {lab.lab.unit} - Patient {lab.patient.subject_id}")
        else:
            logger.debug(f"🧪 Lab: {lab.lab.test_name} - Patient {lab.patient.subject_id}")

        await self._broadcast(lab.model_dump(), self.lab_sse_queues)

        # Trigger ML module
        if self.ml_module:
            await self.ml_module.on_lab(event_data)

    async def _handle_icu(self, event_data: dict):
        """Handle ICU admission event"""
        icu = ICUAdmissionEvent(**event_data)
        self.recent_icu_admissions.append(icu)

        logger.info(f"🏥 ICU admission: {icu.icu_stay.first_careunit} - Patient {icu.patient.subject_id}")

        await self._broadcast(icu.model_dump(), self.icu_sse_queues)

        # Trigger ML module
        if self.ml_module:
            await self.ml_module.on_icu(event_data)

    async def _handle_vitals(self, event_data: dict):
        """Handle vitals/chartevent"""
        chartevent = CharteventEvent(**event_data)
        self.recent_chartevents.append(chartevent)

        logger.debug(f"📊 Chartevent: {chartevent.chartevent.label} - Patient {chartevent.patient.subject_id}")

        await self._broadcast(chartevent.model_dump(), self.vitals_sse_queues)

        # Trigger ML module
        if self.ml_module:
            await self.ml_module.on_vitals(event_data)

    async def _handle_sepsis_alert(self, event_data: dict):
        """Handle sepsis alert event from ML module"""
        alert = SepsisAlert(**event_data)
        self.recent_sepsis_alerts.append(alert)

        if alert.is_critical:
            logger.warning(f"🚨 CRITICAL sepsis alert: {alert.patient.subject_id} - {alert.prediction.sepsis_probability:.2%}")
        else:
            logger.info(f"⚠️  Sepsis alert: {alert.patient.subject_id} - {alert.prediction.sepsis_probability:.2%}")

        await self._broadcast(alert.model_dump(), self.sepsis_sse_queues)

    async def _broadcast(self, event_dict: dict, queues: Set[asyncio.Queue]):
        """Broadcast event to SSE clients"""
        if not queues:
            return

        dead_queues = set()
        for queue in queues:
            try:
                queue.put_nowait(event_dict)
            except asyncio.QueueFull:
                logger.warning("SSE queue full, dropping event")
            except Exception as e:
                logger.error(f"Failed to broadcast: {e}")
                dead_queues.add(queue)

        queues -= dead_queues

    # Subscription methods for SSE
    def subscribe_admissions(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.admission_sse_queues.add(queue)
        logger.info(f"SSE client subscribed to admissions. Total: {len(self.admission_sse_queues)}")
        return queue

    def unsubscribe_admissions(self, queue: asyncio.Queue):
        self.admission_sse_queues.discard(queue)
        logger.info(f"SSE client unsubscribed from admissions. Total: {len(self.admission_sse_queues)}")

    def subscribe_labs(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.lab_sse_queues.add(queue)
        logger.info(f"SSE client subscribed to labs. Total: {len(self.lab_sse_queues)}")
        return queue

    def unsubscribe_labs(self, queue: asyncio.Queue):
        self.lab_sse_queues.discard(queue)
        logger.info(f"SSE client unsubscribed from labs. Total: {len(self.lab_sse_queues)}")

    def subscribe_icu(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.icu_sse_queues.add(queue)
        logger.info(f"SSE client subscribed to ICU. Total: {len(self.icu_sse_queues)}")
        return queue

    def unsubscribe_icu(self, queue: asyncio.Queue):
        self.icu_sse_queues.discard(queue)
        logger.info(f"SSE client unsubscribed from ICU. Total: {len(self.icu_sse_queues)}")

    def subscribe_vitals(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.vitals_sse_queues.add(queue)
        logger.info(f"SSE client subscribed to vitals. Total: {len(self.vitals_sse_queues)}")
        return queue

    def unsubscribe_vitals(self, queue: asyncio.Queue):
        self.vitals_sse_queues.discard(queue)
        logger.info(f"SSE client unsubscribed from vitals. Total: {len(self.vitals_sse_queues)}")

    def subscribe_sepsis_alerts(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.sepsis_sse_queues.add(queue)
        logger.info(f"SSE client subscribed to sepsis alerts. Total: {len(self.sepsis_sse_queues)}")
        return queue

    def unsubscribe_sepsis_alerts(self, queue: asyncio.Queue):
        self.sepsis_sse_queues.discard(queue)
        logger.info(f"SSE client unsubscribed from sepsis alerts. Total: {len(self.sepsis_sse_queues)}")

    # Getter methods for recent data
    def get_recent_admissions(self) -> List[dict]:
        return [a.model_dump() for a in reversed(self.recent_admissions)]

    def get_recent_labs(self) -> List[dict]:
        return [l.model_dump() for l in reversed(self.recent_labs)]

    def get_recent_icu_admissions(self) -> List[dict]:
        return [i.model_dump() for i in reversed(self.recent_icu_admissions)]

    def get_recent_chartevents(self) -> List[dict]:
        return [c.model_dump() for c in reversed(self.recent_chartevents)]

    def get_recent_sepsis_alerts(self) -> List[dict]:
        return [s.model_dump() for s in reversed(self.recent_sepsis_alerts)]

    async def stop(self):
        """Stop the consumer"""
        logger.info("Stopping unified Kafka consumer...")
        self.running = False

        # Flush ML module
        if self.ml_module:
            self.ml_module.flush()

        if self.consumer:
            await asyncio.to_thread(self.consumer.close)

        # Clear all queues
        self.admission_sse_queues.clear()
        self.lab_sse_queues.clear()
        self.icu_sse_queues.clear()
        self.vitals_sse_queues.clear()
        self.sepsis_sse_queues.clear()

        logger.info("Unified Kafka consumer stopped")
