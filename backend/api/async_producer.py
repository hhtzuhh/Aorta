"""
Async Unified Producer for All Topics

Async version of unified_producer.py that runs as a background task
within the FastAPI backend instead of a subprocess.

Key differences from sync version:
- Uses aiosqlite instead of sqlite3
- Direct clock access instead of HTTP calls
- Async database queries
- Kafka producer operations wrapped in executor
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional, List, Tuple
from datetime import datetime
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class AsyncUnifiedProducer:
    """
    Async single Kafka producer for all topics.

    Uses one connection to avoid DNS overload.
    Runs as async task within FastAPI backend.
    """

    def __init__(
        self,
        subject_ids: Optional[list[int]] = None,
        kafka_config_path: str = "_data/kafka_config.json",
        db_path: str = "_data/mimic_demo.db"
    ):
        self.subject_ids = subject_ids

        # Convert to absolute path to avoid working directory issues in async context
        db_path_obj = Path(db_path)
        if not db_path_obj.is_absolute():
            # Resolve relative to project root (parent of backend/)
            project_root = Path(__file__).parent.parent.parent
            db_path_obj = project_root / db_path

        self.db_path = str(db_path_obj.resolve())
        self.db_connection = None  # Persistent connection, opened in warm_up()

        # Validate subject_ids
        if not subject_ids:
            raise ValueError("AsyncUnifiedProducer requires subject_ids (recommend 2-3 patients)")

        # Validate database exists
        if not db_path_obj.exists():
            raise FileNotFoundError(f"Database not found: {db_path_obj}")

        logger.info(f"📊 Database path resolved to: {self.db_path}")
        logger.info(f"📊 Database file size: {db_path_obj.stat().st_size / 1024 / 1024:.1f} MB")

        # Initialize single Kafka producer
        self.producer = self._create_producer(kafka_config_path)

        # Topic names
        self.topics = {
            'admissions': 'hospital-admissions',
            'labs': 'patient-labs',
            'icu': 'icu-admissions',
            'vitals': 'patient-vitals',
        }

        logger.info("✅ AsyncUnifiedProducer initialized")
        logger.info(f"🎯 Filtering by patients: {', '.join(map(str, subject_ids))}")

    def _create_producer(self, kafka_config_path: str) -> Producer:
        """Create single Kafka producer with env var fallback"""
        # Try environment variables first (production)
        bootstrap_servers = os.getenv('AORTA_KAFKA_BOOTSTRAP_SERVERS')

        if bootstrap_servers:
            # Load from environment variables
            logger.info("📝 Loading Kafka config from ENVIRONMENT VARIABLES")
            config = {
                'bootstrap_servers': bootstrap_servers,
                'sasl_username': os.getenv('AORTA_KAFKA_SASL_USERNAME'),
                'sasl_password': os.getenv('AORTA_KAFKA_SASL_PASSWORD'),
                'sasl_mechanism': os.getenv('AORTA_KAFKA_SASL_MECHANISM', 'PLAIN'),
                'security_protocol': os.getenv('AORTA_KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
            }
        else:
            # Fallback to JSON file (development)
            logger.info(f"📝 Loading Kafka config from JSON FILE: {kafka_config_path}")
            if not Path(kafka_config_path).exists():
                raise FileNotFoundError(f"Kafka config not found: {kafka_config_path}")

            with open(kafka_config_path) as f:
                config = json.load(f)

        logger.info("🔧 Creating Kafka producer...")
        logger.info(f"   Bootstrap servers: {config['bootstrap_servers']}")
        logger.info(f"   Security protocol: {config['security_protocol']}")
        logger.info(f"   SASL mechanism: {config['sasl_mechanism']}")
        logger.info(f"   SASL username: {config['sasl_username'][:5]}***")

        return Producer({
            'bootstrap.servers': config['bootstrap_servers'],
            'security.protocol': config['security_protocol'],
            'sasl.mechanism': config['sasl_mechanism'],
            'sasl.username': config['sasl_username'],
            'sasl.password': config['sasl_password'],
            'client.id': 'async-unified-producer',
            'socket.timeout.ms': 60000,  # Increase socket timeout
            'log_level': 0,  # Suppress librdkafka DNS spam
        })

    async def warm_up(self, timeout=30) -> bool:
        """Test connection by sending to one topic and open persistent DB connection"""
        import aiosqlite

        # Open persistent database connection
        logger.info("📊 Opening persistent database connection...")
        self.db_connection = await aiosqlite.connect(self.db_path)
        self.db_connection.row_factory = aiosqlite.Row
        logger.info("✅ Database connection opened")

        logger.info(f"🔥 Testing Kafka connection (timeout={timeout}s)...")
        delivery_success = [False]
        error_msg = [None]

        def callback(err, msg):
            if err:
                error_msg[0] = str(err)
                logger.error(f"   ❌ Connection test failed: {err}")
            else:
                delivery_success[0] = True
                logger.info(f"   ✅ Test message delivered to {msg.topic()}")

        try:
            # Call produce directly - no threading to avoid librdkafka issues
            self.producer.produce(
                topic=self.topics['admissions'],
                value=b'{"test": true}',
                key="__warmup__",
                callback=callback
            )

            logger.info("   ⏳ Flushing producer (waiting for delivery)...")
            self.producer.flush(timeout)
        except Exception as e:
            logger.error(f"   ❌ Producer error: {e}")
            return False

        if delivery_success[0]:
            logger.info("   ✅ AsyncUnifiedProducer connected to Kafka")
        else:
            logger.warning(f"   ⚠️  Warm-up timeout or failed (error: {error_msg[0]})")
        return delivery_success[0]

    async def _send(self, topic: str, event: dict, key: Optional[str] = None):
        """Send event to specified topic - direct call, no threading"""
        if key is None and "patient" in event:
            key = event["patient"].get("subject_id")

        # Call produce directly - it's non-blocking
        # Using threading caused librdkafka DNS issues
        self.producer.produce(
            topic=topic,
            key=str(key) if key else None,
            value=json.dumps(event),
            callback=self._delivery_callback
        )
        self.producer.poll(0)  # Service delivery callbacks

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"❌ Delivery failed: {err}")

    # === Async database query methods ===

    async def _get_admissions(self, window_start: str, window_end: str) -> List:
        """Query admissions from database (async) using persistent connection"""
        query = """
            SELECT
                a.subject_id, a.hadm_id, a.admittime, a.dischtime,
                a.admission_type, a.admit_provider_id,
                a.admission_location, a.discharge_location,
                a.insurance, a.language, a.marital_status, a.race,
                a.hospital_expire_flag,
                p.gender, p.anchor_age
            FROM admissions a
            JOIN patients p ON a.subject_id = p.subject_id
            WHERE a.admittime >= ? AND a.admittime < ?
        """
        params = [window_start, window_end]
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND a.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)
        query += " ORDER BY a.admittime"

        # Use persistent connection
        async with self.db_connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _get_labs(self, window_start: str, window_end: str) -> List:
        """Query labs from database (async) using persistent connection"""
        query = """
            SELECT
                l.labevent_id, l.subject_id, l.hadm_id, l.specimen_id,
                l.charttime, l.storetime,
                l.itemid, l.value, l.valuenum, l.valueuom,
                l.ref_range_lower, l.ref_range_upper, l.flag,
                d.label as test_name, d.fluid, d.category
            FROM labevents l
            JOIN d_labitems d ON l.itemid = d.itemid
            WHERE l.charttime >= ? AND l.charttime < ?
        """
        params = [window_start, window_end]
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND l.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)
        query += " ORDER BY l.charttime"

        # Use persistent connection
        async with self.db_connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _get_icu_stays(self, window_start: str, window_end: str) -> List:
        """Query ICU stays from database (async) using persistent connection"""

        query = """
            SELECT
                i.subject_id, i.hadm_id, i.stay_id,
                i.first_careunit, i.last_careunit,
                i.intime, i.outtime, i.los
            FROM icustays i
            WHERE i.intime >= ? AND i.intime < ?
        """
        params = [window_start, window_end]
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND i.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)
        query += " ORDER BY i.intime"

        # Use persistent connection
        async with self.db_connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _get_vitals(self, window_start: str, window_end: str) -> List:
        """Query vitals from database (async) using persistent connection"""

        query = """
            SELECT
                c.subject_id, c.hadm_id, c.stay_id,
                c.charttime, c.storetime, c.itemid,
                c.value, c.valuenum, c.valueuom, c.warning,
                d.label, d.category, d.unitname, d.param_type,
                i.first_careunit, i.last_careunit, i.los
            FROM chartevents c
            JOIN d_items d ON c.itemid = d.itemid
            LEFT JOIN icustays i ON c.stay_id = i.stay_id
            WHERE c.charttime >= ? AND c.charttime < ?
              AND c.warning = '0'
        """
        params = [window_start, window_end]
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND c.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)
        query += " ORDER BY c.charttime"

        # Use persistent connection
        async with self.db_connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # === Event formatting (same as sync version) ===

    def _format_admission(self, row: dict) -> dict:
        return {
            "event_type": "ADMISSION",
            "event_time": row['admittime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {
                "subject_id": str(row['subject_id']),
                "gender": row['gender'],
                "age": row['anchor_age'],
            },
            "admission": {
                "hadm_id": str(row['hadm_id']),
                "type": row['admission_type'],
                "location": row['admission_location'],
                "insurance": row['insurance'],
                "language": row['language'] or "Unknown",
                "marital_status": row['marital_status'] or "Unknown",
            },
            "discharge": {
                "time": row['dischtime'],
                "location": row['discharge_location'],
            },
            "is_high_priority": row['admission_type'] in ['EMERGENCY', 'URGENT'],
        }

    def _format_lab(self, row: dict) -> dict:
        is_abnormal = row['flag'] in ['abnormal', 'delta'] if row['flag'] else False
        return {
            "event_type": "LAB_RESULT",
            "event_time": row['charttime'],
            "store_time": row['storetime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {"subject_id": str(row['subject_id'])},
            "admission": {"hadm_id": str(row['hadm_id']) if row['hadm_id'] else None},
            "lab": {
                "labevent_id": str(row['labevent_id']),
                "specimen_id": str(row['specimen_id']),
                "itemid": row['itemid'],
                "test_name": row['test_name'],
                "value_numeric": float(row['valuenum']) if row['valuenum'] else None,
                "unit": row['valueuom'],
                "ref_range_lower": float(row['ref_range_lower']) if row['ref_range_lower'] else None,
                "ref_range_upper": float(row['ref_range_upper']) if row['ref_range_upper'] else None,
                "flag": row['flag'],
                "category": row['category'],
            },
            "is_abnormal": is_abnormal,
        }

    def _format_icu(self, row: dict) -> dict:
        return {
            "event_type": "ICU_ADMISSION",
            "event_time": row['intime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {"subject_id": str(row['subject_id'])},
            "admission": {"hadm_id": str(row['hadm_id']) if row['hadm_id'] else None},
            "icu_stay": {
                "stay_id": str(row['stay_id']),
                "first_careunit": row['first_careunit'] or "Unknown",
                "last_careunit": row['last_careunit'] or "Unknown",
                "intime": row['intime'],
                "outtime": row['outtime'],
                "los_days": float(row['los']) if row['los'] else None,
                "status": "DISCHARGED" if row['outtime'] else "ACTIVE",
                "is_transfer": row['first_careunit'] != row['last_careunit'],
            }
        }

    def _format_vitals(self, row: dict) -> dict:
        return {
            "event_type": "CHARTEVENT",
            "event_time": row['charttime'],
            "store_time": row['storetime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {"subject_id": str(row['subject_id'])},
            "admission": {"hadm_id": str(row['hadm_id']) if row['hadm_id'] else None},
            "icu_stay": {
                "stay_id": str(row['stay_id']) if row['stay_id'] else None,
                "first_careunit": row['first_careunit'] or "Unknown",
                "last_careunit": row['last_careunit'] or "Unknown",
                "los_days": float(row['los']) if row['los'] else None,
            },
            "chartevent": {
                "itemid": row['itemid'],
                "label": row['label'] or f"Unknown (ID: {row['itemid']})",
                "category": row['category'],
                "param_type": row['param_type'],
                "value_text": row['value'],
                "value_numeric": float(row['valuenum']) if row['valuenum'] else None,
                "unit": row['valueuom'] or row['unitname'],
                "warning": int(row['warning']),
            }
        }

    async def _process_admissions(self, window_start: str, window_end: str) -> int:
        """Process admissions for current window"""
        rows = await self._get_admissions(window_start, window_end)
        for row in rows:
            event = self._format_admission(row)
            await self._send(self.topics['admissions'], event)
        return len(rows)

    async def _process_labs(self, window_start: str, window_end: str) -> int:
        """Process labs for current window"""
        rows = await self._get_labs(window_start, window_end)
        for row in rows:
            event = self._format_lab(row)
            await self._send(self.topics['labs'], event)
        return len(rows)

    async def _process_icu(self, window_start: str, window_end: str) -> int:
        """Process ICU stays for current window"""
        rows = await self._get_icu_stays(window_start, window_end)
        for row in rows:
            event = self._format_icu(row)
            await self._send(self.topics['icu'], event)
        return len(rows)

    async def _process_vitals(self, window_start: str, window_end: str) -> int:
        """Process vitals for current window"""
        rows = await self._get_vitals(window_start, window_end)
        for row in rows:
            event = self._format_vitals(row)
            await self._send(self.topics['vitals'], event)
        return len(rows)

    async def process_tick(self, clock) -> dict:
        """
        Process one tick for all event types.

        Args:
            clock: SimulationClock instance (direct access, no HTTP!)

        Returns:
            dict with counts for each event type
        """
        # Get current window from clock (direct function call, not HTTP!)
        window_start, window_end = clock.get_current_window()

        # Process event types sequentially to avoid SQLite file locking issues
        # (SQLite doesn't handle concurrent connections well on some systems)
        admissions_count = await self._process_admissions(window_start, window_end)
        labs_count = await self._process_labs(window_start, window_end)
        icu_count = await self._process_icu(window_start, window_end)
        vitals_count = await self._process_vitals(window_start, window_end)

        return {
            'admissions': admissions_count,
            'labs': labs_count,
            'icu': icu_count,
            'vitals': vitals_count
        }

    async def flush(self, timeout=10):
        """Flush pending messages - direct call"""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages failed to deliver")

    async def close(self):
        """Close connections"""
        self.producer.flush(5)

        # Close persistent database connection
        if self.db_connection:
            await self.db_connection.close()
            logger.info("✅ Database connection closed")

        logger.info("✅ AsyncUnifiedProducer closed")
