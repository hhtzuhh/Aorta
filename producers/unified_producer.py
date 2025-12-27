"""
Unified Producer for All Topics

Single Kafka producer that handles all 4 topics to avoid DNS overload
from multiple simultaneous connections.
"""

import json
import sqlite3
import requests
from pathlib import Path
from typing import Optional, List, Tuple
from confluent_kafka import Producer
from datetime import datetime


class UnifiedProducer:
    """
    Single Kafka producer for all topics.

    Uses one connection to avoid DNS overload on Mac.
    """

    def __init__(
        self,
        clock_url: str = "http://localhost:9000",
        subject_ids: Optional[list[int]] = None,
        kafka_config_path: str = "_data/kafka_config.json",
        db_path: str = "_data/mimic_demo.db"
    ):
        self.clock_url = clock_url
        self.subject_ids = subject_ids

        # Validate subject_ids for vitals (required to prevent data overload)
        if not subject_ids:
            raise ValueError("UnifiedProducer requires subject_ids (recommend 2-3 patients)")

        # Initialize database
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Initialize single Kafka producer
        self.producer = self._create_producer(kafka_config_path)

        # Topic names
        self.topics = {
            'admissions': 'hospital-admissions',
            'labs': 'patient-labs',
            'icu': 'icu-admissions',
            'vitals': 'patient-vitals',
        }

        print(f"✅ UnifiedProducer initialized")
        print(f"🎯 Filtering by patients: {', '.join(map(str, subject_ids))}")

    def _create_producer(self, kafka_config_path: str) -> Producer:
        """Create single Kafka producer"""
        if not Path(kafka_config_path).exists():
            raise FileNotFoundError(f"Kafka config not found: {kafka_config_path}")

        with open(kafka_config_path) as f:
            config = json.load(f)

        return Producer({
            'bootstrap.servers': config['bootstrap_servers'],
            'security.protocol': config['security_protocol'],
            'sasl.mechanism': config['sasl_mechanism'],
            'sasl.username': config['sasl_username'],
            'sasl.password': config['sasl_password'],
            'client.id': 'unified-producer',
            'log_level': 0,
        })

    def warm_up(self, timeout=10) -> bool:
        """Test connection by sending to one topic"""
        delivery_success = [False]

        def callback(err, msg):
            if err:
                print(f"   ❌ Connection test failed: {err}")
            else:
                delivery_success[0] = True

        self.producer.produce(
            topic=self.topics['admissions'],
            key="__warmup__",
            value=b'{"test": true}',
            callback=callback
        )
        self.producer.flush(timeout)

        if delivery_success[0]:
            print(f"   ✅ UnifiedProducer connected to Kafka")
        return delivery_success[0]

    def get_current_window(self) -> Tuple[str, str]:
        """Get current time window from clock service"""
        response = requests.get(f"{self.clock_url}/current", timeout=5)
        response.raise_for_status()
        data = response.json()
        return data["window_start"], data["window_end"]

    def _send(self, topic: str, event: dict, key: Optional[str] = None):
        """Send event to specified topic"""
        if key is None and "patient" in event:
            key = event["patient"].get("subject_id")

        self.producer.produce(
            topic=topic,
            key=str(key) if key else None,
            value=json.dumps(event),
            callback=self._delivery_callback
        )
        self.producer.poll(0)

    def _delivery_callback(self, err, msg):
        if err:
            print(f"❌ Delivery failed: {err}")

    # === Admission queries and formatting ===
    def _get_admissions(self, window_start: str, window_end: str) -> List:
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
        return self.conn.execute(query, params).fetchall()

    def _format_admission(self, row) -> dict:
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

    # === Lab queries and formatting ===
    def _get_labs(self, window_start: str, window_end: str) -> List:
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
        return self.conn.execute(query, params).fetchall()

    def _format_lab(self, row) -> dict:
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

    # === ICU queries and formatting ===
    def _get_icu_stays(self, window_start: str, window_end: str) -> List:
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
        return self.conn.execute(query, params).fetchall()

    def _format_icu(self, row) -> dict:
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

    # === Vitals queries and formatting ===
    def _get_vitals(self, window_start: str, window_end: str) -> List:
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
        return self.conn.execute(query, params).fetchall()

    def _format_vitals(self, row) -> dict:
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

    def process_tick(self) -> dict:
        """Process one tick for all event types"""
        window_start, window_end = self.get_current_window()

        counts = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

        # Admissions
        for row in self._get_admissions(window_start, window_end):
            self._send(self.topics['admissions'], self._format_admission(row))
            counts['admissions'] += 1

        # Labs
        for row in self._get_labs(window_start, window_end):
            self._send(self.topics['labs'], self._format_lab(row))
            counts['labs'] += 1

        # ICU
        for row in self._get_icu_stays(window_start, window_end):
            self._send(self.topics['icu'], self._format_icu(row))
            counts['icu'] += 1

        # Vitals
        for row in self._get_vitals(window_start, window_end):
            self._send(self.topics['vitals'], self._format_vitals(row))
            counts['vitals'] += 1

        return counts

    def flush(self, timeout=10):
        """Flush pending messages"""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            print(f"⚠️  {remaining} messages failed to deliver")

    def close(self):
        """Close connections"""
        self.producer.flush(5)
        self.conn.close()
