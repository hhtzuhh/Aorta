#!/usr/bin/env python3
"""
Aorta - Hospital Admission Streaming
Stream MIMIC-IV admissions to Confluent Cloud Kafka

Usage:
    python stream_admissions.py [--max-events 50] [--speed 10]
"""

import sqlite3
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from confluent_kafka import Producer


class AdmissionStreamer:
    """Stream hospital admissions from SQLite to Kafka"""

    def __init__(self, db_path="../_data/mimic_demo.db", kafka_config_path="../_data/kafka_config.json"):
        # Load database
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Load Kafka config
        if not Path(kafka_config_path).exists():
            raise FileNotFoundError(f"Kafka config not found: {kafka_config_path}\nRun: terraform output -json kafka_config > {kafka_config_path}")

        with open(kafka_config_path) as f:
            kafka_config = json.load(f)

        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'security.protocol': kafka_config['security_protocol'],
            'sasl.mechanism': kafka_config['sasl_mechanism'],
            'sasl.username': kafka_config['sasl_username'],
            'sasl.password': kafka_config['sasl_password'],
            'client.id': 'aorta-admission-producer',
        }

        self.producer = Producer(producer_config)
        self.topic = "hospital-admissions"

        print("✅ Connected to Kafka cluster")
        print(f"📡 Bootstrap: {kafka_config['bootstrap_servers']}")
        print(f"📝 Topic: {self.topic}\n")

    def get_admissions(self, limit=None):
        """Get all admissions ordered by admission time"""

        query = """
            SELECT
                a.subject_id,
                a.hadm_id,
                a.admittime,
                a.dischtime,
                a.admission_type,
                a.admission_location,
                a.discharge_location,
                a.insurance,
                a.language,
                a.marital_status,
                a.race,
                p.gender,
                p.anchor_age
            FROM admissions a
            JOIN patients p ON a.subject_id = p.subject_id
            WHERE a.admittime IS NOT NULL
            ORDER BY a.admittime
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def format_admission_event(self, admission):
        """Convert database row to Kafka event"""

        return {
            "event_type": "ADMISSION",
            "timestamp": admission['admittime'],
            "patient": {
                "subject_id": str(admission['subject_id']),
                "age": admission['anchor_age'],
                "gender": admission['gender'],
            },
            "admission": {
                "hadm_id": str(admission['hadm_id']),
                "type": admission['admission_type'],
                "location": admission['admission_location'],
                "insurance": admission['insurance'],
                "marital_status": admission['marital_status'] or "UNKNOWN",
                "language": admission['language'] or "UNKNOWN",
                "race": admission['race'] or "UNKNOWN"
            },
            "discharge": {
                "time": admission['dischtime'],
                "location": admission['discharge_location']
            }
        }

    def is_high_priority(self, admission):
        """Check if admission needs immediate attention"""
        emergency_types = ['EMERGENCY', 'URGENT', 'EW EMER.']
        return admission['admission_type'] in emergency_types

    def delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            print(f"❌ Delivery failed: {err}")
        else:
            # Silently succeed (too verbose to print every success)
            pass

    def stream_admissions(self, replay_speed=10, max_events=50):
        """
        Stream admissions to Kafka in real-time

        Args:
            replay_speed: How many times faster than real-time (10 = 10x speed)
            max_events: Maximum number of admissions to stream
        """

        print("=" * 70)
        print("🏥 AORTA - HOSPITAL ADMISSION STREAM")
        print("=" * 70)
        print(f"⚡ Replay speed: {replay_speed}x")
        print(f"📊 Max events: {max_events}")
        print(f"🎯 Streaming to topic: {self.topic}")
        print("\nStarting stream in chronological order...\n")

        admissions = self.get_admissions(limit=max_events)

        if not admissions:
            print("❌ No admissions found!")
            return

        prev_time = None
        events_sent = 0
        high_priority_count = 0

        for admission in admissions:
            # Format event
            event = self.format_admission_event(admission)

            # Calculate delay (simulate real-time)
            if prev_time:
                real_delay = self._time_diff_seconds(prev_time, admission['admittime'])
                simulated_delay = real_delay / replay_speed
                simulated_delay = min(simulated_delay, 2.0)  # Max 2 sec delay

                if simulated_delay > 0.01:
                    time.sleep(simulated_delay)

            prev_time = admission['admittime']

            # Send to Kafka
            self.producer.produce(
                topic=self.topic,
                key=str(admission['hadm_id']),
                value=json.dumps(event),
                callback=self.delivery_callback
            )

            # Poll to handle callbacks
            self.producer.poll(0)

            # Console output
            events_sent += 1
            priority_flag = "🚨" if self.is_high_priority(admission) else "📋"

            print(f"{priority_flag} [{events_sent:3d}/{max_events}] "
                  f"{admission['admittime']} | "
                  f"{admission['admission_type']:15s} | "
                  f"Patient {admission['subject_id']} → Admission {admission['hadm_id']}")

            if self.is_high_priority(admission):
                high_priority_count += 1
                print(f"   ⚠️  HIGH PRIORITY: {admission['admission_type']} from "
                      f"{admission['admission_location']}")

        # Flush remaining messages
        print("\n⏳ Flushing remaining messages to Kafka...")
        self.producer.flush()

        # Summary
        print("\n" + "=" * 70)
        print("📊 STREAM SUMMARY")
        print("=" * 70)
        print(f"Total admissions streamed: {events_sent}")
        print(f"High-priority admissions: {high_priority_count}")
        print(f"Priority rate: {high_priority_count/events_sent*100:.1f}%")
        print(f"Topic: {self.topic}")
        print("\n✅ SUCCESS! All events sent to Kafka")
        print("\n💡 Next steps:")
        print("   1. Check Confluent Cloud console to see messages")
        print("   2. Create Flink SQL job to process the stream")
        print("   3. Generate alerts for emergency admissions")

    def _time_diff_seconds(self, time1, time2):
        """Calculate seconds between two timestamp strings"""
        try:
            t1 = datetime.strptime(time1, "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(time2, "%Y-%m-%d %H:%M:%S")
            return abs((t2 - t1).total_seconds())
        except:
            return 0.1

    def close(self):
        """Close connections"""
        self.producer.flush()
        self.conn.close()


def main():
    """Run the admission streamer"""

    parser = argparse.ArgumentParser(description='Stream MIMIC-IV admissions to Kafka')
    parser.add_argument('--max-events', type=int, default=50,
                        help='Maximum number of admissions to stream (default: 50)')
    parser.add_argument('--speed', type=int, default=10,
                        help='Replay speed multiplier (default: 10x)')

    args = parser.parse_args()

    streamer = AdmissionStreamer()

    try:
        streamer.stream_admissions(
            replay_speed=args.speed,
            max_events=args.max_events
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Stream interrupted by user")
    finally:
        streamer.close()


if __name__ == "__main__":
    main()
