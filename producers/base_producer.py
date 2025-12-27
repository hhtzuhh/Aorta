"""
Base Producer for Time-Coordinated Streaming
All producers inherit from this to ensure temporal consistency
"""

import json
import sqlite3
import requests
from pathlib import Path
from typing import Optional, List, Tuple
from confluent_kafka import Producer
from datetime import datetime


class TimeAwareProducer:
    """
    Base class for all time-coordinated producers.

    Producers query the simulation clock service for the current time window,
    fetch events from the database within that window, and stream them to Kafka.
    """

    def __init__(
        self,
        clock_url: str = "http://localhost:9000",
        subject_ids: Optional[list[int]] = None,
        kafka_config_path: str = "_data/kafka_config.json",
        db_path: str = "_data/mimic_demo.db"
    ):
        """
        Initialize the time-aware producer.

        Args:
            clock_url: URL of the simulation clock service
            subject_ids: List of patient IDs to filter by (None = all patients)
            kafka_config_path: Path to Kafka configuration JSON
            db_path: Path to SQLite database
        """
        self.clock_url = clock_url
        self.subject_ids = subject_ids

        # Initialize database connection
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Initialize Kafka producer
        self.kafka_producer = self._create_kafka_producer(kafka_config_path)

        # Topic name - to be set by subclass
        self.topic = None

        print(f"✅ {self.__class__.__name__} initialized")
        if subject_ids:
            print(f"🎯 Filtering by patients: {', '.join(map(str, subject_ids))}")

    def warm_up(self, timeout=10) -> bool:
        """
        Force Kafka connection by sending a test message and waiting for delivery.
        This ensures DNS resolution happens now, not during the first real send.

        Returns:
            True if connection succeeded, False otherwise
        """
        if not self.topic:
            return False

        delivery_success = [False]  # Use list to allow modification in callback

        def callback(err, msg):
            if err:
                print(f"   ❌ Connection test failed: {err}")
            else:
                delivery_success[0] = True

        # Send a small test message
        self.kafka_producer.produce(
            topic=self.topic,
            key="__warmup__",
            value=b'{"test": true}',
            callback=callback
        )

        # Wait for delivery confirmation
        self.kafka_producer.flush(timeout)

        if delivery_success[0]:
            print(f"   ✅ {self.__class__.__name__} connected to Kafka")
        return delivery_success[0]

    def _create_kafka_producer(self, kafka_config_path: str) -> Producer:
        """
        Create Kafka producer from configuration file.

        Args:
            kafka_config_path: Path to Kafka config JSON

        Returns:
            Configured Kafka Producer instance
        """
        if not Path(kafka_config_path).exists():
            raise FileNotFoundError(
                f"Kafka config not found: {kafka_config_path}\n"
                f"Run: terraform output -json kafka_config > {kafka_config_path}"
            )

        with open(kafka_config_path) as f:
            kafka_config = json.load(f)

        producer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'security.protocol': kafka_config['security_protocol'],
            'sasl.mechanism': kafka_config['sasl_mechanism'],
            'sasl.username': kafka_config['sasl_username'],
            'sasl.password': kafka_config['sasl_password'],
            'client.id': f'{self.__class__.__name__.lower()}-producer',
            'log_level': 0,  # Suppress librdkafka debug/error spam
        }

        return Producer(producer_config)

    def get_current_window(self) -> Tuple[str, str]:
        """
        Query the clock service for the current time window.

        Returns:
            Tuple of (window_start, window_end) as ISO format strings

        Raises:
            Exception: If clock service is unreachable
        """
        try:
            response = requests.get(f"{self.clock_url}/current", timeout=5)
            response.raise_for_status()
            data = response.json()
            return data["window_start"], data["window_end"]
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to query clock service: {e}")

    def get_events_in_window(self, window_start: str, window_end: str) -> List:
        """
        Query database for events within the time window.

        This method MUST be implemented by subclasses.

        Args:
            window_start: Start of time window (ISO format)
            window_end: End of time window (ISO format)

        Returns:
            List of database rows as events

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclass must implement get_events_in_window()")

    def format_event(self, db_row) -> dict:
        """
        Convert database row to Kafka event format.

        This method MUST be implemented by subclasses.

        Args:
            db_row: Database row from query

        Returns:
            Formatted event dictionary

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclass must implement format_event()")

    def send_to_kafka(self, event: dict, key: Optional[str] = None):
        """
        Send an event to Kafka.

        Args:
            event: Event dictionary to send
            key: Optional message key (defaults to subject_id if available)
        """
        if not self.topic:
            raise ValueError("Topic not set. Subclass must set self.topic")

        # Default key to subject_id if available in event
        if key is None and "patient" in event and "subject_id" in event["patient"]:
            key = event["patient"]["subject_id"]

        self.kafka_producer.produce(
            topic=self.topic,
            key=str(key) if key else None,
            value=json.dumps(event),
            callback=self._delivery_callback
        )

        # Poll to handle callbacks
        self.kafka_producer.poll(0)

    def _delivery_callback(self, err, msg):
        """
        Kafka delivery callback.

        Args:
            err: Error if delivery failed
            msg: Message metadata if successful
        """
        if err:
            print(f"❌ Delivery failed: {err}")
        # Silent on success to avoid verbose output

    def process_tick(self) -> int:
        """
        Process one clock tick: fetch events in current window and send to Kafka.

        Returns:
            Number of events processed

        Raises:
            Exception: If clock service fails or database errors occur
        """
        # Get current time window from clock service
        window_start, window_end = self.get_current_window()

        # Query database for events in this window
        events = self.get_events_in_window(window_start, window_end)

        # Send each event to Kafka
        for db_row in events:
            event = self.format_event(db_row)
            self.send_to_kafka(event)

        return len(events)

    def flush(self, timeout=10):
        """Flush any pending messages to Kafka"""
        remaining = self.kafka_producer.flush(timeout)
        if remaining > 0:
            print(f"⚠️  {remaining} messages failed to deliver")

    def close(self):
        """Close all connections"""
        self.kafka_producer.flush(5)  # 5 second timeout on close
        self.conn.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
