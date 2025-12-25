"""
Time-Coordinated Admission Producer
Streams hospital admissions in sync with simulation clock
"""

from typing import List
from datetime import datetime

from .base_producer import TimeAwareProducer


class AdmissionProducer(TimeAwareProducer):
    """
    Producer for hospital admission events.

    Queries admissions from the database within the current time window
    and streams them to the hospital-admissions Kafka topic.
    """

    def __init__(self, **kwargs):
        """
        Initialize the admission producer.

        Args:
            **kwargs: Passed to TimeAwareProducer (clock_url, subject_id, etc.)
        """
        super().__init__(**kwargs)
        self.topic = "hospital-admissions"

    def get_events_in_window(self, window_start: str, window_end: str) -> List:
        """
        Query admissions within the time window.

        Args:
            window_start: Start of time window (YYYY-MM-DD HH:MM:SS)
            window_end: End of time window (YYYY-MM-DD HH:MM:SS)

        Returns:
            List of admission records
        """
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
                p.gender,
                p.anchor_age
            FROM admissions a
            JOIN patients p ON a.subject_id = p.subject_id
            WHERE a.admittime >= ? AND a.admittime < ?
        """

        params = [window_start, window_end]

        # Add subject_id filter if specified
        if self.subject_id:
            query += " AND a.subject_id = ?"
            params.append(self.subject_id)

        query += " ORDER BY a.admittime"

        cursor = self.conn.execute(query, params)
        return cursor.fetchall()

    def format_event(self, db_row) -> dict:
        """
        Convert database row to admission event format.

        Args:
            db_row: Database row from admissions query

        Returns:
            Formatted admission event
        """
        return {
            "event_type": "ADMISSION",
            "event_time": db_row['admittime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {
                "subject_id": str(db_row['subject_id']),
                "age": db_row['anchor_age'],
                "gender": db_row['gender']
            },
            "admission": {
                "hadm_id": str(db_row['hadm_id']),
                "type": db_row['admission_type'],
                "location": db_row['admission_location'],
                "insurance": db_row['insurance'],
                "language": db_row['language'] or "UNKNOWN",
                "marital_status": db_row['marital_status'] or "UNKNOWN"
            },
            "discharge": {
                "time": db_row['dischtime'],
                "location": db_row['discharge_location']
            }
        }

    def is_high_priority(self, admission_type: str) -> bool:
        """
        Check if admission is high priority.

        Args:
            admission_type: Type of admission

        Returns:
            True if high priority (emergency/urgent)
        """
        emergency_types = ['EMERGENCY', 'URGENT', 'EW EMER.']
        return admission_type in emergency_types
