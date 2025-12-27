"""
Time-Coordinated ICU Stay Producer
Streams ICU admission events in sync with simulation clock
"""

from typing import List
from datetime import datetime

from .base_producer import TimeAwareProducer


class ICUStayProducer(TimeAwareProducer):
    """
    Producer for ICU admission events.

    Queries ICU stays from the database where intime falls within
    the current time window and streams them to the icu-admissions
    Kafka topic.

    Each ICU admission is a separate event, allowing tracking of:
    - Multiple ICU stays per hospital admission
    - ICU unit transfers (first_careunit != last_careunit)
    - ICU admission timing relative to hospital admission
    """

    def __init__(self, **kwargs):
        """
        Initialize the ICU stay producer.

        Args:
            **kwargs: Passed to TimeAwareProducer (clock_url, subject_ids, etc.)
        """
        super().__init__(**kwargs)
        self.topic = "icu-admissions"

    def get_events_in_window(self, window_start: str, window_end: str) -> List:
        """
        Query ICU stays where intime falls within the time window.

        Args:
            window_start: Start of time window (YYYY-MM-DD HH:MM:SS)
            window_end: End of time window (YYYY-MM-DD HH:MM:SS)

        Returns:
            List of ICU stay records
        """
        query = """
            SELECT
                i.subject_id,
                i.hadm_id,
                i.stay_id,
                i.first_careunit,
                i.last_careunit,
                i.intime,
                i.outtime,
                i.los
            FROM icustays i
            WHERE i.intime >= ? AND i.intime < ?
        """

        params = [window_start, window_end]

        # Add optional subject_ids filter
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND i.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)

        query += " ORDER BY i.intime"

        cursor = self.conn.execute(query, params)
        return cursor.fetchall()

    def format_event(self, db_row) -> dict:
        """
        Convert database row to ICU admission event format.

        Args:
            db_row: Database row from icustays query

        Returns:
            Formatted ICU admission event
        """
        # Determine if patient was transferred between ICU units
        is_transfer = (db_row['first_careunit'] != db_row['last_careunit'])

        # Determine status based on outtime
        # In historical data, outtime presence indicates patient left ICU
        status = "DISCHARGED" if db_row['outtime'] else "ACTIVE"

        return {
            "event_type": "ICU_ADMISSION",
            "event_time": db_row['intime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {
                "subject_id": str(db_row['subject_id'])
            },
            "admission": {
                "hadm_id": str(db_row['hadm_id']) if db_row['hadm_id'] else None
            },
            "icu_stay": {
                "stay_id": str(db_row['stay_id']),
                "first_careunit": db_row['first_careunit'] or "Unknown",
                "last_careunit": db_row['last_careunit'] or "Unknown",
                "intime": db_row['intime'],
                "outtime": db_row['outtime'],
                "los_days": float(db_row['los']) if db_row['los'] else None,
                "status": status,
                "is_transfer": is_transfer
            }
        }
