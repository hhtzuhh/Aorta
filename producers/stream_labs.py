"""
Time-Coordinated Lab Events Producer
Streams laboratory test results in sync with simulation clock
"""

from typing import List
from datetime import datetime

from .base_producer import TimeAwareProducer


class LabProducer(TimeAwareProducer):
    """
    Producer for laboratory test events.

    Queries lab events from the database within the current time window
    and streams them to the patient-labs Kafka topic.
    """

    def __init__(self, **kwargs):
        """
        Initialize the lab producer.

        Args:
            **kwargs: Passed to TimeAwareProducer (clock_url, subject_id, etc.)
        """
        super().__init__(**kwargs)
        self.topic = "patient-labs"

    def get_events_in_window(self, window_start: str, window_end: str) -> List:
        """
        Query lab events within the time window.

        Args:
            window_start: Start of time window (YYYY-MM-DD HH:MM:SS)
            window_end: End of time window (YYYY-MM-DD HH:MM:SS)

        Returns:
            List of lab event records
        """
        query = """
            SELECT
                l.subject_id,
                l.hadm_id,
                l.charttime,
                l.itemid,
                l.value,
                l.valuenum,
                l.valueuom,
                d.label as test_name,
                d.category,
                d.fluid
            FROM labevents l
            LEFT JOIN d_labitems d ON l.itemid = d.itemid
            WHERE l.charttime >= ? AND l.charttime < ?
        """

        params = [window_start, window_end]

        # Add subject_id filter if specified
        if self.subject_id:
            query += " AND l.subject_id = ?"
            params.append(self.subject_id)

        query += " ORDER BY l.charttime"

        cursor = self.conn.execute(query, params)
        return cursor.fetchall()

    def format_event(self, db_row) -> dict:
        """
        Convert database row to lab event format.

        Args:
            db_row: Database row from labevents query

        Returns:
            Formatted lab event
        """
        return {
            "event_type": "LAB_RESULT",
            "event_time": db_row['charttime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {
                "subject_id": str(db_row['subject_id'])
            },
            "admission": {
                "hadm_id": str(db_row['hadm_id']) if db_row['hadm_id'] else None
            },
            "lab": {
                "test_name": db_row['test_name'] or f"Unknown (ID: {db_row['itemid']})",
                "value": db_row['value'],
                "value_numeric": db_row['valuenum'],
                "unit": db_row['valueuom'],
                "category": db_row['category'],
                "fluid": db_row['fluid'],
                "itemid": db_row['itemid']
            }
        }

    def is_abnormal(self, test_name: str, value_num: float) -> bool:
        """
        Check if lab value is abnormal (simplified logic).

        This is a simplified example. In production, you'd use proper
        reference ranges from a medical database.

        Args:
            test_name: Name of the lab test
            value_num: Numeric value

        Returns:
            True if potentially abnormal
        """
        if not value_num:
            return False

        # Simple example thresholds (NOT medical advice!)
        simple_ranges = {
            'Creatinine': (0.5, 1.5),
            'Glucose': (70, 140),
            'Potassium': (3.5, 5.0),
            'Sodium': (135, 145),
        }

        for test, (low, high) in simple_ranges.items():
            if test.lower() in test_name.lower():
                return value_num < low or value_num > high

        return False
