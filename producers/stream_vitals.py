"""
Time-Coordinated Chartevents (Vitals) Producer
Streams ALL ICU chartevents in sync with simulation clock
"""

from typing import List
from datetime import datetime

from .base_producer import TimeAwareProducer


class VitalsProducer(TimeAwareProducer):
    """
    Producer for chartevent vital signs.

    Queries ALL chartevents from the database within the current time window
    for selected patients and streams them to the patient-vitals Kafka topic.

    Requires subject_ids parameter to prevent overwhelming event volume.
    """

    def __init__(self, **kwargs):
        """
        Initialize the vitals producer.

        Args:
            **kwargs: Passed to TimeAwareProducer (clock_url, subject_ids, etc.)

        Raises:
            ValueError: If subject_ids not provided
        """
        # Require subject_ids to prevent overwhelming volume
        if not kwargs.get('subject_ids'):
            raise ValueError("VitalsProducer requires subject_ids parameter (recommend 2-3 patients)")

        super().__init__(**kwargs)
        self.topic = "patient-vitals"

    def get_events_in_window(self, window_start: str, window_end: str) -> List:
        """
        Query ALL chartevents within the time window for filtered patients.

        Args:
            window_start: Start of time window (YYYY-MM-DD HH:MM:SS)
            window_end: End of time window (YYYY-MM-DD HH:MM:SS)

        Returns:
            List of chartevent records
        """
        query = """
            SELECT
                c.subject_id, c.hadm_id, c.stay_id,
                c.charttime, c.storetime,
                c.itemid,
                c.value,       -- Text value (always present)
                c.valuenum,    -- Numeric value (NULL for text-only)
                c.valueuom,    -- Unit of measurement
                c.warning,
                d.label,       -- Human-readable label
                d.category,    -- Category (e.g., "Routine Vital Signs")
                d.unitname,    -- Unit name from d_items
                d.param_type,  -- "Numeric", "Text", etc.
                i.first_careunit, i.last_careunit, i.los
            FROM chartevents c
            JOIN d_items d ON c.itemid = d.itemid
            LEFT JOIN icustays i ON c.stay_id = i.stay_id
            WHERE c.charttime >= ? AND c.charttime < ?
              AND c.warning = '0'
        """

        params = [window_start, window_end]

        # Always filter by subject_ids (required)
        if self.subject_ids:
            placeholders = ','.join('?' * len(self.subject_ids))
            query += f" AND c.subject_id IN ({placeholders})"
            params.extend(self.subject_ids)

        query += " ORDER BY c.charttime"

        cursor = self.conn.execute(query, params)
        return cursor.fetchall()

    def format_event(self, db_row) -> dict:
        """
        Convert database row to chartevent format.

        Args:
            db_row: Database row from chartevents query

        Returns:
            Formatted chartevent
        """
        return {
            "event_type": "CHARTEVENT",
            "event_time": db_row['charttime'],
            "store_time": db_row['storetime'],
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "patient": {
                "subject_id": str(db_row['subject_id'])
            },
            "admission": {
                "hadm_id": str(db_row['hadm_id']) if db_row['hadm_id'] else None
            },
            "icu_stay": {
                "stay_id": str(db_row['stay_id']) if db_row['stay_id'] else None,
                "first_careunit": db_row['first_careunit'] or "Unknown",
                "last_careunit": db_row['last_careunit'] or "Unknown",
                "los_days": float(db_row['los']) if db_row['los'] else None
            },
            "chartevent": {
                "itemid": db_row['itemid'],
                "label": db_row['label'] or f"Unknown (ID: {db_row['itemid']})",
                "category": db_row['category'],
                "param_type": db_row['param_type'],
                "value_text": db_row['value'],
                "value_numeric": float(db_row['valuenum']) if db_row['valuenum'] else None,
                "unit": db_row['valueuom'] or db_row['unitname'],
                "warning": int(db_row['warning'])
            }
        }
