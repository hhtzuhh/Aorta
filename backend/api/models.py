"""
Pydantic models for hospital admission events

Models match the JSON schema produced by stream_admissions.py
"""

from typing import Optional
from pydantic import BaseModel, computed_field


class Patient(BaseModel):
    """Patient demographic information"""

    subject_id: str
    age: int
    gender: str


class Admission(BaseModel):
    """Hospital admission details"""

    hadm_id: str
    type: str
    location: str
    insurance: str
    language: str
    marital_status: str


class Discharge(BaseModel):
    """Discharge information"""

    time: Optional[str] = None
    location: Optional[str] = None


class AdmissionEvent(BaseModel):
    """
    Complete admission event from Kafka

    Matches the JSON structure from stream_admissions.py
    """

    event_type: str
    timestamp: str
    patient: Patient
    admission: Admission
    discharge: Discharge

    @computed_field
    @property
    def is_high_priority(self) -> bool:
        """
        Determine if admission is high priority

        Emergency types that require immediate attention:
        - EMERGENCY
        - URGENT
        - EW EMER. (Emergency Ward Emergency)
        """
        emergency_types = {"EMERGENCY", "URGENT", "EW EMER."}
        return self.admission.type in emergency_types

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "ADMISSION",
                "timestamp": "2112-05-28 19:45:00",
                "patient": {
                    "subject_id": "10000032",
                    "age": 52,
                    "gender": "F"
                },
                "admission": {
                    "hadm_id": "29079034",
                    "type": "URGENT",
                    "location": "EMERGENCY ROOM",
                    "insurance": "Medicare",
                    "language": "ENGLISH",
                    "marital_status": "MARRIED"
                },
                "discharge": {
                    "time": "2112-06-05 15:45:00",
                    "location": "HOME"
                }
            }
        }
