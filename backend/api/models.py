"""
Pydantic models for hospital admission and lab events

Models match the JSON schema produced by stream_admissions_coordinated.py and stream_labs.py
"""

from typing import Optional
from pydantic import BaseModel, computed_field


# Reference models (for lab events - minimal data)
class PatientReference(BaseModel):
    """Minimal patient reference (used in lab events)"""
    subject_id: str


class AdmissionReference(BaseModel):
    """Minimal admission reference (used in lab events)"""
    hadm_id: Optional[str] = None


# Full models (for admission events - complete data)
class Patient(BaseModel):
    """Patient demographic information (full)"""

    subject_id: str
    age: int
    gender: str


class Admission(BaseModel):
    """Hospital admission details (full)"""

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

    Matches the JSON structure from stream_admissions_coordinated.py
    """

    event_type: str
    event_time: str
    processing_time: Optional[str] = None
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


class LabTest(BaseModel):
    """Laboratory test details"""

    labevent_id: str
    specimen_id: Optional[str] = None
    test_name: str
    itemid: int
    value_numeric: Optional[float] = None
    unit: Optional[str] = None
    ref_range_lower: Optional[float] = None
    ref_range_upper: Optional[float] = None
    flag: Optional[str] = None
    priority: Optional[str] = None
    category: Optional[str] = None
    fluid: Optional[str] = None


class LabEvent(BaseModel):
    """
    Complete lab event from Kafka

    Matches the JSON structure from stream_labs.py
    Uses reference models since lab events only contain IDs
    """

    event_type: str
    event_time: str
    store_time: Optional[str] = None
    processing_time: str
    patient: PatientReference  # Only subject_id
    admission: AdmissionReference  # Only hadm_id
    lab: LabTest

    @computed_field
    @property
    def is_abnormal(self) -> bool:
        """
        Determine if lab result is abnormal based on flag field
        """
        return self.lab.flag and self.lab.flag.upper() == "ABNORMAL"


class ICUStay(BaseModel):
    """ICU stay details"""

    stay_id: str
    first_careunit: Optional[str] = None
    last_careunit: Optional[str] = None
    intime: Optional[str] = None
    outtime: Optional[str] = None
    los_days: Optional[float] = None
    status: Optional[str] = None
    is_transfer: bool = False


class ICUAdmissionEvent(BaseModel):
    """
    Complete ICU admission event from Kafka

    Matches the JSON structure for ICU_ADMISSION events
    """

    event_type: str
    event_time: str
    patient: PatientReference
    admission: AdmissionReference
    icu_stay: ICUStay


class CharteventData(BaseModel):
    """Chartevent (vital sign) details"""

    itemid: int
    label: str
    category: str
    param_type: str
    value_text: Optional[str] = None
    value_numeric: Optional[float] = None
    unit: Optional[str] = None


class CharteventEvent(BaseModel):
    """
    Complete chartevent (vital sign) event from Kafka

    Matches the JSON structure for CHARTEVENT events
    """

    event_type: str
    event_time: str
    patient: PatientReference
    icu_stay: ICUStay
    chartevent: CharteventData
