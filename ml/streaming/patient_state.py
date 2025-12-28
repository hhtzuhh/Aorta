"""
Patient State Manager - Cumulative state tracking for ML predictions

Maintains patient features across time as events stream in.
State grows cumulatively - old data is kept, new data is added/updated.
"""

import pickle
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PatientState:
    """
    Cumulative patient state for sepsis prediction

    Features are accumulated across ticks:
    - Static features (demographics, comorbidities) set on admission
    - Dynamic features (vitals, labs) updated as new measurements arrive
    - SOFA history tracked for sofa_change calculation
    """

    # Static (from admission event)
    subject_id: str
    hadm_id: str
    age: int = 0
    admission_location: str = "UNKNOWN"

    # Comorbidities (from ComorbidityService on admission)
    diabetes: bool = False
    hypertension: bool = False
    chronic_kidney_disease: bool = False
    congestive_heart_failure: bool = False
    copd: bool = False
    cancer: bool = False
    liver_disease: bool = False

    # Latest vitals (from patient-vitals events)
    heart_rate: Optional[float] = None
    sbp: Optional[float] = None
    dbp: Optional[float] = None
    map: Optional[float] = None  # Mean arterial pressure
    resp_rate: Optional[float] = None
    spo2: Optional[float] = None
    gcs: Optional[float] = None
    temperature: Optional[float] = None

    # Latest labs (from patient-labs events)
    paco2: Optional[float] = None
    bilirubin: Optional[float] = None
    platelets: Optional[float] = None  # Added for SOFA
    creatinine: Optional[float] = None  # Added for SOFA

    # Mechanical ventilation (from chartevents)
    mechanical_vent: bool = False

    # SOFA tracking (for sofa_change calculation)
    sofa_history: List[Tuple[datetime, float]] = field(default_factory=list)

    # Timestamps
    last_updated: datetime = field(default_factory=datetime.now)
    admission_time: datetime = field(default_factory=datetime.now)


class PatientStateManager:
    """
    Manage patient states with checkpoint persistence

    Features:
    - Cumulative state updates (old data kept, new data added/updated)
    - Checkpoint save/load for recovery on restart
    - TTL-based cleanup
    - Feature vector generation for ML model
    """

    def __init__(self, checkpoint_path: Optional[str] = None):
        """
        Initialize patient state manager

        Args:
            checkpoint_path: Path to save/load state checkpoints
        """
        self.states: Dict[str, PatientState] = {}  # hadm_id -> PatientState
        self.ttl_hours = 24  # Remove states older than 24 hours
        self.checkpoint_path = checkpoint_path

    def get_or_create(self, hadm_id: str) -> PatientState:
        """
        Get existing state or create new one

        Args:
            hadm_id: Hospital admission ID

        Returns:
            PatientState instance
        """
        if hadm_id not in self.states:
            self.states[hadm_id] = PatientState(
                subject_id="",  # Will be filled on admission event
                hadm_id=hadm_id,
            )
        return self.states[hadm_id]

    def update_demographics(
        self, hadm_id: str, age: int, admission_location: str, subject_id: str
    ):
        """Update patient demographics (static features)"""
        state = self.get_or_create(hadm_id)
        state.subject_id = subject_id
        state.age = age
        state.admission_location = admission_location
        state.last_updated = datetime.now()

    def update_comorbidities(self, hadm_id: str, comorbidities: Dict[str, bool]):
        """Update patient comorbidities (static features)"""
        state = self.get_or_create(hadm_id)
        state.diabetes = comorbidities.get("diabetes", False)
        state.hypertension = comorbidities.get("hypertension", False)
        state.chronic_kidney_disease = comorbidities.get(
            "chronic_kidney_disease", False
        )
        state.congestive_heart_failure = comorbidities.get(
            "congestive_heart_failure", False
        )
        state.copd = comorbidities.get("copd", False)
        state.cancer = comorbidities.get("cancer", False)
        state.liver_disease = comorbidities.get("liver_disease", False)
        state.last_updated = datetime.now()

    def update_vitals(self, hadm_id: str, vital_name: str, value: float):
        """
        Update patient vital sign (dynamic feature)

        Replaces old value if exists, adds new value if doesn't exist.

        Args:
            hadm_id: Hospital admission ID
            vital_name: Name of vital (heart_rate, sbp, dbp, map, etc.)
            value: Vital measurement value
        """
        state = self.get_or_create(hadm_id)
        setattr(state, vital_name, value)
        state.last_updated = datetime.now()

    def update_labs(self, hadm_id: str, lab_name: str, value: float):
        """
        Update patient lab result (dynamic feature)

        Replaces old value if exists, adds new value if doesn't exist.

        Args:
            hadm_id: Hospital admission ID
            lab_name: Name of lab (paco2, bilirubin, platelets, creatinine)
            value: Lab result value
        """
        state = self.get_or_create(hadm_id)
        setattr(state, lab_name, value)
        state.last_updated = datetime.now()

    def update_sofa(self, hadm_id: str, sofa_score: float):
        """
        Update SOFA score history

        Args:
            hadm_id: Hospital admission ID
            sofa_score: SOFA score value
        """
        state = self.get_or_create(hadm_id)
        state.sofa_history.append((datetime.now(), sofa_score))
        state.last_updated = datetime.now()

    def to_feature_vector(self, hadm_id: str) -> Dict[str, Any]:
        """
        Convert patient state to 23-feature vector for ML model

        Returns all accumulated features (old + new combined).
        Missing features will be None (handled by imputer).

        Args:
            hadm_id: Hospital admission ID

        Returns:
            Dictionary with 23 features matching training data
        """
        state = self.states.get(hadm_id)
        if not state:
            return {}

        return {
            # Demographics (2)
            "age": state.age,
            "admission_location": state.admission_location,
            # Comorbidities (7)
            "diabetes": state.diabetes,
            "hypertension": state.hypertension,
            "chronic_kidney_disease": state.chronic_kidney_disease,
            "congestive_heart_failure": state.congestive_heart_failure,
            "copd": state.copd,
            "cancer": state.cancer,
            "liver_disease": state.liver_disease,
            # Vitals (8)
            "heart_rate": state.heart_rate,
            "sbp": state.sbp,
            "dbp": state.dbp,
            "map": state.map,
            "resp_rate": state.resp_rate,
            "spo2": state.spo2,
            "gcs": state.gcs,
            "temperature": state.temperature,
            # Labs (4)
            "paco2": state.paco2,
            "bilirubin": state.bilirubin,
            "platelets": state.platelets,
            "creatinine": state.creatinine,
            # SOFA (3) - will be filled by prediction logic
            "sofa_total": None,  # Calculated from other features
            "sofa_change": None,  # Calculated from history
            "organ_dysfunction": None,  # Calculated from sofa_total
            # Mechanical ventilation (1)
            "mechanical_vent": state.mechanical_vent,
        }

    def cleanup_expired(self):
        """Remove states older than TTL"""
        now = datetime.now()
        expired = []

        for hadm_id, state in self.states.items():
            age_hours = (now - state.last_updated).total_seconds() / 3600
            if age_hours > self.ttl_hours:
                expired.append(hadm_id)

        for hadm_id in expired:
            del self.states[hadm_id]

        if expired:
            logger.info(f"Cleaned up {len(expired)} expired patient states")

    def save_checkpoint(self, filepath: Optional[str] = None):
        """
        Save state to disk for recovery on restart

        Args:
            filepath: Override checkpoint path
        """
        path = filepath or self.checkpoint_path
        if not path:
            logger.warning("No checkpoint path configured, skipping save")
            return

        try:
            with open(path, "wb") as f:
                pickle.dump(self.states, f)
            logger.info(f"Saved checkpoint: {len(self.states)} patient states to {path}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self, filepath: Optional[str] = None):
        """
        Load state from disk on startup

        Args:
            filepath: Override checkpoint path
        """
        path = filepath or self.checkpoint_path
        if not path or not Path(path).exists():
            logger.info("No checkpoint found, starting fresh")
            return

        try:
            with open(path, "rb") as f:
                self.states = pickle.load(f)
            logger.info(f"Loaded checkpoint: {len(self.states)} patient states from {path}")
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            self.states = {}

    def get_stats(self) -> Dict[str, Any]:
        """Get state manager statistics"""
        if not self.states:
            return {"total_patients": 0}

        feature_counts = {}
        for state in self.states.values():
            features = self.to_feature_vector(state.hadm_id)
            non_null = sum(1 for v in features.values() if v is not None)
            feature_counts[state.hadm_id] = non_null

        return {
            "total_patients": len(self.states),
            "avg_features_per_patient": (
                sum(feature_counts.values()) / len(feature_counts)
                if feature_counts
                else 0
            ),
            "min_features": min(feature_counts.values()) if feature_counts else 0,
            "max_features": max(feature_counts.values()) if feature_counts else 0,
        }
