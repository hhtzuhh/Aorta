"""
ML Prediction Module - Pluggable sepsis prediction engine for UnifiedConsumer

Does NOT consume Kafka directly - receives events via callback methods from UnifiedConsumer.
Maintains patient state, makes predictions, publishes alerts to sepsis-alerts topic.
"""

import json
import joblib
import logging
import pandas as pd
import sqlite3
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional
from confluent_kafka import Producer

from Aorta.ml.features.sofa_calculator import SOFACalculator
from Aorta.ml.features.comorbidity_service import ComorbidityService
from Aorta.ml.streaming.patient_state import PatientStateManager
from Aorta.ml.streaming.throttler import PredictionThrottler
from Aorta.backend.api.models import (
    AdmissionEvent,
    LabEvent,
    CharteventEvent,
    ICUAdmissionEvent,
    SepsisAlert,
    SepsisPrediction,
    PatientReference,
    AdmissionReference,
)

logger = logging.getLogger(__name__)


class MLPredictionModule:
    """
    ML prediction module that plugs into UnifiedConsumer

    Architecture:
    - UnifiedConsumer calls callback methods (on_admission, on_lab, on_vitals, on_icu)
    - Module maintains cumulative patient state
    - Triggers predictions based on feature completeness + tick throttling
    - Publishes alerts to sepsis-alerts topic via its own Producer
    """

    def __init__(
        self,
        kafka_config: dict,
        model_dir: str,
        db_path: str,
        alert_threshold: float = 0.3,
        tick_interval: int = 3,
        tick_duration_minutes: int = 60,
    ):
        """
        Initialize ML prediction module

        Args:
            kafka_config: Kafka configuration dict
            model_dir: Directory containing model artifacts
            db_path: Path to SQLite database (for lazy init)
            alert_threshold: Minimum probability to publish alert (default: 0.3)
            tick_interval: Predict every N ticks (default: 3)
            tick_duration_minutes: Minutes per tick (default: 60)
        """
        logger.info("Initializing MLPredictionModule...")

        # Load model artifacts
        try:
            self.model = joblib.load(f"{model_dir}/sepsis_model.pkl")
            self.imputer = joblib.load(f"{model_dir}/imputer.pkl")
            self.label_encoder = joblib.load(f"{model_dir}/label_encoder.pkl")
            with open(f"{model_dir}/feature_names.json") as f:
                self.feature_names = json.load(f)

            # Validate model compatibility
            assert len(self.feature_names) == 23, "Expected 23 features"
            assert self.imputer.n_features_in_ == 23, "Imputer feature mismatch"
            assert self.model.n_features_in_ == 23, "Model feature mismatch"

            logger.info(f"Loaded model with {self.model.n_estimators} trees")
        except Exception as e:
            logger.error(f"Failed to load model artifacts: {e}")
            raise

        # Initialize services
        self.state_manager = PatientStateManager(checkpoint_path="/tmp/patient_state.pkl")
        self.comorbidity_service = ComorbidityService(db_path)
        self.sofa_calculator = SOFACalculator()
        self.throttler = PredictionThrottler(tick_interval=tick_interval)
        self.tick_duration_minutes = tick_duration_minutes
        self.db_path = db_path

        # Load checkpoint if exists
        self.state_manager.load_checkpoint()

        # Kafka producer for sepsis-alerts (module owns this)
        self.producer = Producer(kafka_config)
        self.alert_topic = "sepsis-alerts"
        self.alert_threshold = alert_threshold

        # Recent alerts (for UnifiedConsumer to display)
        self.recent_alerts = deque(maxlen=100)

        logger.info(
            f"MLPredictionModule initialized (threshold={alert_threshold}, "
            f"throttle={tick_interval} ticks)"
        )

    # ==================== Event Callback Methods ====================
    # Called by UnifiedConsumer

    async def on_admission(self, event_data: dict):
        """
        Handle admission event - initialize patient state

        Args:
            event_data: Raw event dict from Kafka
        """
        try:
            event = AdmissionEvent(**event_data)
            hadm_id = event.admission.hadm_id

            # Initialize patient state
            self.state_manager.update_demographics(
                hadm_id=hadm_id,
                age=event.patient.age,
                admission_location=event.admission.location,
                subject_id=event.patient.subject_id,
            )

            # Load comorbidities
            comorbidities = self.comorbidity_service.get_comorbidities(hadm_id)
            self.state_manager.update_comorbidities(hadm_id, comorbidities)

            # Set admission time
            state = self.state_manager.get_or_create(hadm_id)
            state.admission_time = datetime.fromisoformat(event.event_time)

            logger.info(
                f"Initialized patient state for {hadm_id} "
                f"(age={event.patient.age}, location={event.admission.location})"
            )

        except Exception as e:
            logger.error(f"Error handling admission event: {e}", exc_info=True)

    async def on_vitals(self, event_data: dict):
        """
        Handle vital signs event - update state and maybe predict

        Args:
            event_data: Raw event dict from Kafka
        """
        try:
            event = CharteventEvent(**event_data)

            # Get hadm_id from admission field (producer includes it)
            hadm_id = event_data.get("admission", {}).get("hadm_id")

            if not hadm_id:
                return  # Skip vitals without admission

            # Lazy init if state doesn't exist (out-of-order events)
            if hadm_id not in self.state_manager.states:
                await self._lazy_init_patient_state(hadm_id, event.patient.subject_id)

            # Update state
            label = event.chartevent.label.lower()
            value = event.chartevent.value_numeric

            if value is None:
                return  # Skip non-numeric vitals

            # Map MIMIC labels to feature names
            vital_mapping = {
                "heart rate": "heart_rate",
                "respiratory rate": "resp_rate",
                "o2 saturation": "spo2",
                "temperature fahrenheit": "temperature",
                "non invasive blood pressure systolic": "sbp",
                "non invasive blood pressure diastolic": "dbp",
                "arterial blood pressure mean": "map",
                "gcs total": "gcs",
            }

            feature_name = vital_mapping.get(label)
            if feature_name:
                self.state_manager.update_vitals(hadm_id, feature_name, value)

                # Trigger prediction (with tick-based throttling)
                if self.throttler.should_predict(
                    hadm_id,
                    event_time=event_data.get("event_time"),
                    tick_duration_minutes=self.tick_duration_minutes,
                ):
                    await self._predict_and_publish(hadm_id, event_data.get("event_time"))

        except Exception as e:
            logger.error(f"Error handling vitals event: {e}", exc_info=True)

    async def on_lab(self, event_data: dict):
        """
        Handle lab result event - update state and maybe predict

        Args:
            event_data: Raw event dict from Kafka
        """
        try:
            event = LabEvent(**event_data)
            hadm_id = event.admission.hadm_id if event.admission else None

            if not hadm_id:
                return  # Skip labs without admission

            # Lazy init if needed
            if hadm_id not in self.state_manager.states:
                await self._lazy_init_patient_state(hadm_id, event.patient.subject_id)

            # Update state
            test_name = event.lab.test_name.lower()
            value = event.lab.value_numeric

            if value is None:
                return

            # Map lab names to features
            lab_mapping = {
                "pco2": "paco2",
                "bilirubin, total": "bilirubin",
                "platelet count": "platelets",
                "platelets": "platelets",
                "creatinine": "creatinine",
            }

            feature_name = lab_mapping.get(test_name)
            if feature_name:
                self.state_manager.update_labs(hadm_id, feature_name, value)

                # Check if abnormal (critical event)
                is_critical = event.is_abnormal if hasattr(event, "is_abnormal") else False

                # Trigger prediction (with tick-based throttling, override if abnormal)
                if self.throttler.should_predict(
                    hadm_id,
                    event_time=event_data.get("event_time"),
                    tick_duration_minutes=self.tick_duration_minutes,
                    is_critical_event=is_critical,
                ):
                    await self._predict_and_publish(hadm_id, event_data.get("event_time"))

        except Exception as e:
            logger.error(f"Error handling lab event: {e}", exc_info=True)

    async def on_icu(self, event_data: dict):
        """
        Handle ICU admission event - trigger prediction (critical event)

        Args:
            event_data: Raw event dict from Kafka
        """
        try:
            event = ICUAdmissionEvent(**event_data)
            hadm_id = event.admission.hadm_id if event.admission else None

            if hadm_id and hadm_id in self.state_manager.states:
                # ICU admission is critical - override throttle
                if self.throttler.should_predict(
                    hadm_id,
                    event_time=event_data.get("event_time"),
                    tick_duration_minutes=self.tick_duration_minutes,
                    is_critical_event=True,
                ):
                    await self._predict_and_publish(hadm_id, event_data.get("event_time"))

        except Exception as e:
            logger.error(f"Error handling ICU event: {e}", exc_info=True)

    # ==================== Prediction Logic ====================

    async def _predict_and_publish(self, hadm_id: str, event_time: str):
        """
        Make prediction and publish alert if above threshold

        Args:
            hadm_id: Hospital admission ID
            event_time: Simulation time of the triggering event (ISO format)
        """
        try:
            state = self.state_manager.states.get(hadm_id)
            if not state:
                return

            # Build feature vector (all accumulated features)
            features = self.state_manager.to_feature_vector(hadm_id)

            # Calculate SOFA (requires vitals dictionary)
            vitals_dict = {
                "pao2": features.get("paco2"),  # Using PaCO2 as proxy
                "fio2": 0.21,  # Assume room air
                "spo2": features.get("spo2"),
                "mechanical_vent": features.get("mechanical_vent", False),
                "platelets": features.get("platelets"),
                "bilirubin": features.get("bilirubin"),
                "mean_arterial_pressure": features.get("map"),
                "gcs": features.get("gcs"),
                "creatinine": features.get("creatinine"),
                "urine_output": None,  # Not available in streaming
            }
            sofa_result = self.sofa_calculator.calculate_total_sofa(vitals_dict)
            sofa_score = sofa_result.get("sofa_total", 0)
            features["sofa_total"] = sofa_score

            # Calculate sofa_change
            if len(state.sofa_history) > 0:
                features["sofa_change"] = sofa_score - state.sofa_history[-1][1]
            else:
                features["sofa_change"] = 0

            self.state_manager.update_sofa(hadm_id, sofa_score)
            features["organ_dysfunction"] = 1 if sofa_score >= 2 else 0

            # Check feature completeness
            missing_count = sum(1 for v in features.values() if v is None)
            missing_pct = missing_count / len(self.feature_names)

            if missing_pct > 0.5:
                logger.debug(
                    f"Skipping prediction for {hadm_id}: {missing_pct:.0%} features missing"
                )
                return

            # Convert to DataFrame
            feature_df = pd.DataFrame([features], columns=self.feature_names)

            # Encode categorical features (safe encoding with fallback)
            for col in ["admission_location"]:
                if col in feature_df.columns:
                    try:
                        feature_df[col] = self.label_encoder[col].transform(feature_df[col])
                    except (ValueError, KeyError):
                        # Unknown category - use first class as default
                        logger.warning(
                            f"Unknown {col}: {feature_df[col].values[0]}, using default"
                        )
                        feature_df[col] = self.label_encoder[col].transform(
                            [self.label_encoder[col].classes_[0]]
                        )

            # Impute missing values
            feature_array = self.imputer.transform(feature_df)

            # Predict
            probability = self.model.predict_proba(feature_array)[0][1]

            # Determine risk level
            if probability >= 0.7:
                risk_level = "CRITICAL"
            elif probability >= 0.5:
                risk_level = "HIGH"
            elif probability >= 0.3:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            # Publish alert if above threshold
            if probability >= self.alert_threshold:
                alert = SepsisAlert(
                    event_type="SEPSIS_ALERT",
                    event_time=event_time,  # Use simulation time, not real time
                    patient=PatientReference(
                        subject_id=state.subject_id,
                        age=state.age  # Include age for RAG filtering
                    ),
                    admission=AdmissionReference(hadm_id=hadm_id),
                    prediction=SepsisPrediction(
                        sepsis_probability=probability,
                        risk_level=risk_level,
                        model_version="v1_local",
                        sofa_score=sofa_score,
                    ),
                )

                # Send to Kafka
                self.producer.produce(
                    topic=self.alert_topic,
                    key=state.subject_id,
                    value=alert.model_dump_json(),
                    callback=self._delivery_callback,
                )
                self.producer.poll(0)

                # Store in recent alerts
                self.recent_alerts.append(alert)

                logger.info(
                    f"🚨 Sepsis alert for {hadm_id}: {probability:.2%} ({risk_level}), "
                    f"SOFA={sofa_score:.1f}"
                )

        except Exception as e:
            logger.error(f"Error making prediction for {hadm_id}: {e}", exc_info=True)

    async def _lazy_init_patient_state(self, hadm_id: str, subject_id: str):
        """
        Initialize patient state from database (for out-of-order events)

        Args:
            hadm_id: Hospital admission ID
            subject_id: Patient subject ID
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # Query admission details
            query = """
            SELECT a.subject_id, a.hadm_id, a.admittime, a.admission_location,
                   CAST((JULIANDAY(a.admittime) - JULIANDAY(p.anchor_year || '-01-01')) / 365.25 AS INTEGER) + p.anchor_age AS age
            FROM admissions a
            JOIN patients p ON a.subject_id = p.subject_id
            WHERE a.hadm_id = ?
            """
            row = conn.execute(query, [hadm_id]).fetchone()

            if row:
                self.state_manager.update_demographics(
                    hadm_id=hadm_id,
                    age=row["age"],
                    admission_location=row["admission_location"],
                    subject_id=str(row["subject_id"]),
                )

                # Load comorbidities
                comorbidities = self.comorbidity_service.get_comorbidities(hadm_id)
                self.state_manager.update_comorbidities(hadm_id, comorbidities)

                # Set admission time
                state = self.state_manager.get_or_create(hadm_id)
                state.admission_time = datetime.fromisoformat(row["admittime"])

                logger.info(f"Lazy-initialized patient state for {hadm_id}")

            conn.close()

        except Exception as e:
            logger.error(f"Error lazy-initializing state for {hadm_id}: {e}", exc_info=True)

    # ==================== Utility Methods ====================

    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            logger.error(f"Alert delivery failed: {err}")

    def flush(self):
        """Flush pending Kafka messages (called on shutdown)"""
        logger.info("Flushing ML prediction producer...")
        self.producer.flush(5)

    def get_recent_alerts(self) -> List[SepsisAlert]:
        """Get recent alerts for dashboard"""
        return list(self.recent_alerts)

    def get_stats(self) -> Dict[str, any]:
        """Get module statistics"""
        return {
            "module": "MLPredictionModule",
            "model_version": "v1_local",
            "alert_threshold": self.alert_threshold,
            "tick_interval": self.throttler.tick_interval,
            "state_manager": self.state_manager.get_stats(),
            "throttler": self.throttler.get_stats(),
            "comorbidity_service": self.comorbidity_service.get_stats(),
            "recent_alerts_count": len(self.recent_alerts),
        }
