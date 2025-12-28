"""
Feature Extractor for Sepsis Prediction Model

Extracts 23 required features from MIMIC-IV SQLite database:
- Demographics: age, admission_location
- Comorbidities: diabetes, hypertension, CKD, CHF, COPD, cancer, liver_disease
- Vitals: heart_rate, sbp, dbp, map, resp_rate, spo2, gcs, temperature
- Labs: paco2, bilirubin
- SOFA: sofa_total, sofa_change, organ_dysfunction
- Mechanical ventilation: mechanical_vent
"""

import sqlite3
from typing import Dict, Any, Optional, List
import pandas as pd

from .sofa_calculator import SOFACalculator


class FeatureExtractor:
    """Extract features from MIMIC-IV SQLite database for sepsis prediction"""

    # ICD-9/ICD-10 code patterns for comorbidities
    COMORBIDITY_CODES = {
        "diabetes": ["250%", "E08%", "E09%", "E10%", "E11%", "E13%"],
        "hypertension": ["401%", "402%", "403%", "404%", "405%", "I10%", "I11%", "I12%", "I13%", "I15%"],
        "chronic_kidney_disease": ["585%", "586%", "N18%", "N19%"],
        "congestive_heart_failure": ["428%", "I50%"],
        "copd": ["491%", "492%", "494%", "496%", "J41%", "J42%", "J43%", "J44%"],
        "cancer": ["14%", "15%", "16%", "17%", "18%", "19%", "20%", "C%", "D0%"],
        "liver_disease": ["571%", "572%", "K70%", "K71%", "K72%", "K73%", "K74%", "K76%"],
    }

    # ItemIDs for vitals from d_items
    VITAL_ITEMIDS = {
        "heart_rate": [220045, 211],  # Heart rate
        "sbp": [220050, 220179, 51, 442, 455],  # Systolic BP
        "dbp": [220051, 220180, 8368, 8440, 8441, 8555],  # Diastolic BP
        "mean_bp": [220052, 220181, 456, 52, 6702],  # Mean arterial pressure
        "resp_rate": [220210, 224690, 618, 615, 224689],  # Respiratory rate
        "spo2": [220277, 646],  # SpO2
        "gcs": [220739, 223900, 223901],  # Glasgow Coma Scale
        "temperature": [223761, 223762, 676, 677],  # Temperature
    }

    # Lab itemIDs from d_labitems
    LAB_ITEMIDS = {
        "paco2": [50818],  # PaCO2
        "bilirubin": [50885],  # Total bilirubin
        "creatinine": [50912],  # Creatinine
        "platelets": [51265],  # Platelet count
    }

    def __init__(self, db_path: str):
        """
        Initialize feature extractor

        Args:
            db_path: Path to MIMIC-IV SQLite database
        """
        self.db_path = db_path
        self.sofa_calc = SOFACalculator()

    def _get_connection(self) -> sqlite3.Connection:
        """Create database connection"""
        return sqlite3.connect(self.db_path)

    def extract_demographics(self, hadm_id: str) -> Dict[str, Any]:
        """
        Extract demographic features for a hospital admission

        Args:
            hadm_id: Hospital admission ID

        Returns:
            Dictionary with age and admission_location
        """
        conn = self._get_connection()
        query = """
        SELECT
            CAST((JULIANDAY(a.admittime) - JULIANDAY(p.anchor_year || '-01-01')) / 365.25
                + p.anchor_age AS INTEGER) as age,
            a.admission_location
        FROM admissions a
        JOIN patients p ON a.subject_id = p.subject_id
        WHERE a.hadm_id = ?
        """
        result = pd.read_sql_query(query, conn, params=(hadm_id,))
        conn.close()

        if result.empty:
            return {"age": None, "admission_location": None}

        return {
            "age": result.iloc[0]["age"],
            "admission_location": result.iloc[0]["admission_location"],
        }

    def extract_comorbidities(self, hadm_id: str) -> Dict[str, int]:
        """
        Extract comorbidity flags for a hospital admission

        Args:
            hadm_id: Hospital admission ID

        Returns:
            Dictionary with binary flags for each comorbidity
        """
        conn = self._get_connection()

        comorbidities = {}
        for condition, patterns in self.COMORBIDITY_CODES.items():
            # Build LIKE clause for all patterns
            like_clauses = " OR ".join([f"icd_code LIKE '{pattern}'" for pattern in patterns])

            query = f"""
            SELECT COUNT(*) as has_condition
            FROM diagnoses_icd
            WHERE hadm_id = ? AND ({like_clauses})
            """

            result = pd.read_sql_query(query, conn, params=(hadm_id,))
            comorbidities[condition] = 1 if result.iloc[0]["has_condition"] > 0 else 0

        conn.close()
        return comorbidities

    def extract_latest_vitals(self, hadm_id: str, before_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract latest vital signs for a hospital admission

        Args:
            hadm_id: Hospital admission ID
            before_time: Optional timestamp to get vitals before this time

        Returns:
            Dictionary with latest vital sign values
        """
        conn = self._get_connection()

        vitals = {}

        # Build time constraint
        time_constraint = f"AND charttime <= '{before_time}'" if before_time else ""

        for vital_name, itemids in self.VITAL_ITEMIDS.items():
            itemid_list = ",".join(map(str, itemids))

            query = f"""
            SELECT valuenum
            FROM chartevents
            WHERE hadm_id = ?
                AND itemid IN ({itemid_list})
                AND valuenum IS NOT NULL
                {time_constraint}
            ORDER BY charttime DESC
            LIMIT 1
            """

            result = pd.read_sql_query(query, conn, params=(hadm_id,))

            if not result.empty:
                try:
                    # Convert to float (handles both numeric and text storage)
                    vitals[vital_name] = float(result.iloc[0]["valuenum"])
                except (ValueError, TypeError):
                    vitals[vital_name] = None
            else:
                vitals[vital_name] = None

        conn.close()
        return vitals

    def extract_latest_labs(self, hadm_id: str, before_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract latest lab results for a hospital admission

        Args:
            hadm_id: Hospital admission ID
            before_time: Optional timestamp to get labs before this time

        Returns:
            Dictionary with latest lab values
        """
        conn = self._get_connection()

        labs = {}

        # Build time constraint
        time_constraint = f"AND charttime <= '{before_time}'" if before_time else ""

        for lab_name, itemids in self.LAB_ITEMIDS.items():
            itemid_list = ",".join(map(str, itemids))

            query = f"""
            SELECT valuenum
            FROM labevents
            WHERE hadm_id = ?
                AND itemid IN ({itemid_list})
                AND valuenum IS NOT NULL
                {time_constraint}
            ORDER BY charttime DESC
            LIMIT 1
            """

            result = pd.read_sql_query(query, conn, params=(hadm_id,))

            if not result.empty:
                try:
                    # Convert to float (handles both numeric and text storage)
                    labs[lab_name] = float(result.iloc[0]["valuenum"])
                except (ValueError, TypeError):
                    labs[lab_name] = None
            else:
                labs[lab_name] = None

        conn.close()
        return labs

    def check_mechanical_ventilation(self, hadm_id: str, before_time: Optional[str] = None) -> bool:
        """
        Check if patient is on mechanical ventilation

        Args:
            hadm_id: Hospital admission ID
            before_time: Optional timestamp to check before this time

        Returns:
            True if on mechanical ventilation
        """
        conn = self._get_connection()

        # Check procedureevents for ventilation procedures
        time_constraint = f"AND starttime <= '{before_time}'" if before_time else ""

        # Common mechanical ventilation itemids
        vent_itemids = [225792, 225794]  # Intubation, invasive ventilation

        query = f"""
        SELECT COUNT(*) as vent_count
        FROM procedureevents
        WHERE hadm_id = ?
            AND itemid IN ({','.join(map(str, vent_itemids))})
            {time_constraint}
        """

        result = pd.read_sql_query(query, conn, params=(hadm_id,))
        conn.close()

        return result.iloc[0]["vent_count"] > 0

    def extract_features_for_admission(
        self, hadm_id: str, before_time: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract all 23 features for a hospital admission

        Args:
            hadm_id: Hospital admission ID
            before_time: Optional timestamp to extract features before this time
                        (useful for temporal validation)

        Returns:
            Dictionary with all 23 features
        """
        # Extract demographics
        demographics = self.extract_demographics(hadm_id)

        # Extract comorbidities
        comorbidities = self.extract_comorbidities(hadm_id)

        # Extract vitals
        vitals = self.extract_latest_vitals(hadm_id, before_time)

        # Extract labs
        labs = self.extract_latest_labs(hadm_id, before_time)

        # Check mechanical ventilation
        mechanical_vent = self.check_mechanical_ventilation(hadm_id, before_time)

        # Calculate SOFA score
        sofa_vitals = {
            "spo2": vitals.get("spo2"),
            "mechanical_vent": mechanical_vent,
            "platelets": labs.get("platelets"),
            "bilirubin": labs.get("bilirubin"),
            "mean_arterial_pressure": vitals.get("mean_bp"),
            "gcs": vitals.get("gcs"),
            "creatinine": labs.get("creatinine"),
        }

        sofa_scores = self.sofa_calc.calculate_total_sofa(sofa_vitals)

        # Combine all features
        features = {
            # Demographics (2)
            "age": demographics["age"],
            "admission_location": demographics["admission_location"],
            # Comorbidities (7)
            **comorbidities,
            # Vitals (8)
            "heart_rate": vitals.get("heart_rate"),
            "sbp": vitals.get("sbp"),
            "dbp": vitals.get("dbp"),
            "map": vitals.get("mean_bp"),
            "resp_rate": vitals.get("resp_rate"),
            "spo2": vitals.get("spo2"),
            "gcs": vitals.get("gcs"),
            "temperature": vitals.get("temperature"),
            # Labs (2)
            "paco2": labs.get("paco2"),
            "bilirubin": labs.get("bilirubin"),
            # SOFA (3)
            "sofa_total": sofa_scores["sofa_total"],
            "sofa_change": 0,  # Will be calculated with temporal data
            "organ_dysfunction": 1 if sofa_scores["sofa_total"] >= 2 else 0,
            # Mechanical ventilation (1)
            "mechanical_vent": 1 if mechanical_vent else 0,
        }

        return features

    def extract_dataset(
        self, hadm_ids: Optional[List[str]] = None, max_admissions: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract features for multiple admissions

        Args:
            hadm_ids: Optional list of admission IDs to extract
            max_admissions: Maximum number of admissions to extract (if hadm_ids not provided)

        Returns:
            DataFrame with features for all admissions
        """
        conn = self._get_connection()

        # Get admission IDs if not provided
        if hadm_ids is None:
            if max_admissions:
                query = f"SELECT hadm_id FROM admissions LIMIT {max_admissions}"
            else:
                query = "SELECT hadm_id FROM admissions"

            result = pd.read_sql_query(query, conn)
            hadm_ids = result["hadm_id"].astype(str).tolist()

        conn.close()

        # Extract features for each admission
        all_features = []
        for hadm_id in hadm_ids:
            try:
                features = self.extract_features_for_admission(hadm_id)
                features["hadm_id"] = hadm_id
                all_features.append(features)
            except Exception as e:
                print(f"Error extracting features for {hadm_id}: {e}")
                continue

        # Convert to DataFrame
        df = pd.DataFrame(all_features)

        return df


def main():
    """Example usage: Extract features for a single patient"""
    db_path = "/Users/tzuhan/tzuhan_Files/workspace/GCP_confluent/Aorta/_data/mimic_demo.db"
    extractor = FeatureExtractor(db_path)

    # Example: Extract features for first admission
    conn = sqlite3.connect(db_path)
    result = pd.read_sql_query("SELECT hadm_id FROM admissions LIMIT 1", conn)
    conn.close()

    if not result.empty:
        hadm_id = str(result.iloc[0]["hadm_id"])
        print(f"Extracting features for admission {hadm_id}...")

        features = extractor.extract_features_for_admission(hadm_id)

        print("\nExtracted features:")
        for feature, value in features.items():
            print(f"  {feature}: {value}")


if __name__ == "__main__":
    main()
