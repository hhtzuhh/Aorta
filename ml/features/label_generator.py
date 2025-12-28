"""
Label Generator for Sepsis Prediction

Generates binary labels (sepsis/no sepsis) for hospital admissions
based on ICD-9/ICD-10 diagnosis codes.

Sepsis ICD codes:
- ICD-9: 995.91, 995.92, 038.x, 785.52
- ICD-10: A40.x, A41.x, R65.20, R65.21
"""

import sqlite3
import pandas as pd
from typing import List, Optional


class LabelGenerator:
    """Generate sepsis labels from ICD diagnosis codes"""

    # Sepsis ICD code patterns (without decimal points, leading zeros may be stripped)
    SEPSIS_ICD9_CODES = [
        "99591",  # Sepsis
        "99592",  # Severe sepsis
        "9959%",  # Sepsis family
        "038%",   # Septicemia (handles 0380, 0388, 0389, etc.)
        "78552",  # Septic shock
        "7855%",  # Septic shock family
    ]

    SEPSIS_ICD10_CODES = [
        "A40%",   # Streptococcal sepsis
        "A41%",   # Other sepsis
        "R652%",  # Severe sepsis
    ]

    def __init__(self, db_path: str):
        """
        Initialize label generator

        Args:
            db_path: Path to MIMIC-IV SQLite database
        """
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Create database connection"""
        return sqlite3.connect(self.db_path)

    def has_sepsis_diagnosis(self, hadm_id: str) -> bool:
        """
        Check if admission has sepsis diagnosis

        Args:
            hadm_id: Hospital admission ID

        Returns:
            True if admission has sepsis diagnosis
        """
        conn = self._get_connection()

        # Combine ICD-9 and ICD-10 patterns
        all_patterns = self.SEPSIS_ICD9_CODES + self.SEPSIS_ICD10_CODES
        like_clauses = " OR ".join([f"icd_code LIKE '{pattern}'" for pattern in all_patterns])

        query = f"""
        SELECT COUNT(*) as sepsis_count
        FROM diagnoses_icd
        WHERE hadm_id = ? AND ({like_clauses})
        """

        result = pd.read_sql_query(query, conn, params=(hadm_id,))
        conn.close()

        return result.iloc[0]["sepsis_count"] > 0

    def generate_labels(self, hadm_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Generate sepsis labels for multiple admissions

        Args:
            hadm_ids: Optional list of admission IDs. If None, generates for all admissions.

        Returns:
            DataFrame with hadm_id and sepsis label (0/1)
        """
        conn = self._get_connection()

        # Get all admission IDs if not provided
        if hadm_ids is None:
            result = pd.read_sql_query("SELECT hadm_id FROM admissions", conn)
            hadm_ids = result["hadm_id"].astype(str).tolist()

        conn.close()

        # Generate labels
        labels = []
        for hadm_id in hadm_ids:
            has_sepsis = self.has_sepsis_diagnosis(hadm_id)
            labels.append({"hadm_id": hadm_id, "sepsis": 1 if has_sepsis else 0})

        return pd.DataFrame(labels)

    def get_class_distribution(self) -> pd.Series:
        """
        Get distribution of sepsis cases

        Returns:
            Series with counts for each class
        """
        labels_df = self.generate_labels()
        return labels_df["sepsis"].value_counts()


def main():
    """Example usage"""
    db_path = "/Users/tzuhan/tzuhan_Files/workspace/GCP_confluent/Aorta/_data/mimic_demo.db"
    generator = LabelGenerator(db_path)

    print("Generating labels for all admissions...")
    labels = generator.generate_labels()

    print(f"\nTotal admissions: {len(labels)}")
    print("\nClass distribution:")
    print(labels["sepsis"].value_counts())
    print(f"\nSepsis prevalence: {labels['sepsis'].mean():.2%}")


if __name__ == "__main__":
    main()
