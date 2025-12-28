"""
Comorbidity Service - Fast lookup of comorbidity flags from ICD codes

Precomputes comorbidity flags for all admissions on startup for O(1) lookup.
"""

import sqlite3
import logging
from typing import Dict
from pathlib import Path

logger = logging.getLogger(__name__)


class ComorbidityService:
    """
    Precompute and cache comorbidity flags for all admissions

    ICD code patterns used:
    - Diabetes: 250% (ICD-9), E10-E14% (ICD-10)
    - Hypertension: 401% (ICD-9), I10% (ICD-10)
    - Chronic Kidney Disease: 585% (ICD-9), N18% (ICD-10)
    - Congestive Heart Failure: 428% (ICD-9), I50% (ICD-10)
    - COPD: 491%, 492%, 496% (ICD-9), J44% (ICD-10)
    - Cancer: 140-239% (ICD-9), C% (ICD-10)
    - Liver Disease: 571% (ICD-9), K70-K77% (ICD-10)
    """

    def __init__(self, db_path: str):
        """
        Initialize comorbidity service

        Args:
            db_path: Path to SQLite database

        Raises:
            FileNotFoundError: If database doesn't exist
        """
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        self.db_path = db_path
        self.cache: Dict[str, Dict[str, bool]] = {}
        self._precompute()

    def _precompute(self):
        """
        Precompute comorbidity flags for all admissions

        Queries diagnoses_icd table and builds cache of flags.
        """
        logger.info("Precomputing comorbidity flags...")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Query to extract comorbidity flags using ICD code patterns
        # Note: MIMIC-IV stores ICD codes without decimals and strips leading zeros
        query = """
        SELECT
            hadm_id,
            MAX(CASE
                WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%'
                    OR icd_code LIKE 'E12%' OR icd_code LIKE 'E13%' OR icd_code LIKE 'E14%'
                THEN 1 ELSE 0
            END) AS diabetes,

            MAX(CASE
                WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%'
                THEN 1 ELSE 0
            END) AS hypertension,

            MAX(CASE
                WHEN icd_code LIKE '585%' OR icd_code LIKE 'N18%'
                THEN 1 ELSE 0
            END) AS chronic_kidney_disease,

            MAX(CASE
                WHEN icd_code LIKE '428%' OR icd_code LIKE 'I50%'
                THEN 1 ELSE 0
            END) AS congestive_heart_failure,

            MAX(CASE
                WHEN icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '496%'
                    OR icd_code LIKE 'J44%'
                THEN 1 ELSE 0
            END) AS copd,

            MAX(CASE
                WHEN (icd_code >= '140' AND icd_code < '240') OR icd_code LIKE 'C%' OR icd_code LIKE 'D0%'
                THEN 1 ELSE 0
            END) AS cancer,

            MAX(CASE
                WHEN icd_code LIKE '571%' OR icd_code LIKE 'K70%' OR icd_code LIKE 'K71%'
                    OR icd_code LIKE 'K72%' OR icd_code LIKE 'K73%' OR icd_code LIKE 'K74%'
                    OR icd_code LIKE 'K75%' OR icd_code LIKE 'K76%' OR icd_code LIKE 'K77%'
                THEN 1 ELSE 0
            END) AS liver_disease
        FROM diagnoses_icd
        GROUP BY hadm_id
        """

        cursor = conn.execute(query)
        rows = cursor.fetchall()

        # Build cache
        for row in rows:
            self.cache[str(row["hadm_id"])] = {
                "diabetes": bool(row["diabetes"]),
                "hypertension": bool(row["hypertension"]),
                "chronic_kidney_disease": bool(row["chronic_kidney_disease"]),
                "congestive_heart_failure": bool(row["congestive_heart_failure"]),
                "copd": bool(row["copd"]),
                "cancer": bool(row["cancer"]),
                "liver_disease": bool(row["liver_disease"]),
            }

        conn.close()

        logger.info(f"Precomputed comorbidities for {len(self.cache)} admissions")

        # Log distribution of comorbidities
        if self.cache:
            totals = {
                "diabetes": sum(1 for v in self.cache.values() if v["diabetes"]),
                "hypertension": sum(1 for v in self.cache.values() if v["hypertension"]),
                "chronic_kidney_disease": sum(
                    1 for v in self.cache.values() if v["chronic_kidney_disease"]
                ),
                "congestive_heart_failure": sum(
                    1 for v in self.cache.values() if v["congestive_heart_failure"]
                ),
                "copd": sum(1 for v in self.cache.values() if v["copd"]),
                "cancer": sum(1 for v in self.cache.values() if v["cancer"]),
                "liver_disease": sum(
                    1 for v in self.cache.values() if v["liver_disease"]
                ),
            }
            logger.info(f"Comorbidity distribution: {totals}")

    def get_comorbidities(self, hadm_id: str) -> Dict[str, bool]:
        """
        Get comorbidity flags for an admission

        Args:
            hadm_id: Hospital admission ID

        Returns:
            Dictionary with 7 comorbidity flags (all False if not found)
        """
        return self.cache.get(
            str(hadm_id),
            {
                "diabetes": False,
                "hypertension": False,
                "chronic_kidney_disease": False,
                "congestive_heart_failure": False,
                "copd": False,
                "cancer": False,
                "liver_disease": False,
            },
        )

    def has_any_comorbidity(self, hadm_id: str) -> bool:
        """Check if admission has any comorbidity"""
        comorbidities = self.get_comorbidities(hadm_id)
        return any(comorbidities.values())

    def get_stats(self) -> Dict[str, int]:
        """Get service statistics"""
        return {
            "total_admissions": len(self.cache),
            "admissions_with_comorbidities": sum(
                1 for v in self.cache.values() if any(v.values())
            ),
        }
