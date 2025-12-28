"""
SOFA (Sequential Organ Failure Assessment) Score Calculator

Implements the SOFA scoring system for organ dysfunction assessment.
Based on MIMIC-IV derived SOFA calculations.

Components:
- Respiratory: PaO2/FiO2 ratio (or SpO2/FiO2) + mechanical ventilation
- Coagulation: Platelets
- Liver: Bilirubin
- Cardiovascular: MAP or vasopressor use
- CNS: GCS (Glasgow Coma Scale)
- Renal: Creatinine or urine output
"""

from typing import Optional, Dict, Any


class SOFACalculator:
    """Calculate SOFA score from patient vitals and labs"""

    @staticmethod
    def calculate_respiratory_score(
        pao2: Optional[float] = None,
        fio2: Optional[float] = None,
        spo2: Optional[float] = None,
        mechanical_vent: bool = False,
    ) -> int:
        """
        Respiratory SOFA score (0-4)

        Args:
            pao2: Partial pressure of oxygen (mmHg)
            fio2: Fraction of inspired oxygen (0-1 or 0-100)
            spo2: Oxygen saturation (%)
            mechanical_vent: Whether patient is on mechanical ventilation

        Returns:
            SOFA score 0-4
        """
        # Normalize FiO2 to 0-1 range
        if fio2 is not None and fio2 > 1:
            fio2 = fio2 / 100

        # Calculate PaO2/FiO2 ratio if available
        if pao2 is not None and fio2 is not None and fio2 > 0:
            pf_ratio = pao2 / fio2
        # Otherwise estimate from SpO2/FiO2 (less accurate but useful)
        elif spo2 is not None and fio2 is not None and fio2 > 0:
            # Approximation: SF ratio ≈ PF ratio (with error margin)
            pf_ratio = (spo2 / fio2) * 1.0  # Rough conversion
        else:
            return 0  # Cannot calculate without data

        # SOFA respiratory scoring
        if pf_ratio < 100 and mechanical_vent:
            return 4
        elif pf_ratio < 200 and mechanical_vent:
            return 3
        elif pf_ratio < 300:
            return 2
        elif pf_ratio < 400:
            return 1
        else:
            return 0

    @staticmethod
    def calculate_coagulation_score(platelets: Optional[float] = None) -> int:
        """
        Coagulation SOFA score (0-4)

        Args:
            platelets: Platelet count (10^3/μL)

        Returns:
            SOFA score 0-4
        """
        if platelets is None:
            return 0

        if platelets < 20:
            return 4
        elif platelets < 50:
            return 3
        elif platelets < 100:
            return 2
        elif platelets < 150:
            return 1
        else:
            return 0

    @staticmethod
    def calculate_liver_score(bilirubin: Optional[float] = None) -> int:
        """
        Liver SOFA score (0-4)

        Args:
            bilirubin: Total bilirubin (mg/dL)

        Returns:
            SOFA score 0-4
        """
        if bilirubin is None:
            return 0

        if bilirubin >= 12.0:
            return 4
        elif bilirubin >= 6.0:
            return 3
        elif bilirubin >= 2.0:
            return 2
        elif bilirubin >= 1.2:
            return 1
        else:
            return 0

    @staticmethod
    def calculate_cardiovascular_score(
        mean_arterial_pressure: Optional[float] = None,
        dopamine_dose: Optional[float] = None,
        dobutamine_dose: Optional[float] = None,
        epinephrine_dose: Optional[float] = None,
        norepinephrine_dose: Optional[float] = None,
    ) -> int:
        """
        Cardiovascular SOFA score (0-4)

        Args:
            mean_arterial_pressure: MAP in mmHg
            dopamine_dose: μg/kg/min
            dobutamine_dose: μg/kg/min (any dose)
            epinephrine_dose: μg/kg/min
            norepinephrine_dose: μg/kg/min

        Returns:
            SOFA score 0-4
        """
        # High-dose vasopressors
        if (
            (dopamine_dose is not None and dopamine_dose > 15)
            or (epinephrine_dose is not None and epinephrine_dose > 0.1)
            or (norepinephrine_dose is not None and norepinephrine_dose > 0.1)
        ):
            return 4

        # Medium-dose vasopressors
        if (
            (dopamine_dose is not None and 5 < dopamine_dose <= 15)
            or (epinephrine_dose is not None and 0 < epinephrine_dose <= 0.1)
            or (norepinephrine_dose is not None and 0 < norepinephrine_dose <= 0.1)
        ):
            return 3

        # Low-dose dopamine or any dobutamine
        if (dopamine_dose is not None and 0 < dopamine_dose <= 5) or (
            dobutamine_dose is not None and dobutamine_dose > 0
        ):
            return 2

        # Hypotension
        if mean_arterial_pressure is not None and mean_arterial_pressure < 70:
            return 1

        return 0

    @staticmethod
    def calculate_cns_score(gcs: Optional[float] = None) -> int:
        """
        CNS (Central Nervous System) SOFA score (0-4)

        Args:
            gcs: Glasgow Coma Scale (3-15)

        Returns:
            SOFA score 0-4
        """
        if gcs is None:
            return 0

        if gcs < 6:
            return 4
        elif gcs < 10:
            return 3
        elif gcs < 13:
            return 2
        elif gcs < 15:
            return 1
        else:
            return 0

    @staticmethod
    def calculate_renal_score(
        creatinine: Optional[float] = None, urine_output: Optional[float] = None
    ) -> int:
        """
        Renal SOFA score (0-4)

        Args:
            creatinine: Serum creatinine (mg/dL)
            urine_output: 24-hour urine output (mL)

        Returns:
            SOFA score 0-4
        """
        score = 0

        # Creatinine-based scoring
        if creatinine is not None:
            if creatinine >= 5.0:
                score = max(score, 4)
            elif creatinine >= 3.5:
                score = max(score, 3)
            elif creatinine >= 2.0:
                score = max(score, 2)
            elif creatinine >= 1.2:
                score = max(score, 1)

        # Urine output-based scoring (takes precedence at higher scores)
        if urine_output is not None:
            if urine_output < 200:
                score = max(score, 4)
            elif urine_output < 500:
                score = max(score, 3)

        return score

    def calculate_total_sofa(self, vitals: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate complete SOFA score and components

        Args:
            vitals: Dictionary containing patient vitals and labs:
                - pao2, fio2, spo2, mechanical_vent
                - platelets
                - bilirubin
                - mean_arterial_pressure, dopamine, dobutamine, epinephrine, norepinephrine
                - gcs
                - creatinine, urine_output

        Returns:
            Dictionary with:
                - sofa_respiratory: 0-4
                - sofa_coagulation: 0-4
                - sofa_liver: 0-4
                - sofa_cardiovascular: 0-4
                - sofa_cns: 0-4
                - sofa_renal: 0-4
                - sofa_total: 0-24
        """
        resp = self.calculate_respiratory_score(
            pao2=vitals.get("pao2"),
            fio2=vitals.get("fio2"),
            spo2=vitals.get("spo2"),
            mechanical_vent=vitals.get("mechanical_vent", False),
        )

        coag = self.calculate_coagulation_score(platelets=vitals.get("platelets"))

        liver = self.calculate_liver_score(bilirubin=vitals.get("bilirubin"))

        cardio = self.calculate_cardiovascular_score(
            mean_arterial_pressure=vitals.get("mean_arterial_pressure"),
            dopamine_dose=vitals.get("dopamine"),
            dobutamine_dose=vitals.get("dobutamine"),
            epinephrine_dose=vitals.get("epinephrine"),
            norepinephrine_dose=vitals.get("norepinephrine"),
        )

        cns = self.calculate_cns_score(gcs=vitals.get("gcs"))

        renal = self.calculate_renal_score(
            creatinine=vitals.get("creatinine"), urine_output=vitals.get("urine_output")
        )

        total = resp + coag + liver + cardio + cns + renal

        return {
            "sofa_respiratory": resp,
            "sofa_coagulation": coag,
            "sofa_liver": liver,
            "sofa_cardiovascular": cardio,
            "sofa_cns": cns,
            "sofa_renal": renal,
            "sofa_total": total,
        }

    def calculate_sofa_change(
        self, current_vitals: Dict[str, Any], previous_vitals: Optional[Dict[str, Any]]
    ) -> float:
        """
        Calculate change in SOFA score (for sepsis-3 criteria)

        Args:
            current_vitals: Current patient vitals
            previous_vitals: Previous patient vitals (baseline)

        Returns:
            Change in SOFA score (positive = worsening)
        """
        current_sofa = self.calculate_total_sofa(current_vitals)["sofa_total"]

        if previous_vitals is None:
            # No baseline, assume change from 0
            return current_sofa

        previous_sofa = self.calculate_total_sofa(previous_vitals)["sofa_total"]

        return current_sofa - previous_sofa

    def has_organ_dysfunction(self, vitals: Dict[str, Any]) -> bool:
        """
        Determine if patient has organ dysfunction (SOFA >= 2)

        Args:
            vitals: Patient vitals and labs

        Returns:
            True if SOFA >= 2
        """
        sofa = self.calculate_total_sofa(vitals)
        return sofa["sofa_total"] >= 2
