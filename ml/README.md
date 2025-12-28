# Sepsis Prediction ML System

Machine learning system for predicting sepsis 6 hours before onset, integrated with Aorta streaming pipeline.

## Phase 1: Local Training (COMPLETED)

### Overview
Train XGBoost model on MIMIC-IV demo SQLite database with 23 clinical features.

### Components Created

#### 1. Feature Engineering
- **sofa_calculator.py**: SOFA (Sequential Organ Failure Assessment) score calculator
- **feature_extractor.py**: Extracts 23 features from SQLite database
- **label_generator.py**: Generates sepsis labels from ICD diagnosis codes

#### 2. Model Training
- **train_local.py**: Complete training pipeline with:
  - Feature extraction and encoding
  - Missing value imputation (IterativeImputer)
  - Class imbalance handling (RandomUnderSampler)
  - Hyperparameter tuning (BayesSearchCV)
  - Model artifact saving

#### 3. Model Evaluation
- **evaluate_local.py**: Comprehensive evaluation with:
  - ROC-AUC and precision-recall metrics
  - Confusion matrix visualization
  - Feature importance analysis
  - Performance plots

### Quick Start

#### 1. Install Dependencies
```bash
cd Aorta
uv sync
```

#### 2. Train Model
```bash
# Train on all admissions (275 from demo DB)
python -m ml.training.train_local

# Train on subset for faster testing
python -m ml.training.train_local --max-admissions 100
```

Expected output:
- Model artifacts saved to `ml/models/local/`:
  - `sepsis_model.pkl` - Trained XGBoost model
  - `imputer.pkl` - Fitted imputer for missing values
  - `label_encoder.pkl` - Label encoders for categorical features
  - `feature_names.json` - List of feature names

#### 3. Evaluate Model
```bash
python -m ml.training.evaluate_local
```

Expected output:
- Evaluation plots in `ml/models/local/evaluation/`:
  - `confusion_matrix.png`
  - `roc_curve.png`
  - `pr_curve.png`
  - `feature_importance.png`
  - `feature_importance.csv`

#### 4. Test Feature Extraction
```bash
# Extract features for a single admission
python -m ml.features.feature_extractor

# Check label distribution
python -m ml.features.label_generator
```

### 23 Features Extracted

#### Demographics (2)
1. age
2. admission_location

#### Comorbidities (7)
3. diabetes
4. hypertension
5. chronic_kidney_disease
6. congestive_heart_failure
7. copd
8. cancer
9. liver_disease

#### Vitals (8)
10. heart_rate
11. sbp (systolic blood pressure)
12. dbp (diastolic blood pressure)
13. map (mean arterial pressure)
14. resp_rate
15. spo2
16. gcs (Glasgow Coma Scale)
17. temperature

#### Labs (2)
18. paco2
19. bilirubin

#### SOFA Components (3)
20. sofa_total
21. sofa_change
22. organ_dysfunction

#### Mechanical Ventilation (1)
23. mechanical_vent

### Expected Performance

Based on the MIMIC-IV demo dataset:
- **Target ROC-AUC**: > 0.75
- **Class imbalance**: Typically 5-10% sepsis prevalence
- **Missing data**: ~20-40% for some lab values (handled by imputer)

Note: Performance will be lower than the full production model (0.85+ AUC) due to the small demo dataset size (275 admissions vs. 6M+ in full MIMIC-IV).

### Troubleshooting

#### Issue: "No module named 'ml'"
Solution: Run from the Aorta directory and use `-m` flag:
```bash
cd Aorta
python -m ml.training.train_local
```

#### Issue: "Database is empty"
Solution: Ensure you're using the correct database:
```bash
python -m ml.training.train_local --db-path /path/to/Aorta/_data/mimic_demo.db
```

#### Issue: "Insufficient data for training"
Solution: Check that the database has admissions:
```bash
sqlite3 _data/mimic_demo.db "SELECT COUNT(*) FROM admissions;"
```

### Next Steps

- **Phase 2**: Train on full BigQuery MIMIC-IV dataset for production model
- **Phase 3**: Deploy to real-time streaming pipeline with Kafka consumers

See `docs/ML/sepsis_implementation_plan.md` for complete implementation roadmap.
