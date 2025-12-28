"""
Local Training Script for Sepsis Prediction Model

Trains XGBoost model on MIMIC-IV demo SQLite database.
Follows the notebook approach:
1. Extract features from SQLite
2. Generate labels
3. Encode categorical features
4. Impute missing values
5. Handle class imbalance with RandomUnderSampler
6. Hyperparameter tuning with BayesSearchCV
7. Save model artifacts
"""

import sys
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
from imblearn.under_sampling import RandomUnderSampler
from skopt import BayesSearchCV
from skopt.space import Real, Integer
import xgboost as xgb
import joblib
import json

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from ml.features.feature_extractor import FeatureExtractor
from ml.features.label_generator import LabelGenerator


class SepsisModelTrainer:
    """Train sepsis prediction model on local SQLite data"""

    def __init__(self, db_path: str, output_dir: str = "ml/models/local"):
        """
        Initialize trainer

        Args:
            db_path: Path to MIMIC-IV SQLite database
            output_dir: Directory to save model artifacts
        """
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.feature_extractor = FeatureExtractor(db_path)
        self.label_generator = LabelGenerator(db_path)

        # Model components
        self.label_encoder = None
        self.imputer = None
        self.model = None
        self.feature_names = None

    def load_and_prepare_data(self, max_admissions: int = None) -> tuple:
        """
        Load data and prepare for training

        Args:
            max_admissions: Maximum number of admissions to use

        Returns:
            Tuple of (X, y, feature_names)
        """
        print("Extracting features from database...")
        features_df = self.feature_extractor.extract_dataset(max_admissions=max_admissions)

        print("Generating labels...")
        labels_df = self.label_generator.generate_labels(
            hadm_ids=features_df["hadm_id"].astype(str).tolist()
        )

        # Merge features and labels
        data = features_df.merge(labels_df, on="hadm_id")

        print(f"\nDataset summary:")
        print(f"  Total admissions: {len(data)}")
        print(f"  Sepsis cases: {data['sepsis'].sum()} ({data['sepsis'].mean():.2%})")
        print(f"  Non-sepsis cases: {(data['sepsis'] == 0).sum()}")

        # Separate features and labels
        X = data.drop(columns=["hadm_id", "sepsis"])
        y = data["sepsis"]

        return X, y

    def encode_categorical_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Encode categorical features with LabelEncoder

        Args:
            X: Feature DataFrame

        Returns:
            Encoded DataFrame
        """
        print("\nEncoding categorical features...")

        X_encoded = X.copy()
        categorical_cols = X.select_dtypes(include=["object"]).columns

        self.label_encoder = {}
        for col in categorical_cols:
            le = LabelEncoder()
            # Handle NaN values
            X_encoded[col] = X_encoded[col].fillna("MISSING")
            X_encoded[col] = le.fit_transform(X_encoded[col])
            self.label_encoder[col] = le
            print(f"  Encoded {col}: {len(le.classes_)} categories")

        return X_encoded

    def impute_missing_values(self, X_train: pd.DataFrame, X_test: pd.DataFrame) -> tuple:
        """
        Impute missing values using IterativeImputer

        Args:
            X_train: Training features
            X_test: Test features

        Returns:
            Tuple of (X_train_imputed, X_test_imputed)
        """
        print("\nImputing missing values...")

        # Calculate missing percentages
        missing_pct = (X_train.isnull().sum() / len(X_train) * 100).sort_values(ascending=False)
        print("\nMissing value percentages (top 10):")
        print(missing_pct.head(10))

        # Fit imputer on training data
        self.imputer = IterativeImputer(
            max_iter=10,
            random_state=42,
            verbose=0,
        )

        X_train_imputed = pd.DataFrame(
            self.imputer.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index,
        )

        X_test_imputed = pd.DataFrame(
            self.imputer.transform(X_test),
            columns=X_test.columns,
            index=X_test.index,
        )

        print(f"  Imputation complete. Remaining NaNs in train: {X_train_imputed.isnull().sum().sum()}")

        return X_train_imputed, X_test_imputed

    def handle_class_imbalance(self, X_train: pd.DataFrame, y_train: pd.Series) -> tuple:
        """
        Handle class imbalance using RandomUnderSampler

        Args:
            X_train: Training features
            y_train: Training labels

        Returns:
            Tuple of (X_resampled, y_resampled)
        """
        print("\nHandling class imbalance...")
        print(f"  Before resampling: {y_train.value_counts().to_dict()}")

        # Check if we have both classes
        if len(y_train.unique()) < 2:
            print("  WARNING: Only one class present. Skipping resampling.")
            return X_train, y_train

        rus = RandomUnderSampler(random_state=42)
        X_resampled, y_resampled = rus.fit_resample(X_train, y_train)

        print(f"  After resampling: {pd.Series(y_resampled).value_counts().to_dict()}")

        return X_resampled, y_resampled

    def train_model(self, X_train: pd.DataFrame, y_train: pd.Series) -> xgb.XGBClassifier:
        """
        Train XGBoost model with Bayesian hyperparameter optimization

        Args:
            X_train: Training features
            y_train: Training labels

        Returns:
            Trained model
        """
        print("\nTraining XGBoost model with Bayesian optimization...")

        # Define search space
        search_spaces = {
            "learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "max_depth": Integer(3, 10),
            "min_child_weight": Integer(1, 10),
            "subsample": Real(0.6, 1.0),
            "colsample_bytree": Real(0.6, 1.0),
            "gamma": Real(0, 5),
            "n_estimators": Integer(50, 300),
        }

        # Base XGBoost classifier
        xgb_model = xgb.XGBClassifier(
            objective="binary:logistic",
            eval_metric="auc",
            use_label_encoder=False,
            random_state=42,
        )

        # Bayesian optimization
        bayes_search = BayesSearchCV(
            xgb_model,
            search_spaces,
            n_iter=30,  # Reduced from notebook's 30 for faster training
            cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
            scoring="roc_auc",
            n_jobs=-1,
            random_state=42,
            verbose=1,
        )

        bayes_search.fit(X_train, y_train)

        print(f"\n  Best ROC-AUC: {bayes_search.best_score_:.4f}")
        print(f"  Best parameters: {bayes_search.best_params_}")

        self.model = bayes_search.best_estimator_

        return self.model

    def evaluate_model(self, X_test: pd.DataFrame, y_test: pd.Series):
        """
        Evaluate model on test set

        Args:
            X_test: Test features
            y_test: Test labels
        """
        print("\nEvaluating model on test set...")

        # Predictions
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]

        # Metrics
        roc_auc = roc_auc_score(y_test, y_pred_proba)
        print(f"\n  ROC-AUC: {roc_auc:.4f}")

        print("\n  Classification Report:")
        print(classification_report(y_test, y_pred))

        print("\n  Confusion Matrix:")
        print(confusion_matrix(y_test, y_pred))

    def save_artifacts(self):
        """Save model artifacts to disk"""
        print(f"\nSaving model artifacts to {self.output_dir}...")

        # Save model
        model_path = self.output_dir / "sepsis_model.pkl"
        joblib.dump(self.model, model_path)
        print(f"  Saved model: {model_path}")

        # Save imputer
        imputer_path = self.output_dir / "imputer.pkl"
        joblib.dump(self.imputer, imputer_path)
        print(f"  Saved imputer: {imputer_path}")

        # Save label encoder
        encoder_path = self.output_dir / "label_encoder.pkl"
        joblib.dump(self.label_encoder, encoder_path)
        print(f"  Saved label encoder: {encoder_path}")

        # Save feature names
        feature_names_path = self.output_dir / "feature_names.json"
        with open(feature_names_path, "w") as f:
            json.dump(self.feature_names, f, indent=2)
        print(f"  Saved feature names: {feature_names_path}")

        print("\n  All artifacts saved successfully!")

    def train(self, max_admissions: int = None):
        """
        Complete training pipeline

        Args:
            max_admissions: Maximum number of admissions to use
        """
        # Load data
        X, y = self.load_and_prepare_data(max_admissions)
        self.feature_names = X.columns.tolist()

        # Encode categorical features
        X = self.encode_categorical_features(X)

        # Split data
        print("\nSplitting data (80/20 train/test)...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Impute missing values
        X_train, X_test = self.impute_missing_values(X_train, X_test)

        # Handle class imbalance
        X_train, y_train = self.handle_class_imbalance(X_train, y_train)

        # Train model
        self.train_model(X_train, y_train)

        # Evaluate model
        self.evaluate_model(X_test, y_test)

        # Save artifacts
        self.save_artifacts()


def main():
    """Main training script"""
    import argparse

    parser = argparse.ArgumentParser(description="Train sepsis prediction model")
    parser.add_argument(
        "--db-path",
        type=str,
        default="/Users/tzuhan/tzuhan_Files/workspace/GCP_confluent/Aorta/_data/mimic_demo.db",
        help="Path to MIMIC-IV SQLite database",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="ml/models/local",
        help="Directory to save model artifacts",
    )
    parser.add_argument(
        "--max-admissions",
        type=int,
        default=None,
        help="Maximum number of admissions to use (None = all)",
    )

    args = parser.parse_args()

    # Train model
    trainer = SepsisModelTrainer(args.db_path, args.output_dir)
    trainer.train(max_admissions=args.max_admissions)


if __name__ == "__main__":
    main()
