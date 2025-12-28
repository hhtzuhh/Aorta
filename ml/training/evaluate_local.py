"""
Model Evaluation Script

Evaluates trained sepsis prediction model with detailed metrics:
- Classification report
- Confusion matrix
- ROC curve
- Feature importance analysis
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_curve,
    roc_auc_score,
    precision_recall_curve,
    average_precision_score,
)
import joblib
import json

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from ml.features.feature_extractor import FeatureExtractor
from ml.features.label_generator import LabelGenerator


class ModelEvaluator:
    """Evaluate trained sepsis prediction model"""

    def __init__(self, model_dir: str, db_path: str):
        """
        Initialize evaluator

        Args:
            model_dir: Directory containing model artifacts
            db_path: Path to MIMIC-IV SQLite database
        """
        self.model_dir = Path(model_dir)
        self.db_path = db_path

        # Load model artifacts
        print("Loading model artifacts...")
        self.model = joblib.load(self.model_dir / "sepsis_model.pkl")
        self.imputer = joblib.load(self.model_dir / "imputer.pkl")
        self.label_encoder = joblib.load(self.model_dir / "label_encoder.pkl")

        with open(self.model_dir / "feature_names.json", "r") as f:
            self.feature_names = json.load(f)

        print(f"  Loaded model from {self.model_dir}")

    def prepare_test_data(self, max_admissions: int = None):
        """
        Prepare test data

        Args:
            max_admissions: Maximum number of admissions to evaluate

        Returns:
            Tuple of (X_test, y_test)
        """
        print("\nPreparing test data...")

        extractor = FeatureExtractor(self.db_path)
        generator = LabelGenerator(self.db_path)

        # Extract features
        features_df = extractor.extract_dataset(max_admissions=max_admissions)

        # Generate labels
        labels_df = generator.generate_labels(hadm_ids=features_df["hadm_id"].astype(str).tolist())

        # Merge
        data = features_df.merge(labels_df, on="hadm_id")

        # Separate features and labels
        X = data.drop(columns=["hadm_id", "sepsis"])
        y = data["sepsis"]

        # Encode categorical features
        for col in X.select_dtypes(include=["object"]).columns:
            if col in self.label_encoder:
                X[col] = X[col].fillna("MISSING")
                X[col] = self.label_encoder[col].transform(X[col])

        # Impute missing values
        X = pd.DataFrame(
            self.imputer.transform(X),
            columns=X.columns,
            index=X.index,
        )

        return X, y

    def plot_confusion_matrix(self, y_true, y_pred, output_path: Path = None):
        """
        Plot confusion matrix

        Args:
            y_true: True labels
            y_pred: Predicted labels
            output_path: Path to save plot
        """
        cm = confusion_matrix(y_true, y_pred)

        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False)
        plt.title("Confusion Matrix")
        plt.ylabel("True Label")
        plt.xlabel("Predicted Label")
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path)
            print(f"  Saved confusion matrix: {output_path}")
        else:
            plt.show()

        plt.close()

    def plot_roc_curve(self, y_true, y_pred_proba, output_path: Path = None):
        """
        Plot ROC curve

        Args:
            y_true: True labels
            y_pred_proba: Predicted probabilities
            output_path: Path to save plot
        """
        fpr, tpr, thresholds = roc_curve(y_true, y_pred_proba)
        roc_auc = roc_auc_score(y_true, y_pred_proba)

        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, color="darkorange", lw=2, label=f"ROC curve (AUC = {roc_auc:.4f})")
        plt.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--", label="Random")
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title("Receiver Operating Characteristic (ROC) Curve")
        plt.legend(loc="lower right")
        plt.grid(alpha=0.3)
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path)
            print(f"  Saved ROC curve: {output_path}")
        else:
            plt.show()

        plt.close()

    def plot_precision_recall_curve(self, y_true, y_pred_proba, output_path: Path = None):
        """
        Plot precision-recall curve

        Args:
            y_true: True labels
            y_pred_proba: Predicted probabilities
            output_path: Path to save plot
        """
        precision, recall, thresholds = precision_recall_curve(y_true, y_pred_proba)
        avg_precision = average_precision_score(y_true, y_pred_proba)

        plt.figure(figsize=(8, 6))
        plt.plot(recall, precision, color="darkorange", lw=2, label=f"PR curve (AP = {avg_precision:.4f})")
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel("Recall")
        plt.ylabel("Precision")
        plt.title("Precision-Recall Curve")
        plt.legend(loc="lower left")
        plt.grid(alpha=0.3)
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path)
            print(f"  Saved PR curve: {output_path}")
        else:
            plt.show()

        plt.close()

    def plot_feature_importance(self, top_n: int = 20, output_path: Path = None):
        """
        Plot feature importance

        Args:
            top_n: Number of top features to display
            output_path: Path to save plot
        """
        # Get feature importance
        importance = self.model.feature_importances_
        feature_importance_df = pd.DataFrame(
            {"feature": self.feature_names, "importance": importance}
        ).sort_values("importance", ascending=False)

        # Plot top N features
        plt.figure(figsize=(10, 8))
        top_features = feature_importance_df.head(top_n)
        sns.barplot(data=top_features, x="importance", y="feature", palette="viridis")
        plt.title(f"Top {top_n} Feature Importances")
        plt.xlabel("Importance")
        plt.ylabel("Feature")
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path)
            print(f"  Saved feature importance: {output_path}")
        else:
            plt.show()

        plt.close()

        return feature_importance_df

    def evaluate(self, max_admissions: int = None, save_plots: bool = True):
        """
        Complete evaluation pipeline

        Args:
            max_admissions: Maximum number of admissions to evaluate
            save_plots: Whether to save plots to disk
        """
        # Prepare test data
        X_test, y_test = self.prepare_test_data(max_admissions)

        print(f"\nTest set size: {len(X_test)}")
        print(f"Sepsis cases: {y_test.sum()} ({y_test.mean():.2%})")

        # Make predictions
        print("\nMaking predictions...")
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]

        # Calculate metrics
        print("\n" + "=" * 60)
        print("EVALUATION RESULTS")
        print("=" * 60)

        roc_auc = roc_auc_score(y_test, y_pred_proba)
        avg_precision = average_precision_score(y_test, y_pred_proba)

        print(f"\nROC-AUC Score: {roc_auc:.4f}")
        print(f"Average Precision: {avg_precision:.4f}")

        print("\nClassification Report:")
        print(classification_report(y_test, y_pred, target_names=["No Sepsis", "Sepsis"]))

        print("\nConfusion Matrix:")
        cm = confusion_matrix(y_test, y_pred)
        print(cm)

        # Generate plots
        if save_plots:
            print("\nGenerating evaluation plots...")
            plots_dir = self.model_dir / "evaluation"
            plots_dir.mkdir(exist_ok=True)

            self.plot_confusion_matrix(y_test, y_pred, plots_dir / "confusion_matrix.png")
            self.plot_roc_curve(y_test, y_pred_proba, plots_dir / "roc_curve.png")
            self.plot_precision_recall_curve(y_test, y_pred_proba, plots_dir / "pr_curve.png")
            feature_importance = self.plot_feature_importance(top_n=20, output_path=plots_dir / "feature_importance.png")

            # Save feature importance to CSV
            feature_importance.to_csv(plots_dir / "feature_importance.csv", index=False)
            print(f"  Saved feature importance CSV: {plots_dir / 'feature_importance.csv'}")

        print("\n" + "=" * 60)
        print("Evaluation complete!")
        print("=" * 60)


def main():
    """Main evaluation script"""
    import argparse

    parser = argparse.ArgumentParser(description="Evaluate sepsis prediction model")
    parser.add_argument(
        "--model-dir",
        type=str,
        default="ml/models/local",
        help="Directory containing model artifacts",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/Users/tzuhan/tzuhan_Files/workspace/GCP_confluent/Aorta/_data/mimic_demo.db",
        help="Path to MIMIC-IV SQLite database",
    )
    parser.add_argument(
        "--max-admissions",
        type=int,
        default=None,
        help="Maximum number of admissions to evaluate (None = all)",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip saving plots",
    )

    args = parser.parse_args()

    # Evaluate model
    evaluator = ModelEvaluator(args.model_dir, args.db_path)
    evaluator.evaluate(max_admissions=args.max_admissions, save_plots=not args.no_plots)


if __name__ == "__main__":
    main()
