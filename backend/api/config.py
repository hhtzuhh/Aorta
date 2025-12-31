"""
Configuration management for Aorta Backend API

Loads Kafka credentials and application settings from environment
or configuration files.
"""

import json
import os
from pathlib import Path
from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""

    # Kafka Configuration
    kafka_bootstrap_servers: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_security_protocol: str = "SASL_SSL"
    kafka_topic: str = "hospital-admissions"
    kafka_group_id: str = "aorta-dashboard-consumer-v2"  # Changed to force fresh start

    # Application Settings
    cors_origins: List[str] = ["*"]  # Allow all origins (can be restricted via env var)
    max_recent_admissions: int = 50

    # RAG Configuration (MongoDB Atlas + Gemini)
    mongodb_connection_string: str = ""
    mongodb_username: str = ""
    mongodb_password: str = ""
    mongodb_database: str = "sepsis_guidelines"
    mongodb_collection: str = "guideline_chunks"
    gemini_api_key: str = ""
    rag_enabled: bool = True
    rag_probability_threshold: float = 0.5

    # Environment
    environment: str = "development"

    class Config:
        env_prefix = "AORTA_"
        case_sensitive = False

    @classmethod
    def load_from_kafka_config(cls, config_path: str = "_data/kafka_config.json"):
        """Load Kafka configuration from JSON file"""

        # Try multiple paths to find the config file
        # 1. Relative to current directory (when running from Aorta/)
        # 2. Relative to backend directory (when running from Aorta/backend)
        # 3. Relative to this file's location

        possible_paths = [
            Path(config_path),                                    # _data/kafka_config.json
            Path("..") / config_path,                            # ../_data/kafka_config.json
            Path(__file__).parent.parent.parent / config_path,   # Aorta/_data/kafka_config.json
        ]

        config_file = None
        for path in possible_paths:
            if path.exists():
                config_file = path
                break

        if not config_file:
            raise FileNotFoundError(
                f"Kafka config not found. Tried: {[str(p) for p in possible_paths]}. "
                f"Run: terraform output -json kafka_config > _data/kafka_config.json"
            )

        with open(config_file) as f:
            kafka_config = json.load(f)

        # Try to load RAG config (optional)
        rag_config_path = "_data/rag_config.json"
        rag_possible_paths = [
            Path(rag_config_path),
            Path("..") / rag_config_path,
            Path(__file__).parent.parent.parent / rag_config_path,
        ]

        rag_settings = {}
        for path in rag_possible_paths:
            if path.exists():
                with open(path) as f:
                    rag_config = json.load(f)
                    rag_settings = {
                        "mongodb_connection_string": rag_config.get("mongodb_connection_string", ""),
                        "mongodb_username": rag_config.get("mongodb_username", ""),
                        "mongodb_password": rag_config.get("mongodb_password", ""),
                        "mongodb_database": rag_config.get("mongodb_database", "sepsis_guidelines"),
                        "mongodb_collection": rag_config.get("mongodb_collection", "guideline_chunks"),
                        "gemini_api_key": rag_config.get("gemini_api_key", ""),
                        "rag_enabled": rag_config.get("rag_enabled", True),
                        "rag_probability_threshold": rag_config.get("rag_probability_threshold", 0.5),
                    }
                break

        # Map JSON config to Settings
        return cls(
            kafka_bootstrap_servers=kafka_config.get("bootstrap_servers", ""),
            kafka_sasl_username=kafka_config.get("sasl_username", ""),
            kafka_sasl_password=kafka_config.get("sasl_password", ""),
            kafka_sasl_mechanism=kafka_config.get("sasl_mechanism", "PLAIN"),
            kafka_security_protocol=kafka_config.get("security_protocol", "SASL_SSL"),
            **rag_settings,
        )


# Global settings instance
def get_settings() -> Settings:
    """Get settings instance - loads from JSON in dev, env vars in prod"""

    # Check if running in production (Cloud Run sets PORT)
    if os.getenv("PORT"):
        # Production: load from environment variables
        return Settings()
    else:
        # Development: load from _data/kafka_config.json
        return Settings.load_from_kafka_config()


settings = get_settings()
