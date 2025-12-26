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
    cors_origins: List[str] = ["http://localhost:5173"]
    max_recent_admissions: int = 50

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

        # Map JSON config to Settings
        return cls(
            kafka_bootstrap_servers=kafka_config.get("bootstrap_servers", ""),
            kafka_sasl_username=kafka_config.get("sasl_username", ""),
            kafka_sasl_password=kafka_config.get("sasl_password", ""),
            kafka_sasl_mechanism=kafka_config.get("sasl_mechanism", "PLAIN"),
            kafka_security_protocol=kafka_config.get("security_protocol", "SASL_SSL"),
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
