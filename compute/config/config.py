"""
Configuration management for Rosetta Compute Engine.

Loads environment variables and provides configuration access.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from functools import lru_cache
from compute.core.security import decrypt_value

# Try to load dotenv if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration for Rosetta config DB."""
    host: str = "localhost"
    port: int = 5432
    database: str = "rosetta"
    user: str = "postgres"
    password: str = "postgres"
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def dsn(self) -> dict:
        """Get connection parameters as dict for psycopg2."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": decrypt_value(self.password),
        }


@dataclass
class DebeziumConfig:
    """Debezium engine configuration."""
    offset_storage_path: str = "./tmp/offsets"
    offset_flush_interval_ms: int = 60000
    
    def get_offset_file(self, pipeline_name: str) -> str:
        """Get offset file path for a specific pipeline."""
        path = Path(self.offset_storage_path)
        path.mkdir(parents=True, exist_ok=True)
        return str(path / f"{pipeline_name}.dat")


@dataclass  
class PipelineConfig:
    """Pipeline processing configuration."""
    max_batch_size: int = 2048
    max_queue_size: int = 8192
    poll_interval_ms: int = 500
    slot_max_retries: int = 6
    slot_retry_delay_ms: int = 10000
    heartbeat_interval_ms: int = 10000


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class Config:
    """
    Central configuration for Rosetta Compute Engine.
    
    Loads configuration from environment variables with sensible defaults.
    """
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    debezium: DebeziumConfig = field(default_factory=DebeziumConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            database=DatabaseConfig(
                host=os.getenv("ROSETTA_DB_HOST", "localhost"),
                port=int(os.getenv("ROSETTA_DB_PORT", "5432")),
                database=os.getenv("ROSETTA_DB_NAME", "rosetta"),
                user=os.getenv("ROSETTA_DB_USER", "postgres"),
                password=os.getenv("ROSETTA_DB_PASSWORD", "postgres"),
            ),
            debezium=DebeziumConfig(
                offset_storage_path=os.getenv("DEBEZIUM_OFFSET_STORAGE_PATH", "./tmp/offsets"),
                offset_flush_interval_ms=int(os.getenv("DEBEZIUM_OFFSET_FLUSH_INTERVAL_MS", "60000")),
            ),
            pipeline=PipelineConfig(
                max_batch_size=int(os.getenv("PIPELINE_MAX_BATCH_SIZE", "2048")),
                max_queue_size=int(os.getenv("PIPELINE_MAX_QUEUE_SIZE", "8192")),
                poll_interval_ms=int(os.getenv("PIPELINE_POLL_INTERVAL_MS", "500")),
            ),
            logging=LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO"),
                format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            ),
        )


@lru_cache(maxsize=1)
def get_config() -> Config:
    """
    Get singleton configuration instance.
    
    Uses lru_cache to ensure only one Config instance exists.
    """
    return Config.from_env()
