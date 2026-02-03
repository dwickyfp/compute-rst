"""
Snowflake destination using Snowpipe Streaming.

Provides real-time data ingestion to Snowflake using the Streaming Ingest SDK.
"""

import json
import logging
import uuid
from typing import Any, Optional
from pathlib import Path

from compute.destinations.base import BaseDestination, CDCRecord
from compute.core.models import Destination, PipelineDestinationTableSync
from compute.core.exceptions import DestinationException
from compute.core.security import decrypt_value

logger = logging.getLogger(__name__)


class SnowflakeDestination(BaseDestination):
    """
    Snowflake destination using Snowpipe Streaming SDK.
    
    Provides low-latency data ingestion via streaming channels.
    """
    
    # Required config keys
    REQUIRED_CONFIG = ["account", "user", "private_key"]
    
    def __init__(self, config: Destination):
        """
        Initialize Snowflake destination.
        
        Args:
            config: Destination configuration from database
        """
        super().__init__(config)
        self._client = None
        self._channels: dict[str, Any] = {}  # table_name -> channel
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate required configuration keys."""
        cfg = self._config.config
        missing = [k for k in self.REQUIRED_CONFIG if k not in cfg]
        if missing:
            raise DestinationException(
                f"Missing required Snowflake config: {missing}",
                {"destination_id": self._config.id}
            )
    
    @property
    def account(self) -> str:
        """Get Snowflake account."""
        return self._config.config["account"]
    
    @property
    def user(self) -> str:
        """Get Snowflake user."""
        return self._config.config["user"]
    
    @property
    def database(self) -> str:
        """Get target database."""
        return self._config.config.get("database", "")
    
    @property
    def schema(self) -> str:
        """Get target schema."""
        return self._config.config.get("schema", "PUBLIC")
    
    @property
    def landing_database(self) -> str:
        """Get landing database for Snowpipe."""
        return self._config.config.get("landing_database", self.database)
    
    @property
    def landing_schema(self) -> str:
        """Get landing schema for Snowpipe."""
        return self._config.config.get("landing_schema", self.schema)
    
    def _create_profile_dict(self) -> dict[str, Any]:
        """Create profile dict for StreamingIngestClient."""
        cfg = self._config.config
        profile = {
            "account": cfg["account"],
            "user": cfg["user"],
            "private_key": cfg["private_key"],
            "url": cfg.get("url", f"https://{cfg['account']}.snowflakecomputing.com:443"),
        }
        
        # Decrypt private_key_passphrase if present
        if "private_key_passphrase" in cfg:
            profile["private_key_passphrase"] = decrypt_value(cfg["private_key_passphrase"])
        
        return profile
    
    def initialize(self) -> None:
        """
        Initialize Snowpipe Streaming client.
        """
        if self._is_initialized:
            return
        
        try:
            # Import here to avoid issues if SDK not installed
            from snowflake.ingest.streaming import StreamingIngestClient
            
            profile = self._create_profile_dict()
            
            # Write profile to temp file (SDK requires file path)
            profile_path = Path(f"/tmp/snowflake_profile_{uuid.uuid4()}.json")
            profile_path.write_text(json.dumps(profile))
            
            self._client = StreamingIngestClient(
                client_name=f"rosetta_{self._config.name}_{uuid.uuid4().hex[:8]}",
                db_name=self.landing_database,
                schema_name=self.landing_schema,
                pipe_name=f"rosetta_pipe_{self._config.name}",
                profile_json=str(profile_path),
            )
            
            self._is_initialized = True
            self._logger.info(f"Snowflake destination initialized: {self._config.name}")
            
        except ImportError:
            raise DestinationException(
                "snowflake-ingest package not installed. Install with: pip install snowflake-ingest",
                {"destination_id": self._config.id}
            )
        except Exception as e:
            raise DestinationException(
                f"Failed to initialize Snowflake client: {e}",
                {"destination_id": self._config.id}
            )
    
    def _get_or_create_channel(self, table_name: str) -> Any:
        """
        Get or create a streaming channel for a table.
        
        Args:
            table_name: Target table name
            
        Returns:
            Streaming channel
        """
        if table_name not in self._channels:
            channel, _ = self._client.open_channel(
                f"{table_name}_channel_{uuid.uuid4().hex[:8]}"
            )
            self._channels[table_name] = channel
            self._logger.debug(f"Created channel for table: {table_name}")
        
        return self._channels[table_name]
    
    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write batch of records to Snowflake via Snowpipe Streaming.
        
        Args:
            records: CDC records to write
            table_sync: Table sync configuration
            
        Returns:
            Number of records written
        """
        if not self._is_initialized:
            self.initialize()
        
        target_table = table_sync.table_name_target
        channel = self._get_or_create_channel(target_table)
        
        written = 0
        for i, record in enumerate(records):
            try:
                if record.is_delete:
                    # For deletes, we could mark as deleted or skip
                    # Depends on destination table design (soft delete vs hard delete)
                    row_data = {
                        **record.value,
                        "_rosetta_deleted": True,
                        "_rosetta_op": record.operation,
                    }
                else:
                    row_data = {
                        **record.value,
                        "_rosetta_deleted": False,
                        "_rosetta_op": record.operation,
                    }
                
                channel.append_row(row_data, str(i))
                written += 1
                
            except Exception as e:
                self._logger.error(f"Failed to write record to Snowflake: {e}")
                # Continue with other records
        
        return written
    
    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: dict[str, Any],
    ) -> bool:
        """
        Create Snowflake table if it doesn't exist.
        
        Note: Snowpipe Streaming requires table to exist.
        This should be handled separately via Snowflake SQL.
        
        Args:
            table_name: Target table name
            schema: Schema from Debezium
            
        Returns:
            True if table was created
        """
        # TODO: Implement table creation via Snowflake SQL
        # For now, tables should be pre-created
        self._logger.warning(
            f"Table creation not yet implemented for Snowflake. "
            f"Please ensure table {table_name} exists."
        )
        return False
    
    def close(self) -> None:
        """Close all channels and client."""
        for table_name, channel in self._channels.items():
            try:
                channel.close()
                self._logger.debug(f"Closed channel for table: {table_name}")
            except Exception as e:
                self._logger.warning(f"Error closing channel for {table_name}: {e}")
        
        self._channels.clear()
        
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                self._logger.warning(f"Error closing Snowflake client: {e}")
            self._client = None
        
        self._is_initialized = False
        self._logger.info(f"Snowflake destination closed: {self._config.name}")
