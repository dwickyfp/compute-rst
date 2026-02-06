"""
Snowflake destination using Snowpipe Streaming.

Provides real-time data ingestion to Snowflake using the snowpipe-streaming SDK.
"""

import base64
import json
import logging
import tempfile
import uuid
from datetime import datetime
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
        self._clients: dict[str, Any] = {}  # table_name -> client
        self._channels: dict[str, Any] = {}  # table_name -> channel
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate required configuration keys."""
        cfg = self._config.config
        missing = [k for k in self.REQUIRED_CONFIG if k not in cfg]
        if missing:
            raise DestinationException(
                f"Missing required Snowflake config: {missing}",
                {"destination_id": self._config.id},
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

    def _get_private_key_content(self) -> str:
        """Normalize private key content while preserving PEM structure."""
        cfg = self._config.config

        # Handle private key - ensure proper PEM format with newlines
        private_key = cfg["private_key"]

        self._logger.debug(f"Original private_key length: {len(private_key)}")

        # Replace escaped \n (literal backslash-n) with actual newlines
        if "\\n" in private_key:
            private_key = private_key.replace("\\n", "\n")
            self._logger.debug("Replaced escaped \\n with actual newlines")

        # Normalize all line endings to \n (Unix-style); preserve content otherwise
        private_key = private_key.replace("\r\n", "\n").replace("\r", "\n")

        # Validate that we have a properly formatted key
        if not (private_key.startswith("-----BEGIN") and "-----END" in private_key):
            raise DestinationException(
                "Private key must be in PEM format with proper headers (-----BEGIN/-----END)",
                {"destination_id": self._config.id},
            )

        lines = private_key.split("\n")
        self._logger.debug(f"Final private_key length: {len(private_key)}")
        self._logger.debug(f"First 60 chars: {private_key[:60]}")
        self._logger.debug(f"Number of lines: {len(lines)}")

        return private_key

    def _create_properties_dict(self, private_key_content: str) -> dict[str, Any]:
        """Create properties dict for StreamingIngestClient."""
        cfg = self._config.config

        properties = {
            "account": cfg["account"],
            "user": cfg["user"],
            "private_key": private_key_content,  # Pass content directly, not file path
            "url": cfg.get("url", f"https://{cfg['account']}.snowflakecomputing.com"),
        }

        # Add role if specified
        if "role" in cfg:
            properties["role"] = cfg["role"]

        # Decrypt private_key_passphrase if present
        if "private_key_passphrase" in cfg:
            properties["private_key_passphrase"] = decrypt_value(
                cfg["private_key_passphrase"]
            )

        self._logger.info(
            f"Created properties for Snowflake client: {list(properties.keys())}"
        )

        return properties

    def initialize(self) -> None:
        """
        Initialize Snowpipe Streaming destination.
        """
        if self._is_initialized:
            return

        # Mark as initialized - clients will be created per table
        self._is_initialized = True
        self._logger.info(f"Snowflake destination initialized: {self._config.name}")

    def _get_or_create_client(self, table_name: str) -> Any:
        """
        Get or create a streaming client for a table.

        Args:
            table_name: Target table name

        Returns:
            Streaming client
        """
        if table_name not in self._clients:
            try:
                # Import here to avoid issues if SDK not installed
                from snowflake.ingest.streaming import StreamingIngestClient

                # Get private key content directly
                private_key_content = self._get_private_key_content()
                properties = self._create_properties_dict(private_key_content)

                # Use table name as-is for pipe name (should already have LANDING_ prefix if needed)
                # Pipe name format: <TABLE_NAME>-STREAMING
                pipe_name = f"{table_name}-STREAMING"
                self._logger.info(f"Using pipe name: {pipe_name}")
                self._logger.info(
                    f"Database properties: {self.landing_database}, {self.landing_schema}"
                )

                # Simple client name similar to Rust example pattern
                client = StreamingIngestClient(
                    client_name=f"{table_name}_DEFAULT",
                    db_name=self.landing_database,
                    schema_name=self.landing_schema,
                    pipe_name=pipe_name,
                    properties=properties,
                )

                self._clients[table_name] = client
                self._logger.info(
                    f"Created Snowflake client for table {table_name} with pipe {pipe_name}"
                )

            except ImportError:
                raise DestinationException(
                    "snowpipe-streaming package not installed. Install with: pip install snowpipe-streaming",
                    {"destination_id": self._config.id},
                )
            except Exception as e:
                raise DestinationException(
                    f"Failed to initialize Snowflake client for table {table_name}: {e}",
                    {"destination_id": self._config.id},
                )

        return self._clients[table_name]

    def _get_or_create_channel(self, table_name: str) -> Any:
        """
        Get or create a streaming channel for a table.

        Args:
            table_name: Target table name

        Returns:
            Streaming channel
        """
        if table_name not in self._channels:
            client = self._get_or_create_client(table_name)
            channel, _ = client.open_channel(
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
        print(f"Table Process : {target_table}")

        channel = self._get_or_create_channel(f"LANDING_{target_table.upper()}")

        written = 0
        for i, record in enumerate(records):
            try:
                # Determine operation type: c (create), u (update), d (delete)
                if record.is_delete:
                    operation = "d"
                else:
                    op_lower = record.operation.lower() if record.operation else ""
                    if "update" in op_lower or op_lower == "u":
                        operation = "u"
                    else:
                        operation = "c"  # default to create/insert

                row_data = {
                    **record.value,
                    "OPERATION": operation,
                    "SYNC_TIMESTAMP_ROSETTA": datetime.utcnow(),
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
        """Close all channels and clients."""
        for table_name, channel in self._channels.items():
            try:
                channel.close()
                self._logger.debug(f"Closed channel for table: {table_name}")
            except Exception as e:
                self._logger.warning(f"Error closing channel for {table_name}: {e}")

        self._channels.clear()

        for table_name, client in self._clients.items():
            try:
                client.close()
                self._logger.debug(f"Closed client for table: {table_name}")
            except Exception as e:
                self._logger.warning(
                    f"Error closing Snowflake client for {table_name}: {e}"
                )

        self._clients.clear()

        self._is_initialized = False
        self._logger.info(f"Snowflake destination closed: {self._config.name}")
