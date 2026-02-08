"""
Snowflake destination using native Connect REST API.

Provides real-time data ingestion to Snowflake via Snowpipe Streaming
without external SDK dependencies.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from compute.core.exceptions import DestinationException
from compute.core.models import Destination, PipelineDestinationTableSync
from compute.core.security import decrypt_value
from compute.destinations.base import BaseDestination, CDCRecord
from compute.destinations.snowflake.client import SnowpipeClient

logger = logging.getLogger(__name__)


class SnowflakeDestination(BaseDestination):
    """
    Snowflake destination using native Snowpipe Streaming REST API.

    Provides low-latency data ingestion via streaming channels without
    requiring the snowpipe-streaming SDK.
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
        self._client: Optional[SnowpipeClient] = None
        self._channel_tokens: dict[str, str] = {}  # table_name -> continuation_token
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
    def role(self) -> str:
        """Get Snowflake role."""
        return self._config.config.get("role", "")

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
        private_key = cfg["private_key"]

        self._logger.debug(f"Original private_key length: {len(private_key)}")

        # Replace escaped \n (literal backslash-n) with actual newlines
        if "\\n" in private_key:
            private_key = private_key.replace("\\n", "\n")
            self._logger.debug("Replaced escaped \\n with actual newlines")

        # Normalize all line endings to \n (Unix-style)
        private_key = private_key.replace("\r\n", "\n").replace("\r", "\n")

        # Validate PEM format
        if not (private_key.startswith("-----BEGIN") and "-----END" in private_key):
            raise DestinationException(
                "Private key must be in PEM format with proper headers (-----BEGIN/-----END)",
                {"destination_id": self._config.id},
            )

        return private_key

    def _get_passphrase(self) -> Optional[str]:
        """Get decrypted passphrase if present."""
        cfg = self._config.config
        if "private_key_passphrase" in cfg:
            return decrypt_value(cfg["private_key_passphrase"])
        return None

    def initialize(self) -> None:
        """Initialize Snowpipe Streaming destination."""
        if self._is_initialized:
            return

        try:
            private_key = self._get_private_key_content()
            passphrase = self._get_passphrase()

            self._client = SnowpipeClient(
                account_id=self.account,
                user=self.user,
                private_key_pem=private_key,
                database=self.database,
                schema=self.schema,
                role=self.role,
                landing_database=self.landing_database,
                landing_schema=self.landing_schema,
                passphrase=passphrase,
            )

            self._is_initialized = True
            self._logger.info(f"Snowflake destination initialized: {self._config.name}")

        except Exception as e:
            raise DestinationException(
                f"Failed to initialize Snowflake destination: {e}",
                {"destination_id": self._config.id},
            )

    def _convert_record_to_row(self, record: CDCRecord) -> dict[str, Any]:
        """
        Convert a CDC record to a Snowflake row dict.

        - Column names are uppercased for Snowflake
        - Adds OPERATION field (c/u/d)
        - Adds SYNC_TIMESTAMP_ROSETTA field
        - Converts complex types using schema metadata
        """
        # Determine operation type
        if record.is_delete:
            operation = "d"
        elif record.is_update:
            operation = "u"
        else:
            operation = "c"

        # Build schema map if available
        schema_map = {}
        if record.schema and "fields" in record.schema:
            try:
                # Debezium structure: schema -> fields (list) -> field (dict)
                # Field dict usually has "field" (name) and "name" (type name like io.debezium.time.Date)
                for field in record.schema["fields"]:
                    if "field" in field:
                        schema_map[field["field"]] = field
            except Exception:
                pass

        # Build row with uppercase column names and type conversion
        row = {}
        for k, v in record.value.items():
            field_schema = schema_map.get(k)
            row[k.upper()] = self._convert_value_for_snowflake(v, field_schema)
        
        row["OPERATION"] = operation
        row["SYNC_TIMESTAMP_ROSETTA"] = datetime.now(timezone.utc).isoformat()

        return row

    def _convert_value_for_snowflake(self, value: Any, field_schema: Optional[dict] = None) -> Any:
        """
        Convert a value to be compatible with Snowflake using schema metadata.
        
        Handles:
        - Decimals (Base64 encoded bytes) -> Decimal/float/string
        - Date (int32 epoch days) -> 'YYYY-MM-DD'
        - Timestamp (int64 epoch micros) -> ISO format
        - JSON/Array strings -> parsed JSON object/list
        - Geospatial types -> WKT/GeoJSON string
        """
        import json
        import base64
        import decimal
        from datetime import date, timedelta
        
        if value is None:
            return None
            
        # 1. Use Schema Metadata if available
        if field_schema:
            type_name = field_schema.get("name")
            
            # DECIMAL Handling
            if type_name == "org.apache.kafka.connect.data.Decimal":
                if isinstance(value, str):
                    # Likely Base64 encoded unscaled value if coming as string from JSON
                    try:
                        # Decode Base64
                        decoded = base64.b64decode(value)
                        # Convert bytes to int (big endian, signed)
                        unscaled = int.from_bytes(decoded, byteorder='big', signed=True)
                        # Get scale from parameters
                        scale = int(field_schema.get("parameters", {}).get("scale", 0))
                        # Create Decimal
                        from decimal import Context
                        d = decimal.Decimal(unscaled) / (decimal.Decimal(10) ** scale)
                        return float(d) # Snowflake prefers float/string for numeric in JSON
                    except Exception:
                        pass # Fallback to default handling
                elif isinstance(value, bytes):
                    # Raw bytes handling
                    try:
                         unscaled = int.from_bytes(value, byteorder='big', signed=True)
                         scale = int(field_schema.get("parameters", {}).get("scale", 0))
                         d = decimal.Decimal(unscaled) / (decimal.Decimal(10) ** scale)
                         return float(d)
                    except Exception:
                        pass

            # DATE Handling (io.debezium.time.Date -> int32 days since epoch)
            if type_name == "io.debezium.time.Date":
                if isinstance(value, int):
                    return (date(1970, 1, 1) + timedelta(days=value)).isoformat()
            
            # TIMESTAMP Handling (MicroTimestamp -> int64 micros since epoch)
            if type_name == "io.debezium.time.MicroTimestamp":
                 if isinstance(value, int):
                     return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()

            # JSON/ARRAY Handling
            # If value is string but schema says it's an Array or Struct, unlikely but possible
            # Debezium usually sends arrays as lists in JSON.
        
        # 2. General Type Handling (Fallback or where schema is missing/generic)
        
        # Handle dict (could be geospatial GeoJSON/WKT or nested object)
        if isinstance(value, dict):
            # Complex types for Snowflake VARIANT/GEOGRAPHY should be JSON strings
            return json.dumps(value)
        
        # Handle list (Array type)
        if isinstance(value, list):
            # Snowflake ARRAY column needs JSON string or list structure
            # For streaming, list structure is usually fine, but let's be consistent
            # If we return list, Python SDK/Connector handles it.
            return value 
            
        # Handle bytes (could be WKB geospatial or binary data)
        if isinstance(value, bytes):
            return value.hex()
            
        # Handle JSON strings that look like arrays/objects (for ARRAY/VARIANT columns)
        if isinstance(value, str):
            value_stripped = value.strip()
            if (value_stripped.startswith("{") and value_stripped.endswith("}")) or \
               (value_stripped.startswith("[") and value_stripped.endswith("]")):
                try:
                    # Attempt to parse JSON string to native object
                    # This ensures Snowflake treats it as VARIANT/ARRAY, not a string literal
                    return json.loads(value)
                except:
                    pass
        
        # Pass through other types (str, int, float, bool)
        return value

    async def _write_batch_async(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """Async implementation of batch writing."""
        if not self._is_initialized or self._client is None:
            self.initialize()

        target_table = table_sync.table_name_target
        landing_table = f"LANDING_{target_table.upper()}"

        self._logger.info(f"Writing {len(records)} records to {landing_table}")

        # Ensure channel is open
        if landing_table not in self._channel_tokens:
            token = await self._client.open_channel(landing_table, "default")
            self._channel_tokens[landing_table] = token

        # Convert records to rows
        rows = [self._convert_record_to_row(record) for record in records]

        # Insert rows
        try:
            next_token = await self._client.insert_rows(
                landing_table,
                "default",
                rows,
                self._channel_tokens.get(landing_table),
            )
            self._channel_tokens[landing_table] = next_token

            self._logger.debug(f"Successfully wrote {len(rows)} rows to {landing_table}")
            return len(rows)

        except Exception as e:
            self._logger.error(f"Failed to write to {landing_table}: {e}")
            # Clear token to force channel re-open on retry
            self._channel_tokens.pop(landing_table, None)
            raise

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
        if not records:
            return 0

        self._logger.info(
            f"[Snowflake] write_batch called with {len(records)} records "
            f"for table_sync: {table_sync.table_name} -> {table_sync.table_name_target}"
        )

        # Run async method - handle different event loop scenarios
        try:
            # Try to get running loop (Python 3.10+ style)
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context - need to run in thread pool
                self._logger.debug("[Snowflake] Running in existing event loop context")
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        self._run_async_in_new_loop,
                        self._write_batch_async(records, table_sync)
                    )
                    result = future.result(timeout=120)
                    self._logger.info(f"[Snowflake] Successfully wrote {result} records")
                    return result
            except RuntimeError:
                # No running loop - we can use asyncio.run() directly
                self._logger.debug("[Snowflake] No running event loop, using asyncio.run()")
                result = asyncio.run(self._write_batch_async(records, table_sync))
                self._logger.info(f"[Snowflake] Successfully wrote {result} records")
                return result
        except Exception as e:
            self._logger.error(f"[Snowflake] write_batch failed: {e}", exc_info=True)
            raise

    def _run_async_in_new_loop(self, coro):
        """Run a coroutine in a new event loop (for use in thread pool)."""
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()

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
        self._logger.warning(
            f"Table creation not yet implemented for Snowflake. "
            f"Please ensure table {table_name} exists."
        )
        return False

    def close(self) -> None:
        """Close client and cleanup resources."""
        if self._client is not None:
            try:
                # Run async close
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            executor.submit(asyncio.run, self._client.close()).result()
                    else:
                        loop.run_until_complete(self._client.close())
                except RuntimeError:
                    asyncio.run(self._client.close())
            except Exception as e:
                self._logger.warning(f"Error closing Snowflake client: {e}")
            finally:
                self._client = None

        self._channel_tokens.clear()
        self._is_initialized = False
        self._logger.info(f"Snowflake destination closed: {self._config.name}")
