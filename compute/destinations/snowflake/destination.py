"""
Snowflake destination using native Connect REST API.

Provides real-time data ingestion to Snowflake via Snowpipe Streaming
without external SDK dependencies.
"""

import asyncio
import logging
import threading
from concurrent.futures import Future
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
        
        # Background event loop for async operations
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

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

    def _start_background_loop(self) -> None:
        """Start background thread with event loop."""
        if self._loop is not None and self._thread is not None and self._thread.is_alive():
            return

        def run_loop(loop: asyncio.AbstractEventLoop):
             asyncio.set_event_loop(loop)
             loop.run_forever()

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=run_loop, 
            args=(self._loop,), 
            daemon=True,
            name=f"SnowflakeDestination-{self._config.id}"
        )
        self._thread.start()
        self._logger.info(f"Started background loop for Snowflake destination {self._config.id}")

    async def _initialize_async(self) -> None:
        """Initialize client in background loop."""
        if self._client is not None:
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
            # Authenticate immediately to verify config
            await self._client.authenticate()
            
            self._is_initialized = True
            self._logger.info(f"Snowflake destination initialized: {self._config.name}")

        except Exception as e:
            raise DestinationException(
                f"Failed to initialize Snowflake destination: {e}",
                {"destination_id": self._config.id},
            )

    def initialize(self) -> None:
        """Initialize Snowpipe Streaming destination."""
        if self._is_initialized:
            return

        self._start_background_loop()
        
        # Run initialization in background loop
        future = asyncio.run_coroutine_threadsafe(self._initialize_async(), self._loop)
        try:
            future.result(timeout=30)
        except Exception as e:
             self._logger.error(f"Initialization failed: {e}")
             raise

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
        # Debezium schema structure:
        # {
        #   "type": "struct",
        #   "fields": [
        #     {"field": "before", "type": "struct", "fields": [...]},
        #     {"field": "after", "type": "struct", "fields": [...column definitions...]},
        #     {"field": "source", ...},
        #     {"field": "op", ...}
        #   ]
        # }
        # We need to extract fields from "after" (or "before" for deletes)
        schema_map = {}
        if record.schema and "fields" in record.schema:
            try:
                target_field = "before" if record.is_delete else "after"
                for field in record.schema["fields"]:
                    field_name = field.get("field")
                    
                    # Found the after/before field - extract its nested fields
                    if field_name == target_field and "fields" in field:
                        for col_field in field["fields"]:
                            if "field" in col_field:
                                schema_map[col_field["field"]] = col_field
                        break
                    # Fallback: direct field mapping (simpler schema structure)
                    elif field_name and field_name not in ("before", "after", "source", "op", "ts_ms", "transaction"):
                        schema_map[field_name] = field
                        
            except Exception as e:
                self._logger.warning(f"Failed to parse schema: {e}")

        # Build row with uppercase column names and type conversion
        row = {}
        for k, v in record.value.items():
            field_schema = schema_map.get(k)
            converted = self._convert_value_for_snowflake(v, field_schema)
            row[k.upper()] = converted
            
            # Debug logging for type conversions
            if field_schema:
                type_name = field_schema.get("name", "unknown")
                if type_name != "unknown":
                    self._logger.debug(
                        f"Converted {k}: {type(v).__name__}({v}) -> {type(converted).__name__}({converted}) "
                        f"[schema: {type_name}]"
                    )
        
        row["OPERATION"] = operation
        row["SYNC_TIMESTAMP_ROSETTA"] = datetime.now(timezone.utc).isoformat()

        return row

    def _convert_value_for_snowflake(self, value: Any, field_schema: Optional[dict] = None) -> Any:
        """
        Convert a value to be compatible with Snowflake using schema metadata.
        
        Matches Rust implementation's cell_to_json_value() approach:
        - Decimals (Base64 encoded bytes) -> Decimal/float/string
        - Date (int32 epoch days) -> 'YYYY-MM-DD'
        - Timestamp (int64 epoch micros/nanos) -> ISO format
        - ZonedTimestamp (string) -> pass through
        - Time (int64 micros) -> 'HH:MM:SS.ffffff'
        - UUID (string) -> pass through
        - JSON/Array -> native Python types
        """
        import json
        import base64
        import decimal
        from datetime import date, timedelta, time as dt_time
        
        if value is None:
            return None
        
        # 1. Use Schema Metadata if available (Debezium provides type hints)
        if field_schema:
            type_name = field_schema.get("name")
            
            # DECIMAL Handling (org.apache.kafka.connect.data.Decimal)
            # Debezium sends as Base64-encoded big-endian signed integer
            if type_name == "org.apache.kafka.connect.data.Decimal":
                if isinstance(value, str):
                    try:
                        decoded = base64.b64decode(value)
                        unscaled = int.from_bytes(decoded, byteorder='big', signed=True)
                        scale = int(field_schema.get("parameters", {}).get("scale", 0))
                        d = decimal.Decimal(unscaled) / (decimal.Decimal(10) ** scale)
                        # Return as string for Snowflake NUMERIC precision
                        return str(d)
                    except Exception:
                        pass
                elif isinstance(value, (int, float)):
                    # Already numeric, return as-is
                    return value
            
            # DATE Handling (io.debezium.time.Date -> int32 days since epoch)
            if type_name == "io.debezium.time.Date":
                if isinstance(value, int):
                    return (date(1970, 1, 1) + timedelta(days=value)).isoformat()
                elif isinstance(value, str):
                    # Already ISO format string
                    return value
            
            # TIMESTAMP Handling - Multiple Debezium types
            # MicroTimestamp: int64 microseconds since epoch UTC
            if type_name == "io.debezium.time.MicroTimestamp":
                if isinstance(value, int):
                    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()
            
            # NanoTimestamp: int64 nanoseconds since epoch UTC
            if type_name == "io.debezium.time.NanoTimestamp":
                if isinstance(value, int):
                    return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc).isoformat()
            
            # Timestamp (without timezone): int64 milliseconds since epoch
            if type_name == "io.debezium.time.Timestamp":
                if isinstance(value, int):
                    return datetime.fromtimestamp(value / 1_000, tz=timezone.utc).isoformat()
            
            # ZonedTimestamp: Already ISO-8601 string with timezone
            if type_name == "io.debezium.time.ZonedTimestamp":
                # Pass through as-is (already formatted correctly)
                return value
            
            # TIME Handling
            # MicroTime: int64 microseconds since midnight
            if type_name == "io.debezium.time.MicroTime":
                if isinstance(value, int):
                    total_seconds = value / 1_000_000
                    hours = int(total_seconds // 3600)
                    minutes = int((total_seconds % 3600) // 60)
                    seconds = int(total_seconds % 60)
                    microseconds = value % 1_000_000
                    return dt_time(hours, minutes, seconds, microseconds).isoformat()
            
            # NanoTime: int64 nanoseconds since midnight
            if type_name == "io.debezium.time.NanoTime":
                if isinstance(value, int):
                    total_micros = value // 1000
                    total_seconds = total_micros / 1_000_000
                    hours = int(total_seconds // 3600)
                    minutes = int((total_seconds % 3600) // 60)
                    seconds = int(total_seconds % 60)
                    microseconds = total_micros % 1_000_000
                    return dt_time(hours, minutes, seconds, microseconds).isoformat()
        
        # 2. General Type Handling (Fallback for missing schema)
        
        # Handle dict (nested object, geospatial GeoJSON/WKT)
        if isinstance(value, dict):
            # Complex types for Snowflake VARIANT should be JSON strings
            return json.dumps(value)
        
        # Handle list (Array type)
        if isinstance(value, list):
            # Return list directly - Snowflake handles it
            return value
        
        # Handle bytes (WKB geospatial or binary data)
        if isinstance(value, bytes):
            return value.hex()
        
        # Handle JSON strings that look like arrays/objects
        if isinstance(value, str):
            value_stripped = value.strip()
            if (value_stripped.startswith("{") and value_stripped.endswith("}")) or \
               (value_stripped.startswith("[") and value_stripped.endswith("]")):
                try:
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
        """Async implementation of batch writing in background loop."""
        # Note: self._client is already initialized in this loop via _initialize_async
        if self._client is None:
             raise DestinationException("Client not initialized", {"destination_id": self._config.id})

        target_table = table_sync.table_name_target.upper()
        if target_table.startswith("LANDING_"):
            landing_table = target_table
        else:
            landing_table = f"LANDING_{target_table}"

        self._logger.info(f"Writing {len(records)} records to {landing_table}")

        # Ensure channel is open and we have tokens
        if landing_table not in self._channel_tokens:
            channel_resp = await self._client.open_channel(landing_table, "default")
            self._channel_tokens[landing_table] = channel_resp.next_continuation_token or ""

        # Convert records to rows
        rows = [self._convert_record_to_row(record) for record in records]
        
        # Filter out rows where all data values are null (excluding metadata fields)
        metadata_fields = {"OPERATION", "SYNC_TIMESTAMP_ROSETTA"}
        valid_rows = []
        skipped_count = 0
        for row in rows:
            # Check if any non-metadata field has a non-null value
            has_data = any(
                v is not None 
                for k, v in row.items() 
                if k not in metadata_fields
            )
            if has_data:
                valid_rows.append(row)
            else:
                skipped_count += 1
        
        if skipped_count > 0:
            self._logger.warning(
                f"Skipped {skipped_count} rows with all null values for {landing_table}"
            )
        
        # If no valid rows remain, return early
        if not valid_rows:
            self._logger.info(f"No valid rows to write to {landing_table} (all rows had null values)")
            return 0
        
        # Insert rows
        try:
            next_token = await self._client.insert_rows(
                landing_table,
                "default",
                valid_rows,
                self._channel_tokens.get(landing_table),
            )
            
            # Update state on success
            self._channel_tokens[landing_table] = next_token

            self._logger.debug(f"Successfully wrote {len(valid_rows)} rows to {landing_table}")
            return len(valid_rows)

        except Exception as e:
            self._logger.error(f"Failed to write to {landing_table}: {e}")
            # Clear tokens to force channel re-open on retry
            self._channel_tokens.pop(landing_table, None)
            raise

    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write batch of records to Snowflake via Snowpipe Streaming.
        
        Dispatches async work to background thread.
        """
        if not records:
            return 0

        self._logger.info(
            f"[Snowflake] write_batch called with {len(records)} records "
            f"for table_sync: {table_sync.table_name} -> {table_sync.table_name_target}"
        )

        if self._loop is None or not self._loop.is_running():
            self.initialize()
            
        future = asyncio.run_coroutine_threadsafe(
            self._write_batch_async(records, table_sync), 
            self._loop
        )
        
        try:
            return future.result(timeout=120)
        except Exception as e:
            self._logger.error(f"[Snowflake] write_batch failed: {e}", exc_info=True)
            raise

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
        # Stop background loop
        if self._loop and self._loop.is_running():
            try:
                # Close client in loop
                if self._client:
                     asyncio.run_coroutine_threadsafe(self._client.close(), self._loop).result(timeout=5)
                
                # Stop loop
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread:
                    self._thread.join(timeout=5)
            except Exception as e:
                self._logger.warning(f"Error stopping background loop: {e}")
                
        self._client = None
        self._loop = None
        self._thread = None
        self._channel_tokens.clear()
        self._is_initialized = False
        self._logger.info(f"Snowflake destination closed: {self._config.name}")

