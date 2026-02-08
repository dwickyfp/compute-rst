"""
Debezium change event handler for processing CDC records.

Parses Debezium events and routes them to appropriate destinations.
"""

import json
import logging
from typing import Any, Optional
from dataclasses import dataclass

from pydbzengine import ChangeEvent, BasePythonChangeHandler

from compute.destinations.base import CDCRecord, BaseDestination
from compute.core.models import (
    Pipeline,
    PipelineDestination,
    PipelineDestinationTableSync,
)
from compute.core.repository import (
    DataFlowRepository,
    TableSyncRepository,
    PipelineDestinationRepository,
)
from compute.core.exceptions import DestinationException

logger = logging.getLogger(__name__)


@dataclass
class RoutingInfo:
    """Information for routing a record to a destination."""

    pipeline_destination: PipelineDestination
    table_sync: PipelineDestinationTableSync
    destination: BaseDestination


class CDCEventHandler(BasePythonChangeHandler):
    """
    Debezium change event handler that routes CDC records to destinations.

    Handles parsing of Debezium events and routing to configured destinations
    based on pipeline configuration.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
    ):
        """
        Initialize CDC event handler.

        Args:
            pipeline: Pipeline configuration with destinations loaded
            destinations: Dict mapping destination_id to BaseDestination instances
        """
        self._pipeline = pipeline
        self._destinations = destinations
        self._logger = logging.getLogger(f"{__name__}.{pipeline.name}")

        # Build routing table: table_name -> list of RoutingInfo
        self._routing_table: dict[str, list[RoutingInfo]] = {}
        self._build_routing_table()

    def _build_routing_table(self) -> None:
        """Build routing table from pipeline configuration."""
        self._logger.info(f"Building routing table for pipeline {self._pipeline.name}")
        self._logger.info(f"Pipeline has {len(self._pipeline.destinations)} destination(s)")
        
        for pd in self._pipeline.destinations:
            self._logger.info(f"Processing pipeline_destination {pd.id} -> destination_id {pd.destination_id}")
            destination = self._destinations.get(pd.destination_id)
            if not destination:
                self._logger.warning(
                    f"Destination {pd.destination_id} not found for pipeline {self._pipeline.name}"
                )
                continue

            self._logger.info(f"Destination {pd.destination_id} has {len(pd.table_syncs)} table_sync(s)")
            for table_sync in pd.table_syncs:
                table_name = table_sync.table_name
                self._logger.info(f"  Adding routing for table: '{table_name}' -> target: '{table_sync.table_name_target}'")

                if table_name not in self._routing_table:
                    self._routing_table[table_name] = []

                self._routing_table[table_name].append(
                    RoutingInfo(
                        pipeline_destination=pd,
                        table_sync=table_sync,
                        destination=destination,
                    )
                )

        self._logger.info(f"Built routing table with {len(self._routing_table)} tables: {list(self._routing_table.keys())}")

    def _parse_destination_to_table_name(self, destination: str) -> str:
        """
        Extract table name from Debezium destination topic.

        Format: topic_prefix.schema.table_name
        Example: rosetta_my-pipeline.public.tbl_sales_stream_company -> tbl_sales_stream_company

        Args:
            destination: Debezium topic/destination (may be JPype Java string)

        Returns:
            Table name (last segment after splitting by '.')
        """
        # Convert JPype Java string to Python string
        dest_str = str(destination) if destination is not None else ""

        if not dest_str:
            self._logger.debug("Empty destination received, skipping")
            return ""

        # Split by dot to extract table name
        parts = dest_str.split(".")
        if len(parts) == 0:
            self._logger.warning(
                f"Could not parse destination '{dest_str}', no parts after split"
            )
            return ""

        # Skip non-table destinations (heartbeats, transaction commits, etc.)
        # Valid table destinations have format: topic_prefix.schema.table_name (3 parts)
        if len(parts) < 3:
            self._logger.debug(f"Skipping non-table destination: '{dest_str}'")
            return ""

        table_name = parts[-1]  # Always extract last part as table name
        self._logger.debug(
            f"Parsed destination '{dest_str}' -> table_name '{table_name}'"
        )
        return table_name

    def _parse_record(self, record: ChangeEvent) -> Optional[CDCRecord]:
        """
        Parse Debezium ChangeEvent into CDCRecord.

        Args:
            record: Raw Debezium change event

        Returns:
            Parsed CDCRecord or None if parsing fails
        """
        try:
            destination = record.destination()
            key_data = record.key()
            value_data = record.value()

            # Ensure we have Python strings (handle java.lang.String from JPype)
            # Parse JSON strings with null safety
            key_obj = json.loads(str(key_data)) if key_data is not None else {}
            value_obj = json.loads(str(value_data)) if value_data is not None else {}

            # Extract payload from Debezium format
            payload = value_obj.get("payload", {})
            op = payload.get("op")

            # Extract key payload
            if isinstance(key_obj, dict) and "payload" in key_obj:
                key = key_obj["payload"]
            else:
                key = key_obj

            # Extract value based on operation type
            if op in ("c", "u", "r"):  # create, update, read (snapshot)
                value = payload.get("after", {})
            elif op == "d":  # delete
                value = payload.get("before", {})
            elif op == "m":  # message (heartbeat)
                # Skip heartbeat messages
                return None
            else:
                value = payload if payload else {}

            # Get table name from destination topic
            table_name = self._parse_destination_to_table_name(destination)
            if not table_name:
                # Skip records with invalid/empty destination
                return None

            # Extract schema if available
            schema = value_obj.get("schema")

            return CDCRecord(
                operation=op or "u",
                table_name=table_name,
                key=key if isinstance(key, dict) else {},
                value=value if isinstance(value, dict) else {},
                schema=schema,
                timestamp=payload.get("ts_ms"),
            )

        except Exception as e:
            self._logger.error(f"Failed to parse record: {e}", exc_info=True)
            self._logger.error(
                f"Record details - destination: {record.destination() if record else 'N/A'}"
            )
            return None

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        """
        Handle a batch of Debezium change events.

        Routes each record to configured destinations based on table name.

        Args:
            records: List of Debezium change events
        """
        self._logger.info(f"Received batch of {len(records)} records")

        # Group records by table
        records_by_table: dict[str, list[CDCRecord]] = {}
        skipped_count = 0
        ops_seen = []

        for record in records:
            cdc_record = self._parse_record(record)
            if cdc_record is None:
                skipped_count += 1
                # Log what's being skipped for debugging
                try:
                    value_data = record.value()
                    dest_data = record.destination()
                    if value_data:
                        value_obj = json.loads(str(value_data))
                        op = value_obj.get("payload", {}).get("op", "unknown")
                        ops_seen.append(op)
                        if op == "unknown":
                            # Log full structure to understand the format
                            self._logger.info(
                                f"Unknown op record - destination: {dest_data}, "
                                f"payload keys: {list(value_obj.get('payload', {}).keys())}, "
                                f"full payload: {value_obj.get('payload', {})}"
                            )
                    else:
                        ops_seen.append("empty_value")
                except Exception as parse_ex:
                    ops_seen.append(f"parse_error:{parse_ex}")
                continue

            table_name = cdc_record.table_name
            ops_seen.append(cdc_record.operation)
            if table_name not in records_by_table:
                records_by_table[table_name] = []
            records_by_table[table_name].append(cdc_record)

        self._logger.info(
            f"Grouped into {len(records_by_table)} tables: {list(records_by_table.keys())} "
            f"(skipped: {skipped_count}, ops: {ops_seen})"
        )
        self._logger.info(
            f"Routing table has {len(self._routing_table)} tables: {list(self._routing_table.keys())}"
        )

        # Process each table's records
        for table_name, table_records in records_by_table.items():
            self._logger.info(
                f"Processing {len(table_records)} records for table '{table_name}'"
            )
            self._process_table_records(table_name, table_records)

    def _process_table_records(
        self,
        table_name: str,
        records: list[CDCRecord],
    ) -> None:
        """
        Process records for a specific table.

        Routes to all configured destinations for this table.
        Each destination is processed independently - if one fails, others continue.

        Args:
            table_name: Source table name
            records: CDC records for this table
        """
        routing_list = self._routing_table.get(table_name)

        if not routing_list:
            self._logger.warning(
                f"No routing configured for table: {table_name}. Available tables: {list(self._routing_table.keys())}"
            )
            return

        # Process each destination independently with individual error handling
        for routing in routing_list:
            self._process_single_destination(routing, table_name, records)

    def _process_single_destination(
        self,
        routing: RoutingInfo,
        table_name: str,
        records: list[CDCRecord],
    ) -> None:
        """
        Process records for a single destination with isolated error handling.

        If this destination fails, it won't affect other destinations in the same pipeline.
        Error state is tracked in pipelines_destination and pipelines_destination_table_sync.

        Args:
            routing: Routing information for the destination
            table_name: Source table name
            records: CDC records to write
        """
        try:
            dest_type = (
                routing.destination._config.type
                if hasattr(routing.destination, "_config")
                else "unknown"
            )
            dest_name = routing.destination.name
            self._logger.info(
                f"Routing {len(records)} records to destination '{dest_name}' "
                f"(type: {dest_type}, table: {table_name})"
            )

            # Write to destination - this is where connection/table errors can occur
            written = routing.destination.write_batch(records, routing.table_sync)

            self._logger.info(
                f"✓ Successfully wrote {written} records to {dest_name} for table {table_name}"
            )

            # Update data flow monitoring
            if written > 0:
                self._update_monitoring(
                    routing, f"LANDING_{table_name.upper()}", written
                )

            # Clear error state if previously failed - destination is now healthy
            if routing.pipeline_destination.is_error or routing.table_sync.is_error:
                self._logger.info(
                    f"Clearing error state for destination {dest_name} - now running successfully"
                )
                PipelineDestinationRepository.update_error(
                    routing.pipeline_destination.id, False
                )
                TableSyncRepository.update_error(routing.table_sync.id, False)
                # Update in-memory state
                routing.pipeline_destination.is_error = False
                routing.pipeline_destination.error_message = None
                routing.table_sync.is_error = False
                routing.table_sync.error_message = None

        except DestinationException as e:
            # Destination-specific error (e.g., table not exists, schema mismatch)
            error_msg = f"Destination error: {str(e)}"
            self._logger.error(
                f"✗ Failed to write to destination {routing.destination.name} "
                f"for table {table_name}: {error_msg}",
                exc_info=False,
            )

            # Update error state for both table sync and pipeline destination
            TableSyncRepository.update_error(routing.table_sync.id, True, error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, error_msg
            )

            # Update in-memory state
            routing.table_sync.is_error = True
            routing.table_sync.error_message = error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = error_msg

        except Exception as e:
            # Unexpected error (connection issues, authentication, etc.)
            error_msg = f"Unexpected error: {str(e)}"
            self._logger.error(
                f"✗ Unexpected error writing to destination {routing.destination.name} "
                f"for table {table_name}: {error_msg}",
                exc_info=True,
            )

            # Update error state for both table sync and pipeline destination
            TableSyncRepository.update_error(routing.table_sync.id, True, error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, error_msg
            )

            # Update in-memory state
            routing.table_sync.is_error = True
            routing.table_sync.error_message = error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = error_msg

    def _update_monitoring(
        self,
        routing: RoutingInfo,
        table_name: str,
        record_count: int,
    ) -> None:
        """
        Update data flow record monitoring.

        Args:
            routing: Routing information
            table_name: Table name
            record_count: Number of records written
        """
        try:
            DataFlowRepository.increment_count(
                pipeline_id=self._pipeline.id,
                pipeline_destination_id=routing.pipeline_destination.id,
                source_id=self._pipeline.source_id,
                table_sync_id=routing.table_sync.id,
                table_name=table_name,
                count=record_count,
            )
        except Exception as e:
            self._logger.warning(f"Failed to update monitoring: {e}")
