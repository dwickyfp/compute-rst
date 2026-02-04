"""
Debezium engine wrapper for running CDC pipelines.

Provides high-level interface for creating and running Debezium engines.
"""

import logging
from typing import Any, Optional

from pydbzengine import DebeziumJsonEngine

from compute.config import get_config
from compute.core.models import Pipeline, DestinationType
from compute.core.event_handler import CDCEventHandler
from compute.core.repository import (
    PipelineRepository,
    TableMetadataRepository,
    PipelineMetadataRepository,
)
from compute.core.exceptions import PipelineException
from compute.sources.base import BaseSource
from compute.sources.postgresql import PostgreSQLSource
from compute.destinations.base import BaseDestination
from compute.destinations.snowflake import SnowflakeDestination
from compute.destinations.postgresql import PostgreSQLDestination

logger = logging.getLogger(__name__)


class PipelineEngine:
    """
    Debezium pipeline engine for running CDC pipelines.

    Manages the lifecycle of a single pipeline including:
    - Loading configuration from database
    - Creating source and destination instances
    - Running Debezium engine
    - Handling status updates
    """

    def __init__(self, pipeline_id: int):
        """
        Initialize pipeline engine.

        Args:
            pipeline_id: ID of pipeline to run
        """
        self._pipeline_id = pipeline_id
        self._pipeline: Optional[Pipeline] = None
        self._source: Optional[BaseSource] = None
        self._destinations: dict[int, BaseDestination] = {}
        self._engine: Optional[DebeziumJsonEngine] = None
        self._logger = logging.getLogger(f"{__name__}.Pipeline_{pipeline_id}")
        self._is_running = False

    def _load_pipeline(self) -> Pipeline:
        """Load pipeline configuration from database."""
        pipeline = PipelineRepository.get_by_id(
            self._pipeline_id, include_relations=True
        )

        if pipeline is None:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} not found",
                {"pipeline_id": self._pipeline_id},
            )

        if pipeline.source is None:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} has no source configured",
                {"pipeline_id": self._pipeline_id},
            )

        if not pipeline.destinations:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} has no destinations configured",
                {"pipeline_id": self._pipeline_id},
            )

        return pipeline

    def _create_source(self, pipeline: Pipeline) -> BaseSource:
        """
        Create source instance based on configuration.

        Currently only PostgreSQL is supported.
        """
        # For now, all sources are PostgreSQL
        return PostgreSQLSource(pipeline.source)

    def _create_destination(
        self, destination_type: str, config: Any
    ) -> BaseDestination:
        """
        Create destination instance based on type.

        Args:
            destination_type: Type of destination (SNOWFLAKE, POSTGRES)
            config: Destination configuration model

        Returns:
            BaseDestination instance
        """
        if destination_type.upper() == DestinationType.SNOWFLAKE.value:
            return SnowflakeDestination(config)
        elif destination_type.upper() == DestinationType.POSTGRES.value:
            return PostgreSQLDestination(config)
        else:
            raise PipelineException(
                f"Unsupported destination type: {destination_type}",
                {"destination_type": destination_type},
            )

    def _get_table_include_list(self, source_id: int) -> list[str]:
        """
        Get list of tables to include in CDC.

        Tables are loaded from table_metadata_list for this source.

        Args:
            source_id: Source ID

        Returns:
            List of table names
        """
        tables = TableMetadataRepository.get_table_names_for_source(source_id)

        if not tables:
            self._logger.warning(
                f"No tables found in table_metadata_list for source {source_id}"
            )

        return tables

    def initialize(self) -> None:
        """
        Initialize pipeline engine.

        Loads configuration and creates source/destination instances.
        Each destination is initialized independently - if one fails during init,
        others can still be used.
        """
        self._pipeline = self._load_pipeline()
        self._source = self._create_source(self._pipeline)

        # Create and initialize destination instances independently
        successful_destinations = 0
        failed_destinations = 0

        for pd in self._pipeline.destinations:
            if not pd.destination:
                self._logger.warning(
                    f"Pipeline destination {pd.id} has no destination config, skipping"
                )
                continue

            try:
                dest = self._create_destination(pd.destination.type, pd.destination)
                dest.initialize()
                self._destinations[pd.destination_id] = dest
                successful_destinations += 1

                # Clear any previous initialization errors
                from compute.core.repository import PipelineDestinationRepository

                if pd.is_error:
                    PipelineDestinationRepository.update_error(pd.id, False)
                    self._logger.info(
                        f"Cleared error state for destination {pd.destination.name}"
                    )

            except Exception as e:
                # Log error but continue with other destinations
                error_msg = (
                    f"Failed to initialize destination {pd.destination.name}: {str(e)}"
                )
                self._logger.error(error_msg, exc_info=True)
                failed_destinations += 1

                # Update error state in database
                from compute.core.repository import PipelineDestinationRepository

                PipelineDestinationRepository.update_error(pd.id, True, error_msg)

        if successful_destinations == 0:
            raise PipelineException(
                f"Pipeline {self._pipeline.name} has no working destinations. "
                f"All {failed_destinations} destination(s) failed to initialize.",
                {"pipeline_id": self._pipeline_id},
            )

        self._logger.info(
            f"Pipeline {self._pipeline.name} initialized: "
            f"{successful_destinations} destination(s) ready, "
            f"{failed_destinations} destination(s) failed"
        )

    def run(self) -> None:
        """
        Run the pipeline engine.

        Starts Debezium engine and begins processing CDC events.
        """
        if self._pipeline is None:
            self.initialize()

        config = get_config()

        # Get table include list
        table_list = self._get_table_include_list(self._pipeline.source_id)

        if not table_list:
            raise PipelineException(
                "No tables configured for this pipeline",
                {"pipeline_id": self._pipeline_id},
            )

        # Build Debezium properties
        offset_file = config.debezium.get_offset_file(self._pipeline.name)
        props = self._source.build_debezium_props(
            pipeline_name=self._pipeline.name,
            table_include_list=table_list,
            offset_file=offset_file,
        )

        # Create event handler
        handler = CDCEventHandler(
            pipeline=self._pipeline,
            destinations=self._destinations,
        )

        # Update metadata
        PipelineMetadataRepository.upsert(self._pipeline_id, "RUNNING")

        self._logger.info(f"Starting pipeline {self._pipeline.name}")
        self._is_running = True

        try:
            # Create and run Debezium engine
            self._engine = DebeziumJsonEngine(properties=props, handler=handler)
            self._engine.run()
        except Exception as e:
            self._logger.error(f"Pipeline {self._pipeline.name} failed: {e}")
            PipelineMetadataRepository.upsert(self._pipeline_id, "ERROR", str(e))
            raise
        finally:
            self._is_running = False

    def stop(self) -> None:
        """Stop the pipeline engine."""
        self._is_running = False

        # Close destinations
        for dest in self._destinations.values():
            try:
                dest.close()
            except Exception as e:
                self._logger.warning(f"Error closing destination: {e}")

        self._destinations.clear()

        # Update metadata
        if self._pipeline:
            PipelineMetadataRepository.upsert(self._pipeline_id, "PAUSED")

        self._logger.info(f"Pipeline {self._pipeline_id} stopped")

    @property
    def is_running(self) -> bool:
        """Check if pipeline is running."""
        return self._is_running


def run_pipeline(pipeline_id: int) -> None:
    """
    Convenience function to run a pipeline.

    Args:
        pipeline_id: ID of pipeline to run
    """
    engine = PipelineEngine(pipeline_id)
    engine.run()
