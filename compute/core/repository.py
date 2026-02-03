"""
Repository classes for database CRUD operations.

Provides data access layer for Rosetta Compute Engine.
"""

import logging
from typing import Optional
from datetime import datetime

from compute.core.database import DatabaseSession
from compute.core.models import (
    Source,
    Destination,
    Pipeline,
    PipelineDestination,
    PipelineDestinationTableSync,
    PipelineMetadata,
    TableMetadataList,
    DataFlowRecordMonitoring,
)
from compute.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)


class SourceRepository:
    """Repository for Source CRUD operations."""
    
    @staticmethod
    def get_by_id(source_id: int) -> Optional[Source]:
        """Get source by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM sources WHERE id = %s",
                (source_id,)
            )
            row = session.fetchone()
            return Source.from_dict(row) if row else None
    
    @staticmethod
    def get_by_name(name: str) -> Optional[Source]:
        """Get source by name."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM sources WHERE name = %s",
                (name,)
            )
            row = session.fetchone()
            return Source.from_dict(row) if row else None
    
    @staticmethod
    def get_all() -> list[Source]:
        """Get all sources."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM sources ORDER BY id")
            return [Source.from_dict(row) for row in session.fetchall()]


class DestinationRepository:
    """Repository for Destination CRUD operations."""
    
    @staticmethod
    def get_by_id(destination_id: int) -> Optional[Destination]:
        """Get destination by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM destinations WHERE id = %s",
                (destination_id,)
            )
            row = session.fetchone()
            return Destination.from_dict(row) if row else None
    
    @staticmethod
    def get_all() -> list[Destination]:
        """Get all destinations."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM destinations ORDER BY id")
            return [Destination.from_dict(row) for row in session.fetchall()]


class PipelineRepository:
    """Repository for Pipeline CRUD operations."""
    
    @staticmethod
    def get_by_id(pipeline_id: int, include_relations: bool = False) -> Optional[Pipeline]:
        """
        Get pipeline by ID.
        
        Args:
            pipeline_id: Pipeline ID
            include_relations: If True, load source and destinations
        """
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines WHERE id = %s",
                (pipeline_id,)
            )
            row = session.fetchone()
            
            if not row:
                return None
            
            pipeline = Pipeline.from_dict(row)
            
            if include_relations:
                # Load source
                pipeline.source = SourceRepository.get_by_id(pipeline.source_id)
                
                # Load destinations
                pipeline.destinations = PipelineDestinationRepository.get_by_pipeline_id(
                    pipeline_id, include_table_syncs=True
                )
            
            return pipeline
    
    @staticmethod
    def get_by_name(name: str) -> Optional[Pipeline]:
        """Get pipeline by name."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines WHERE name = %s",
                (name,)
            )
            row = session.fetchone()
            return Pipeline.from_dict(row) if row else None
    
    @staticmethod
    def get_active_pipelines() -> list[Pipeline]:
        """Get all pipelines with START status."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines WHERE status = 'START' ORDER BY id"
            )
            return [Pipeline.from_dict(row) for row in session.fetchall()]
    
    @staticmethod
    def get_all() -> list[Pipeline]:
        """Get all pipelines."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM pipelines ORDER BY id")
            return [Pipeline.from_dict(row) for row in session.fetchall()]
    
    @staticmethod
    def update_status(pipeline_id: int, status: str) -> bool:
        """Update pipeline status."""
        with DatabaseSession() as session:
            session.execute(
                """
                UPDATE pipelines 
                SET status = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (status, pipeline_id)
            )
            return session.rowcount > 0


class PipelineDestinationRepository:
    """Repository for PipelineDestination CRUD operations."""
    
    @staticmethod
    def get_by_id(pd_id: int) -> Optional[PipelineDestination]:
        """Get pipeline destination by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination WHERE id = %s",
                (pd_id,)
            )
            row = session.fetchone()
            return PipelineDestination.from_dict(row) if row else None
    
    @staticmethod
    def get_by_pipeline_id(
        pipeline_id: int, 
        include_table_syncs: bool = False
    ) -> list[PipelineDestination]:
        """Get all destinations for a pipeline."""
        with DatabaseSession() as session:
            session.execute(
                """
                SELECT pd.*, d.name as dest_name, d.type as dest_type, d.config as dest_config
                FROM pipelines_destination pd
                JOIN destinations d ON pd.destination_id = d.id
                WHERE pd.pipeline_id = %s
                ORDER BY pd.id
                """,
                (pipeline_id,)
            )
            
            results = []
            for row in session.fetchall():
                pd = PipelineDestination.from_dict(row)
                pd.destination = Destination(
                    id=row["destination_id"],
                    name=row["dest_name"],
                    type=row["dest_type"],
                    config=row["dest_config"],
                )
                
                if include_table_syncs:
                    pd.table_syncs = TableSyncRepository.get_by_pipeline_destination_id(pd.id)
                
                results.append(pd)
            
            return results
    
    @staticmethod
    def update_error(pd_id: int, is_error: bool, error_message: Optional[str] = None) -> bool:
        """Update pipeline destination error status."""
        with DatabaseSession() as session:
            if is_error:
                session.execute(
                    """
                    UPDATE pipelines_destination 
                    SET is_error = TRUE, error_message = %s, last_error_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_message, pd_id)
                )
            else:
                session.execute(
                    """
                    UPDATE pipelines_destination 
                    SET is_error = FALSE, error_message = NULL, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (pd_id,)
                )
            return session.rowcount > 0


class TableSyncRepository:
    """Repository for PipelineDestinationTableSync CRUD operations."""
    
    @staticmethod
    def get_by_id(sync_id: int) -> Optional[PipelineDestinationTableSync]:
        """Get table sync by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination_table_sync WHERE id = %s",
                (sync_id,)
            )
            row = session.fetchone()
            return PipelineDestinationTableSync.from_dict(row) if row else None
    
    @staticmethod
    def get_by_pipeline_destination_id(pd_id: int) -> list[PipelineDestinationTableSync]:
        """Get all table syncs for a pipeline destination."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination_table_sync WHERE pipeline_destination_id = %s ORDER BY id",
                (pd_id,)
            )
            return [PipelineDestinationTableSync.from_dict(row) for row in session.fetchall()]
    
    @staticmethod
    def get_by_table_name(pd_id: int, table_name: str) -> Optional[PipelineDestinationTableSync]:
        """Get table sync by pipeline destination ID and table name."""
        with DatabaseSession() as session:
            session.execute(
                """
                SELECT * FROM pipelines_destination_table_sync 
                WHERE pipeline_destination_id = %s AND table_name = %s
                """,
                (pd_id, table_name)
            )
            row = session.fetchone()
            return PipelineDestinationTableSync.from_dict(row) if row else None
    
    @staticmethod
    def update_error(sync_id: int, is_error: bool, error_message: Optional[str] = None) -> bool:
        """Update table sync error status."""
        with DatabaseSession() as session:
            if is_error:
                session.execute(
                    """
                    UPDATE pipelines_destination_table_sync 
                    SET is_error = TRUE, error_message = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_message, sync_id)
                )
            else:
                session.execute(
                    """
                    UPDATE pipelines_destination_table_sync 
                    SET is_error = FALSE, error_message = NULL, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (sync_id,)
                )
            return session.rowcount > 0


class TableMetadataRepository:
    """Repository for TableMetadataList CRUD operations."""
    
    @staticmethod
    def get_by_source_id(source_id: int) -> list[TableMetadataList]:
        """Get all table metadata for a source."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM table_metadata_list WHERE source_id = %s ORDER BY table_name",
                (source_id,)
            )
            return [TableMetadataList.from_dict(row) for row in session.fetchall()]
    
    @staticmethod
    def get_table_names_for_source(source_id: int) -> list[str]:
        """Get list of table names for a source."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT table_name FROM table_metadata_list WHERE source_id = %s ORDER BY table_name",
                (source_id,)
            )
            return [row["table_name"] for row in session.fetchall()]


class PipelineMetadataRepository:
    """Repository for PipelineMetadata CRUD operations."""
    
    @staticmethod
    def get_by_pipeline_id(pipeline_id: int) -> Optional[PipelineMetadata]:
        """Get metadata for a pipeline."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipeline_metadata WHERE pipeline_id = %s",
                (pipeline_id,)
            )
            row = session.fetchone()
            return PipelineMetadata.from_dict(row) if row else None
    
    @staticmethod
    def upsert(pipeline_id: int, status: str, error: Optional[str] = None) -> int:
        """Insert or update pipeline metadata."""
        with DatabaseSession() as session:
            if error:
                session.execute(
                    """
                    INSERT INTO pipeline_metadata (pipeline_id, status, last_error, last_error_at, last_start_at)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (pipeline_id) DO UPDATE 
                    SET status = EXCLUDED.status, 
                        last_error = EXCLUDED.last_error,
                        last_error_at = NOW(),
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (pipeline_id, status, error)
                )
            else:
                session.execute(
                    """
                    INSERT INTO pipeline_metadata (pipeline_id, status, last_start_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (pipeline_id) DO UPDATE 
                    SET status = EXCLUDED.status,
                        last_start_at = CASE WHEN EXCLUDED.status = 'RUNNING' THEN NOW() ELSE pipeline_metadata.last_start_at END,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (pipeline_id, status)
                )
            row = session.fetchone()
            return row["id"] if row else 0


class DataFlowRepository:
    """Repository for DataFlowRecordMonitoring CRUD operations."""
    
    @staticmethod
    def insert(record: DataFlowRecordMonitoring) -> int:
        """Insert a new data flow record."""
        with DatabaseSession() as session:
            session.execute(
                """
                INSERT INTO data_flow_record_monitoring 
                (pipeline_id, pipeline_destination_id, source_id, pipeline_destination_table_sync_id, table_name, record_count)
                VALUES (%(pipeline_id)s, %(pipeline_destination_id)s, %(source_id)s, %(pipeline_destination_table_sync_id)s, %(table_name)s, %(record_count)s)
                RETURNING id
                """,
                record.to_insert_dict()
            )
            row = session.fetchone()
            return row["id"] if row else 0
    
    @staticmethod
    def increment_count(
        pipeline_id: int,
        pipeline_destination_id: int,
        source_id: int,
        table_sync_id: int,
        table_name: str,
        count: int = 1
    ) -> None:
        """Insert new record count entry (append-only for time series monitoring)."""
        with DatabaseSession() as session:
            session.execute(
                """
                INSERT INTO data_flow_record_monitoring 
                (pipeline_id, pipeline_destination_id, source_id, pipeline_destination_table_sync_id, table_name, record_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (pipeline_id, pipeline_destination_id, source_id, table_sync_id, table_name, count)
            )
