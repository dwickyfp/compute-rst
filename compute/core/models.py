"""
Data models for Rosetta Compute Engine.

Dataclass representations of database tables.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from enum import Enum


class PipelineStatus(str, Enum):
    """Pipeline status enum."""
    START = "START"
    PAUSE = "PAUSE"
    REFRESH = "REFRESH"


class DestinationType(str, Enum):
    """Destination type enum."""
    SNOWFLAKE = "SNOWFLAKE"
    POSTGRES = "POSTGRES"


class MetadataStatus(str, Enum):
    """Pipeline metadata status enum."""
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


@dataclass
class Source:
    """Source configuration model (sources table)."""
    id: int
    name: str
    pg_host: str
    pg_port: int
    pg_database: str
    pg_username: str
    pg_password: Optional[str]
    publication_name: str
    replication_id: int
    is_publication_enabled: bool = False
    is_replication_enabled: bool = False
    last_check_replication_publication: Optional[datetime] = None
    total_tables: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "Source":
        """Create Source from database row dict."""
        return cls(
            id=data["id"],
            name=data["name"],
            pg_host=data["pg_host"],
            pg_port=data["pg_port"],
            pg_database=data["pg_database"],
            pg_username=data["pg_username"],
            pg_password=data.get("pg_password"),
            publication_name=data["publication_name"],
            replication_id=data["replication_id"],
            is_publication_enabled=data.get("is_publication_enabled", False),
            is_replication_enabled=data.get("is_replication_enabled", False),
            last_check_replication_publication=data.get("last_check_replication_publication"),
            total_tables=data.get("total_tables", 0),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class Destination:
    """Destination configuration model (destinations table)."""
    id: int
    name: str
    type: str
    config: dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "Destination":
        """Create Destination from database row dict."""
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            config=data.get("config", {}),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )
    
    @property
    def is_snowflake(self) -> bool:
        """Check if destination is Snowflake."""
        return self.type.upper() == DestinationType.SNOWFLAKE.value
    
    @property
    def is_postgres(self) -> bool:
        """Check if destination is PostgreSQL."""
        return self.type.upper() == DestinationType.POSTGRES.value


@dataclass
class Pipeline:
    """Pipeline configuration model (pipelines table)."""
    id: int
    name: str
    source_id: int
    status: str = PipelineStatus.PAUSE.value
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Loaded relations
    source: Optional[Source] = None
    destinations: list["PipelineDestination"] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: dict) -> "Pipeline":
        """Create Pipeline from database row dict."""
        return cls(
            id=data["id"],
            name=data["name"],
            source_id=data["source_id"],
            status=data.get("status", PipelineStatus.PAUSE.value),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )
    
    @property
    def is_running(self) -> bool:
        """Check if pipeline should be running."""
        return self.status == PipelineStatus.START.value
    
    @property
    def is_paused(self) -> bool:
        """Check if pipeline is paused."""
        return self.status == PipelineStatus.PAUSE.value


@dataclass
class PipelineDestination:
    """Pipeline destination mapping (pipelines_destination table)."""
    id: int
    pipeline_id: int
    destination_id: int
    is_error: bool = False
    error_message: Optional[str] = None
    last_error_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Loaded relations
    destination: Optional[Destination] = None
    table_syncs: list["PipelineDestinationTableSync"] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: dict) -> "PipelineDestination":
        """Create PipelineDestination from database row dict."""
        return cls(
            id=data["id"],
            pipeline_id=data["pipeline_id"],
            destination_id=data["destination_id"],
            is_error=data.get("is_error", False),
            error_message=data.get("error_message"),
            last_error_at=data.get("last_error_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class PipelineDestinationTableSync:
    """Table sync configuration (pipelines_destination_table_sync table)."""
    id: int
    pipeline_destination_id: int
    table_name: str
    table_name_target: str
    custom_sql: Optional[str] = None
    filter_sql: Optional[str] = None
    is_exists_table_landing: bool = False
    is_exists_stream: bool = False
    is_exists_task: bool = False
    is_exists_table_destination: bool = False
    is_error: bool = False
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "PipelineDestinationTableSync":
        """Create PipelineDestinationTableSync from database row dict."""
        return cls(
            id=data["id"],
            pipeline_destination_id=data["pipeline_destination_id"],
            table_name=data["table_name"],
            table_name_target=data["table_name_target"],
            custom_sql=data.get("custom_sql"),
            filter_sql=data.get("filter_sql"),
            is_exists_table_landing=data.get("is_exists_table_landing", False),
            is_exists_stream=data.get("is_exists_stream", False),
            is_exists_task=data.get("is_exists_task", False),
            is_exists_table_destination=data.get("is_exists_table_destination", False),
            is_error=data.get("is_error", False),
            error_message=data.get("error_message"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class PipelineMetadata:
    """Pipeline runtime metadata (pipeline_metadata table)."""
    id: int
    pipeline_id: int
    status: str = MetadataStatus.RUNNING.value
    last_error: Optional[str] = None
    last_error_at: Optional[datetime] = None
    last_start_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "PipelineMetadata":
        """Create PipelineMetadata from database row dict."""
        return cls(
            id=data["id"],
            pipeline_id=data["pipeline_id"],
            status=data.get("status", MetadataStatus.RUNNING.value),
            last_error=data.get("last_error"),
            last_error_at=data.get("last_error_at"),
            last_start_at=data.get("last_start_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class TableMetadataList:
    """Table metadata from source (table_metadata_list table)."""
    id: int
    source_id: int
    table_name: str
    schema_table: Optional[dict] = None
    is_changes_schema: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "TableMetadataList":
        """Create TableMetadataList from database row dict."""
        return cls(
            id=data["id"],
            source_id=data["source_id"],
            table_name=data["table_name"],
            schema_table=data.get("schema_table"),
            is_changes_schema=data.get("is_changes_schema", False),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class DataFlowRecordMonitoring:
    """Data flow record monitoring (data_flow_record_monitoring table)."""
    id: Optional[int] = None
    pipeline_id: int = 0
    pipeline_destination_id: Optional[int] = None
    source_id: int = 0
    pipeline_destination_table_sync_id: int = 0
    table_name: str = ""
    record_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "DataFlowRecordMonitoring":
        """Create DataFlowRecordMonitoring from database row dict."""
        return cls(
            id=data.get("id"),
            pipeline_id=data["pipeline_id"],
            pipeline_destination_id=data.get("pipeline_destination_id"),
            source_id=data["source_id"],
            pipeline_destination_table_sync_id=data["pipeline_destination_table_sync_id"],
            table_name=data["table_name"],
            record_count=data.get("record_count", 0),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )
    
    def to_insert_dict(self) -> dict:
        """Convert to dict for INSERT."""
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_destination_id": self.pipeline_destination_id,
            "source_id": self.source_id,
            "pipeline_destination_table_sync_id": self.pipeline_destination_table_sync_id,
            "table_name": self.table_name,
            "record_count": self.record_count,
        }
