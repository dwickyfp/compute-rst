# Core module
# Note: Heavy imports (engine, manager) require pydbzengine to be installed
# Import them directly when needed to avoid import errors when pydbzengine is not installed

from compute.core.database import DatabaseSession, get_db_connection
from compute.core.models import (
    Source,
    Destination,
    Pipeline,
    PipelineDestination,
    PipelineDestinationTableSync,
    PipelineMetadata,
    TableMetadataList,
    DataFlowRecordMonitoring,
    PipelineStatus,
    DestinationType,
    MetadataStatus,
)
from compute.core.exceptions import (
    RosettaException,
    DatabaseException,
    PipelineException,
    SourceException,
    DestinationException,
)
from compute.core.repository import (
    SourceRepository,
    DestinationRepository,
    PipelineRepository,
    PipelineDestinationRepository,
    TableSyncRepository,
    TableMetadataRepository,
    PipelineMetadataRepository,
    DataFlowRepository,
)

__all__ = [
    # Database
    "DatabaseSession",
    "get_db_connection",
    # Models
    "Source",
    "Destination",
    "Pipeline",
    "PipelineDestination",
    "PipelineDestinationTableSync",
    "PipelineMetadata",
    "TableMetadataList",
    "DataFlowRecordMonitoring",
    "PipelineStatus",
    "DestinationType",
    "MetadataStatus",
    # Repositories
    "SourceRepository",
    "DestinationRepository",
    "PipelineRepository",
    "PipelineDestinationRepository",
    "TableSyncRepository",
    "TableMetadataRepository",
    "PipelineMetadataRepository",
    "DataFlowRepository",
    # Exceptions
    "RosettaException",
    "DatabaseException",
    "PipelineException",
    "SourceException",
    "DestinationException",
]


def __getattr__(name: str):
    """Lazy import for modules that require pydbzengine."""
    if name == "CDCEventHandler":
        from compute.core.event_handler import CDCEventHandler
        return CDCEventHandler
    elif name == "PipelineEngine":
        from compute.core.engine import PipelineEngine
        return PipelineEngine
    elif name == "run_pipeline":
        from compute.core.engine import run_pipeline
        return run_pipeline
    elif name == "PipelineManager":
        from compute.core.manager import PipelineManager
        return PipelineManager
    elif name == "CDCRecord":
        from compute.destinations.base import CDCRecord
        return CDCRecord
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
