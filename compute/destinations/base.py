"""
Abstract base class for all CDC destinations.

Provides the interface that all destination implementations must follow.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
import logging

from compute.core.models import Destination, PipelineDestinationTableSync

logger = logging.getLogger(__name__)


@dataclass
class CDCRecord:
    """
    Represents a single CDC record from Debezium.
    
    Attributes:
        operation: Type of operation (c=create, u=update, d=delete, r=read/snapshot)
        table_name: Source table name
        key: Record key (primary key)
        value: Record value (after for insert/update, before for delete)
        schema: Table schema information
        timestamp: Event timestamp
    """
    operation: str
    table_name: str
    key: dict[str, Any]
    value: dict[str, Any]
    schema: Optional[dict[str, Any]] = None
    timestamp: Optional[int] = None
    
    @property
    def is_insert(self) -> bool:
        """Check if this is an insert operation."""
        return self.operation in ("c", "r")
    
    @property
    def is_update(self) -> bool:
        """Check if this is an update operation."""
        return self.operation == "u"
    
    @property
    def is_delete(self) -> bool:
        """Check if this is a delete operation."""
        return self.operation == "d"


class BaseDestination(ABC):
    """
    Abstract base class for CDC destinations.
    
    All destination implementations (Snowflake, PostgreSQL, etc.) must inherit
    from this class and implement the required methods.
    """
    
    def __init__(self, config: Destination):
        """
        Initialize base destination.
        
        Args:
            config: Destination configuration from database
        """
        self._config = config
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._is_initialized = False
    
    @property
    def config(self) -> Destination:
        """Get destination configuration."""
        return self._config
    
    @property
    def destination_id(self) -> int:
        """Get destination ID."""
        return self._config.id
    
    @property
    def name(self) -> str:
        """Get destination name."""
        return self._config.name
    
    @property
    def destination_type(self) -> str:
        """Get destination type."""
        return self._config.type
    
    @abstractmethod
    def initialize(self) -> None:
        """
        Initialize destination connection and resources.
        
        Should be called before writing any records.
        """
        pass
    
    @abstractmethod
    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write a batch of CDC records to the destination.
        
        Args:
            records: List of CDC records to write
            table_sync: Table sync configuration
            
        Returns:
            Number of records successfully written
        """
        pass
    
    @abstractmethod
    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: dict[str, Any],
    ) -> bool:
        """
        Create destination table if it doesn't exist.
        
        Args:
            table_name: Target table name
            schema: Table schema from Debezium
            
        Returns:
            True if table was created, False if already exists
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close destination connection and cleanup resources.
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Validate that connection to destination is possible.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.initialize()
            return True
        except Exception as e:
            self._logger.error(f"Failed to validate destination connection: {e}")
            return False
        finally:
            self.close()
    
    def __enter__(self) -> "BaseDestination":
        """Context manager enter."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        self.close()
        return False
