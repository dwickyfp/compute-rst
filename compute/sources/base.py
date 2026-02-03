"""
Abstract base class for all CDC sources.

Provides the interface that all source implementations must follow.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

from compute.core.models import Source

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """
    Abstract base class for CDC sources.
    
    All source implementations (PostgreSQL, MySQL, etc.) must inherit from this class
    and implement the required methods.
    """
    
    def __init__(self, config: Source):
        """
        Initialize base source.
        
        Args:
            config: Source configuration from database
        """
        self._config = config
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @property
    def config(self) -> Source:
        """Get source configuration."""
        return self._config
    
    @property
    def source_id(self) -> int:
        """Get source ID."""
        return self._config.id
    
    @property
    def name(self) -> str:
        """Get source name."""
        return self._config.name
    
    @abstractmethod
    def build_debezium_props(
        self, 
        pipeline_name: str,
        table_include_list: list[str],
        offset_file: str
    ) -> dict[str, Any]:
        """
        Build Debezium connector properties.
        
        Args:
            pipeline_name: Unique name for this pipeline
            table_include_list: List of tables to include (schema.table format)
            offset_file: Path to offset storage file
            
        Returns:
            Dict of Debezium connector properties
        """
        pass
    
    @abstractmethod
    def get_connection_string(self) -> str:
        """
        Get connection string for the source database.
        
        Returns:
            Database connection string
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate that connection to source is possible.
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def get_slot_name(self, pipeline_name: str) -> str:
        """
        Generate replication slot name for a pipeline.
        
        Args:
            pipeline_name: Pipeline name
            
        Returns:
            Replication slot name
        """
        pass
    
    def get_connector_class(self) -> str:
        """
        Get Debezium connector class name.
        
        Override in subclasses for different database types.
        """
        raise NotImplementedError("Subclass must implement get_connector_class()")
    
    def get_plugin_name(self) -> str:
        """
        Get logical decoding plugin name.
        
        Override in subclasses for different database types.
        """
        raise NotImplementedError("Subclass must implement get_plugin_name()")
