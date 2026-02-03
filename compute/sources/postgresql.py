"""
PostgreSQL source implementation.

Provides PostgreSQL-specific Debezium connector configuration.
"""

import logging
from typing import Any, Optional

import psycopg2

from compute.sources.base import BaseSource
from compute.core.models import Source
from compute.core.security import decrypt_value
from compute.config import get_config

logger = logging.getLogger(__name__)


class PostgreSQLSource(BaseSource):
    """
    PostgreSQL CDC source using Debezium.
    
    Configures PostgreSQL logical replication with pgoutput plugin.
    """
    
    # Debezium PostgreSQL connector class
    CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"
    
    # Default plugin for PostgreSQL 10+
    PLUGIN_NAME = "pgoutput"
    
    # Default schema
    DEFAULT_SCHEMA = "public"
    
    def __init__(self, config: Source):
        """
        Initialize PostgreSQL source.
        
        Args:
            config: Source configuration from database
        """
        super().__init__(config)
    
    def get_connector_class(self) -> str:
        """Get Debezium connector class."""
        return self.CONNECTOR_CLASS
    
    def get_plugin_name(self) -> str:
        """Get logical decoding plugin name."""
        return self.PLUGIN_NAME
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        cfg = self._config
        password = decrypt_value(cfg.pg_password or "")
        return f"postgresql://{cfg.pg_username}:{password}@{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_database}"
    
    def get_slot_name(self, pipeline_name: str) -> str:
        """
        Generate replication slot name.
        
        Format: rosetta_{replication_id}_{sanitized_pipeline_name}
        """
        # Sanitize pipeline name for slot (only lowercase alphanumeric and underscore)
        sanitized = pipeline_name.lower().replace("-", "_")
        sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
        return f"supabase_etl_apply_{self._config.replication_id}"
    
    def validate_connection(self) -> bool:
        """
        Validate PostgreSQL connection.
        
        Checks:
        1. Can connect to database
        2. wal_level is set to 'logical'
        3. Replication slot can be created (user has permission)
        """
        try:
            conn = psycopg2.connect(
                host=self._config.pg_host,
                port=self._config.pg_port,
                dbname=self._config.pg_database,
                user=self._config.pg_username,
                password=decrypt_value(self._config.pg_password or ""),
            )
            
            with conn.cursor() as cur:
                # Check wal_level
                cur.execute("SHOW wal_level")
                wal_level = cur.fetchone()[0]
                if wal_level != "logical":
                    logger.warning(f"wal_level is '{wal_level}', should be 'logical'")
                    return False
                
                logger.info(f"PostgreSQL source validated: {self._config.name}")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to validate PostgreSQL source: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def build_debezium_props(
        self,
        pipeline_name: str,
        table_include_list: list[str],
        offset_file: str,
    ) -> dict[str, Any]:
        """
        Build Debezium PostgreSQL connector properties.
        
        Args:
            pipeline_name: Unique name for this pipeline
            table_include_list: List of tables to include (schema.table format)
            offset_file: Path to offset storage file
            
        Returns:
            Dict of Debezium connector properties
        """
        config = get_config()
        slot_name = self.get_slot_name(pipeline_name)
        
        # Build table include list with schema prefix
        tables_with_schema = []
        for table in table_include_list:
            if "." not in table:
                tables_with_schema.append(f"{self.DEFAULT_SCHEMA}.{table}")
            else:
                tables_with_schema.append(table)
        
        props = {
            # Engine identification
            "name": pipeline_name,
            "connector.class": self.CONNECTOR_CLASS,
            
            # Offset storage
            "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename": offset_file,
            "offset.flush.interval.ms": str(config.debezium.offset_flush_interval_ms),
            
            # PostgreSQL connection
            "database.hostname": self._config.pg_host,
            "database.port": str(self._config.pg_port),
            "database.user": self._config.pg_username,
            "database.password": decrypt_value(self._config.pg_password or ""),
            "database.dbname": self._config.pg_database,
            
            # Replication settings
            "plugin.name": self.PLUGIN_NAME,
            "slot.name": slot_name,
            "publication.name": self._config.publication_name,
            
            # Snapshot behavior - skip initial data snapshot
            "snapshot.mode": "no_data",
            
            # Table filtering
            "schema.include.list": self.DEFAULT_SCHEMA,
            "table.include.list": ",".join(tables_with_schema),
            
            # Heartbeat to prevent WAL growth
            "heartbeat.interval.ms": str(config.pipeline.heartbeat_interval_ms),
            "heartbeat.action.query": "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar)",
            
            # Performance settings
            "max.batch.size": str(config.pipeline.max_batch_size),
            "max.queue.size": str(config.pipeline.max_queue_size),
            "poll.interval.ms": str(config.pipeline.poll_interval_ms),
            
            # Slot management
            "slot.drop.on.stop": "false",
            "slot.max.retries": str(config.pipeline.slot_max_retries),
            "slot.retry.delay.ms": str(config.pipeline.slot_retry_delay_ms),
            
            # Topic prefix for routing
            "topic.prefix": f"rosetta_{pipeline_name}",
        }
        
        return props
    
    def build_heartbeat_query(self) -> str:
        """Get heartbeat query for WAL advancement."""
        return "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar)"
    
    @classmethod
    def from_source_id(cls, source_id: int) -> Optional["PostgreSQLSource"]:
        """
        Create PostgreSQLSource from source ID.
        
        Args:
            source_id: Database source ID
            
        Returns:
            PostgreSQLSource instance or None if not found
        """
        from compute.core.repository import SourceRepository
        
        source = SourceRepository.get_by_id(source_id)
        if source is None:
            return None
        
        return cls(source)
