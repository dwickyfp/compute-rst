"""
PostgreSQL destination using DuckDB for MERGE INTO operations.

Provides CDC data sync to PostgreSQL with filter and custom SQL support.
"""

import logging
import re
from typing import Any, Optional
from contextlib import contextmanager

import duckdb
import psycopg2

from compute.destinations.base import BaseDestination, CDCRecord
from compute.core.models import Destination, PipelineDestinationTableSync
from compute.core.exceptions import DestinationException
from compute.core.security import decrypt_value

logger = logging.getLogger(__name__)


class PostgreSQLDestination(BaseDestination):
    """
    PostgreSQL destination using DuckDB for efficient MERGE INTO operations.
    
    Flow:
    1. Create table if not exists (based on CDC schema)
    2. Filter columns (optional, from filter_sql)
    3. Custom SQL transformation (optional, from custom_sql)
    4. MERGE INTO destination table
    """
    
    # Required config keys
    REQUIRED_CONFIG = ["host", "port", "database", "user", "password"]
    
    def __init__(self, config: Destination):
        """
        Initialize PostgreSQL destination.
        
        Args:
            config: Destination configuration from database
        """
        super().__init__(config)
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._pg_conn: Optional[psycopg2.extensions.connection] = None
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate required configuration keys."""
        cfg = self._config.config
        missing = [k for k in self.REQUIRED_CONFIG if k not in cfg]
        if missing:
            raise DestinationException(
                f"Missing required PostgreSQL config: {missing}",
                {"destination_id": self._config.id}
            )
    
    @property
    def host(self) -> str:
        """Get PostgreSQL host."""
        return self._config.config["host"]
    
    @property
    def port(self) -> int:
        """Get PostgreSQL port."""
        return int(self._config.config.get("port", 5432))
    
    @property
    def database(self) -> str:
        """Get PostgreSQL database."""
        return self._config.config["database"]
    
    @property
    def user(self) -> str:
        """Get PostgreSQL user."""
        return self._config.config["user"]
    
    @property
    def password(self) -> str:
        """Get PostgreSQL password (decrypted)."""
        return decrypt_value(self._config.config.get("password", ""))
    
    @property
    def schema(self) -> str:
        """Get target schema."""
        return self._config.config.get("schema", "public")
    
    def _get_postgres_connection_string(self) -> str:
        """Get PostgreSQL connection string for DuckDB."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def initialize(self) -> None:
        """
        Initialize DuckDB connection with PostgreSQL extension.
        """
        if self._is_initialized:
            return
        
        try:
            # Create in-memory DuckDB connection
            self._duckdb_conn = duckdb.connect(":memory:")
            
            # Install and load PostgreSQL extension
            self._duckdb_conn.execute("INSTALL postgres;")
            self._duckdb_conn.execute("LOAD postgres;")
            
            # Attach PostgreSQL database
            conn_str = self._get_postgres_connection_string()
            self._duckdb_conn.execute(f"""
                ATTACH '{conn_str}' AS pg_dest (TYPE postgres, READ_WRITE);
            """)
            
            # Also create direct psycopg2 connection for DDL operations
            self._pg_conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
            )
            self._pg_conn.autocommit = True
            
            self._is_initialized = True
            self._logger.info(f"PostgreSQL destination initialized: {self._config.name}")
            
        except Exception as e:
            raise DestinationException(
                f"Failed to initialize PostgreSQL destination: {e}",
                {"destination_id": self._config.id}
            )
    
    def _get_table_schema(self, table_name: str) -> dict[str, dict]:
        """
        Get column info from target PostgreSQL table.
        
        Args:
            table_name: Target table name
            
        Returns:
            Dict mapping column_name -> {
                'type': postgres_type,
                'scale': numeric_scale (for decimals),
                'udt_name': underlying type name (for PostGIS types)
            }
        """
        with self._pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, numeric_scale, udt_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (self.schema, table_name))
            
            schema = {}
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1].lower()
                scale = row[2]
                udt_name = row[3].lower() if row[3] else None
                
                # For USER-DEFINED types (like PostGIS), use udt_name as the type
                if data_type == 'user-defined' and udt_name:
                    data_type = udt_name
                
                schema[col_name] = {
                    'type': data_type,
                    'scale': scale,
                    'udt_name': udt_name
                }
            return schema
    
    def _convert_debezium_value(self, value, column_name: str, column_info: dict):
        """
        Convert Debezium-encoded value to proper Python type for PostgreSQL.
        
        Args:
            value: Raw value from Debezium
            column_name: Column name
            column_info: Column metadata (type, scale)
            
        Returns:
            Converted value ready for PostgreSQL
        """
        import datetime
        import base64
        from decimal import Decimal
        
        if value is None:
            return None
        
        pg_type = column_info.get('type', 'text')
        
        try:
            if pg_type == 'date':
                # Debezium sends DATE as days since epoch
                if isinstance(value, int):
                    return datetime.date(1970, 1, 1) + datetime.timedelta(days=value)
                return value
            
            elif pg_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp'):
                # Debezium sends TIMESTAMP as microseconds since epoch
                if isinstance(value, int):
                    return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=value)
                return value
            
            elif pg_type == 'time without time zone':
                # Debezium sends TIME as microseconds since midnight
                if isinstance(value, int):
                    return (datetime.datetime.min + datetime.timedelta(microseconds=value)).time()
                return value
            
            elif pg_type in ('numeric', 'decimal'):
                # Debezium sends NUMERIC/DECIMAL as Base64-encoded big-endian byte array
                if isinstance(value, str) and not value.replace('.', '').replace('-', '').isdigit():
                    try:
                        # Decode Base64 to bytes
                        decoded_bytes = base64.b64decode(value)
                        # Convert big-endian bytes to integer
                        int_value = int.from_bytes(decoded_bytes, byteorder='big', signed=True)
                        
                        # Use actual schema scale if available, otherwise heuristic
                        scale = column_info.get('scale')
                        if scale is None:
                            # Fallback heuristic
                            if 'price' in column_name.lower() or 'rate' in column_name.lower():
                                scale = 4
                            elif 'pct' in column_name.lower() or 'percent' in column_name.lower():
                                scale = 2
                            else:
                                scale = 2
                        
                        return Decimal(int_value) / Decimal(10 ** scale)
                    except Exception as e:
                        self._logger.warning(f"Failed to decode Base64 numeric for {column_name}: {e}")
                        return value
                elif isinstance(value, (int, float)):
                    return Decimal(str(value))
                return value
            
            elif pg_type in ('integer', 'bigint', 'smallint'):
                return int(value) if value is not None else None
            
            elif pg_type in ('real', 'double precision'):
                return float(value) if value is not None else None
            
            elif pg_type == 'boolean':
                return bool(value) if value is not None else None
            
            elif pg_type in ('json', 'jsonb'):
                # Convert to JSON string - PostgreSQL can implicitly convert JSON string to jsonb
                import json
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                elif isinstance(value, str):
                    # Already a JSON string - validate and return
                    try:
                        json.loads(value)  # Validate it's valid JSON
                        return value
                    except:
                        return json.dumps(value)  # Wrap as JSON string
                else:
                    return json.dumps(value)
            
            elif pg_type == 'ARRAY' or '[]' in str(pg_type):
                # Convert to PostgreSQL array literal format: {a,b,c}
                if isinstance(value, list):
                    # Format as PostgreSQL array literal
                    formatted_items = []
                    for item in value:
                        if item is None:
                            formatted_items.append('NULL')
                        elif isinstance(item, str):
                            # Escape quotes and wrap in quotes
                            escaped = item.replace('"', '\\"')
                            formatted_items.append(f'"{escaped}"')
                        else:
                            formatted_items.append(str(item))
                    return '{' + ','.join(formatted_items) + '}'
                elif isinstance(value, str):
                    # Already a PostgreSQL array string
                    if value.startswith('{') and value.endswith('}'):
                        return value
                    return value
                return value
            
            elif pg_type in ('geometry', 'geography', 'point', 'polygon', 'linestring'):
                # Handle PostGIS types - Debezium sends dict with 'wkb' and 'srid'
                if isinstance(value, dict) and 'wkb' in value:
                    try:
                        # Value is {'wkb': 'Base64...', 'srid': 4326}
                        wkb_b64 = value['wkb']
                        if not wkb_b64:
                            return None
                        
                        # Decode Base64 WKB to bytes
                        wkb_bytes = base64.b64decode(wkb_b64)
                        
                        # Convert bytes to Hex string for PostgreSQL
                        # PostgreSQL expects hex string for WKB in generic handling
                        return wkb_bytes.hex()
                    except Exception as e:
                        self._logger.warning(f"Failed to process geometry WKB: {e}")
                        return None
                return value
            
            elif pg_type == 'ARRAY' or '[]' in str(pg_type):
                # Handle array types
                return value
            
            else:
                # Default: return as-is (text, varchar, etc.)
                return value
                
        except Exception as e:
            self._logger.warning(f"Failed to convert {column_name} ({pg_type}): {e}, using raw value")
            return value
    
    def _parse_filter_sql(self, filter_sql: str) -> list[str]:
        """
        Parse filter_sql into list of WHERE clauses.
        
        Format: "column_1 = '11';column_2>1"
        
        Args:
            filter_sql: Semicolon-separated filter conditions
            
        Returns:
            List of individual filter conditions
        """
        if not filter_sql:
            return []
        
        # Split by semicolon and strip whitespace
        filters = [f.strip() for f in filter_sql.split(";") if f.strip()]
        return filters
    
    def _apply_filters(
        self,
        records: list[CDCRecord],
        filter_sql: Optional[str],
    ) -> list[CDCRecord]:
        """
        Apply filter conditions to records.
        
        Args:
            records: CDC records to filter
            filter_sql: Filter conditions (semicolon-separated)
            
        Returns:
            Filtered records
        """
        if not filter_sql:
            return records
        
        filters = self._parse_filter_sql(filter_sql)
        if not filters:
            return records
        
        filtered = []
        for record in records:
            if self._record_matches_filters(record.value, filters):
                filtered.append(record)
        
        return filtered
    
    def _record_matches_filters(self, record: dict, filters: list[str]) -> bool:
        """
        Check if a record matches all filter conditions.
        
        Supports basic conditions: =, !=, >, <, >=, <=
        
        Args:
            record: Record data dict
            filters: List of filter conditions
            
        Returns:
            True if record matches all conditions
        """
        for condition in filters:
            # Parse condition (simple implementation)
            # Supports: column = 'value', column > 1, column >= 1, etc.
            match = re.match(
                r"(\w+)\s*(=|!=|<>|>|<|>=|<=)\s*(.+)",
                condition.strip()
            )
            
            if not match:
                self._logger.warning(f"Could not parse filter condition: {condition}")
                continue
            
            column, operator, value = match.groups()
            
            # Get record value
            if column not in record:
                return False
            
            record_value = record[column]
            
            # Parse comparison value (remove quotes if string)
            value = value.strip()
            if value.startswith("'") and value.endswith("'"):
                compare_value = value[1:-1]
            else:
                try:
                    compare_value = float(value) if "." in value else int(value)
                except ValueError:
                    compare_value = value
            
            # Perform comparison
            try:
                if operator == "=":
                    if record_value != compare_value:
                        return False
                elif operator in ("!=", "<>"):
                    if record_value == compare_value:
                        return False
                elif operator == ">":
                    if not (record_value > compare_value):
                        return False
                elif operator == "<":
                    if not (record_value < compare_value):
                        return False
                elif operator == ">=":
                    if not (record_value >= compare_value):
                        return False
                elif operator == "<=":
                    if not (record_value <= compare_value):
                        return False
            except TypeError:
                # Type mismatch in comparison
                return False
        
        return True
    
    def _execute_custom_sql(
        self,
        records: list[CDCRecord],
        table_name: str,
        custom_sql: str,
    ) -> list[dict]:
        """
        Execute custom SQL transformation on records.
        
        Creates a temporary table with records, then executes the custom SQL.
        The custom SQL can reference the table by its original name.
        
        Args:
            records: CDC records
            table_name: Source table name (for reference in SQL)
            custom_sql: Custom SQL query
            
        Returns:
            Transformed records as dicts
        """
        if not custom_sql or not records:
            return [r.value for r in records]
        
        try:
            # Create temporary table with record data
            temp_table = f"_temp_{table_name.replace('.', '_')}"
            
            # Get columns from first record
            columns = list(records[0].value.keys())
            
            # Create temp table
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            # Build CREATE TABLE statement
            col_defs = ", ".join([f'"{c}" VARCHAR' for c in columns])
            self._duckdb_conn.execute(f"CREATE TABLE {temp_table} ({col_defs})")
            
            # Insert records
            for record in records:
                values = [str(record.value.get(c, "")) for c in columns]
                placeholders = ", ".join(["?" for _ in columns])
                self._duckdb_conn.execute(
                    f"INSERT INTO {temp_table} VALUES ({placeholders})",
                    values
                )
            
            # Replace table name in custom SQL
            # Handle both "table_name" and "schema.table_name" formats
            sql = custom_sql.replace(table_name, temp_table)
            if "." in table_name:
                bare_name = table_name.split(".")[-1]
                sql = sql.replace(bare_name, temp_table)
            
            # Execute custom SQL
            result = self._duckdb_conn.execute(sql).fetchall()
            
            # Get column names from result
            result_columns = [desc[0] for desc in self._duckdb_conn.description]
            
            # Convert to dicts
            transformed = []
            for row in result:
                transformed.append(dict(zip(result_columns, row)))
            
            # Cleanup temp table
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            return transformed
            
        except Exception as e:
            self._logger.error(f"Custom SQL execution failed: {e}")
            # Fall back to original records
            return [r.value for r in records]
    
    def _get_primary_key_columns(self, record: CDCRecord) -> list[str]:
        """
        Get primary key columns from record key.
        
        Args:
            record: CDC record
            
        Returns:
            List of primary key column names
        """
        if record.key:
            return list(record.key.keys())
        # Default to first column if no key info
        return list(record.value.keys())[:1]
    
    def _merge_into_postgres(
        self,
        records: list[dict],
        target_table: str,
        key_columns: list[str],
    ) -> int:
        """
        Perform MERGE INTO operation on PostgreSQL via DuckDB.
        
        Args:
            records: Records to merge
            target_table: Target table name
            key_columns: Primary key columns for merge condition
            
        Returns:
            Number of affected rows
        """
        if not records:
            return 0
        
        self._logger.info(f"Starting MERGE INTO for {len(records)} records to table '{target_table}'")
        
        try:
            # Get target table schema for type conversion
            table_schema = self._get_table_schema(target_table)
            self._logger.debug(f"Target schema for {target_table}: {table_schema}")
            
            # Create temporary source table
            temp_source = "_merge_source"
            columns = list(records[0].keys())
            
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")
            
            # Convert first record values using schema mapping
            def convert_record(record, log_first=False):
                converted = []
                for c in columns:
                    raw_value = record.get(c)
                    
                    # Get column info from schema (default to text if not found)
                    col_info = table_schema.get(c, {'type': 'text'})
                    pg_type = col_info.get('type', 'text')
                    
                    if log_first:
                        self._logger.info(f"  Column '{c}': raw={repr(raw_value)} (type={type(raw_value).__name__}) -> pg_type='{pg_type}' scale={col_info.get('scale')}")
                    
                    # Pass full column info to converter
                    converted_value = self._convert_debezium_value(raw_value, c, col_info)
                    
                    if log_first:
                        self._logger.info(f"    Converted: {repr(converted_value)} (type={type(converted_value).__name__})")
                    converted.append(converted_value)
                return converted
            
            # Log first record for debugging
            self._logger.info(f"=== DEBUG: First record raw data for table '{target_table}' ===")
            first_values = convert_record(records[0], log_first=True)
            self._logger.info("=== END DEBUG ===")
            
            # Map PostgreSQL types to DuckDB types for explicit table creation
            def pg_to_duckdb_type(pg_type):
                if pg_type in ('json', 'jsonb'):
                    print(f'pg_type: {pg_type}')
                if pg_type in ('date',): return 'DATE'
                if pg_type in ('timestamp', 'timestamp without time zone', 'timestamp with time zone'): return 'TIMESTAMP'
                if pg_type in ('time', 'time without time zone'): return 'TIME'
                if pg_type in ('integer', 'int', 'serial'): return 'INTEGER'
                if pg_type in ('bigint', 'bigserial'): return 'BIGINT'
                if pg_type in ('smallint',): return 'SMALLINT'
                if pg_type in ('boolean', 'bool'): return 'BOOLEAN'
                if pg_type in ('real', 'float4'): return 'FLOAT'
                if pg_type in ('double precision', 'float8', 'numeric', 'decimal'): return 'DOUBLE'
                # Store complex types as VARCHAR - PostgreSQL will handle implicit conversion
                if pg_type in ('json', 'jsonb'): return 'JSON'  # DuckDB has JSON type
                if pg_type == 'text[]': return 'VARCHAR[]'  # Array of text
                if pg_type == 'integer[]': return 'INTEGER[]'  # Array of integers
                if '[]' in pg_type: return 'VARCHAR[]'  
                if pg_type in ('geometry', 'geography'): return 'VARCHAR'  # Store as hex WKB
                return 'VARCHAR'  # Default fallback
                
            # Create explicit column definitions
            col_defs = []
            for c in columns:
                col_info = table_schema.get(c, {'type': 'text'})
                pg_type = col_info.get('type', 'text')
                duck_type = pg_to_duckdb_type(pg_type)
                col_defs.append(f'"{c}" {duck_type}')
            
            col_def_str = ", ".join(col_defs)
            self._duckdb_conn.execute(f"CREATE TABLE {temp_source} ({col_def_str})")
            
            # Insert values
            placeholders = ", ".join(["?" for _ in columns])
            
            # Insert all records with type-converted values
            for record in records:
                values = convert_record(record)
                self._duckdb_conn.execute(
                    f"INSERT INTO {temp_source} VALUES ({placeholders})",
                    values
                )
            
            
            # Build MERGE INTO statement
            full_table = f"pg_dest.{self.schema}.{target_table}"
            
            # Join condition on key columns
            join_conditions = " AND ".join([
                f'target."{k}" = source."{k}"' for k in key_columns
            ])
            
            # Update columns (excluding keys)
            update_cols = [c for c in columns if c not in key_columns]
            update_set = ", ".join([
                f'"{c}" = source."{c}"' for c in update_cols
            ])
            
            # Insert columns
            insert_cols = ", ".join([f'"{c}"' for c in columns])
            insert_vals = ", ".join([f'source."{c}"' for c in columns])
            
            merge_sql = f"""
                MERGE INTO {full_table} AS target
                USING {temp_source} AS source
                ON {join_conditions}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            
            self._duckdb_conn.execute(merge_sql)
            
            # Cleanup
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")
            
            return len(records)
            
        except Exception as e:
            self._logger.error(f"MERGE INTO failed: {e}")
            raise DestinationException(f"MERGE INTO failed: {e}")
    
    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write batch of records to PostgreSQL.
        
        Flow:
        1. Apply filter_sql (if defined)
        2. Apply custom_sql transformation (if defined)
        3. MERGE INTO destination table
        
        Args:
            records: CDC records to write
            table_sync: Table sync configuration
            
        Returns:
            Number of records written
        """
        if not self._is_initialized:
            self.initialize()
        
        if not records:
            return 0
        
        target_table = table_sync.table_name_target
        source_table = table_sync.table_name
        
        # Step 1: Apply filters
        filtered_records = self._apply_filters(records, table_sync.filter_sql)
        
        if not filtered_records:
            self._logger.debug(f"All records filtered out for {target_table}")
            return 0
        
        # Step 2: Apply custom SQL transformation
        if table_sync.custom_sql:
            transformed = self._execute_custom_sql(
                filtered_records, source_table, table_sync.custom_sql
            )
        else:
            transformed = [r.value for r in filtered_records]
        
        # Get primary key columns from first record
        key_columns = self._get_primary_key_columns(filtered_records[0])
        
        # Step 3: MERGE INTO destination
        written = self._merge_into_postgres(transformed, target_table, key_columns)
        
        self._logger.debug(f"Wrote {written} records to {target_table}")
        return written
    
    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: dict[str, Any],
    ) -> bool:
        """
        Create PostgreSQL table based on Debezium schema.
        
        Args:
            table_name: Target table name
            schema: Debezium schema dict
            
        Returns:
            True if table was created
        """
        if not self._pg_conn:
            self.initialize()
        
        try:
            # Check if table exists
            with self._pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (self.schema, table_name))
                exists = cur.fetchone()[0]
                
                if exists:
                    return False
                
                # Build CREATE TABLE from Debezium schema
                columns = self._schema_to_pg_columns(schema)
                
                if not columns:
                    self._logger.warning(f"No columns found in schema for {table_name}")
                    return False
                
                col_defs = ", ".join(columns)
                sql = f'CREATE TABLE "{self.schema}"."{table_name}" ({col_defs})'
                
                cur.execute(sql)
                self._logger.info(f"Created table: {self.schema}.{table_name}")
                return True
                
        except Exception as e:
            self._logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _schema_to_pg_columns(self, schema: dict[str, Any]) -> list[str]:
        """
        Convert Debezium schema to PostgreSQL column definitions.
        
        Args:
            schema: Debezium schema dict
            
        Returns:
            List of column definitions
        """
        # Debezium type to PostgreSQL type mapping
        type_map = {
            "int32": "INTEGER",
            "int64": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "string": "TEXT",
            "bytes": "BYTEA",
        }
        
        columns = []
        fields = schema.get("fields", [])
        
        for field in fields:
            name = field.get("field", field.get("name", ""))
            field_type = field.get("type", "string")
            
            if not name:
                continue
            
            # Handle complex types
            if isinstance(field_type, dict):
                field_type = field_type.get("type", "string")
            
            pg_type = type_map.get(field_type, "TEXT")
            optional = field.get("optional", True)
            
            col_def = f'"{name}" {pg_type}'
            if not optional:
                col_def += " NOT NULL"
            
            columns.append(col_def)
        
        return columns
    
    def close(self) -> None:
        """Close DuckDB and PostgreSQL connections."""
        if self._duckdb_conn:
            try:
                self._duckdb_conn.close()
            except Exception as e:
                self._logger.warning(f"Error closing DuckDB connection: {e}")
            self._duckdb_conn = None
        
        if self._pg_conn:
            try:
                self._pg_conn.close()
            except Exception as e:
                self._logger.warning(f"Error closing PostgreSQL connection: {e}")
            self._pg_conn = None
        
        self._is_initialized = False
        self._logger.info(f"PostgreSQL destination closed: {self._config.name}")
