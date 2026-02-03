from typing import List
import json
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import DebeziumJsonEngine


class PrintChangeHandler(BasePythonChangeHandler):
    """
    A custom change event handler class.
    This class processes batches of Debezium change events received from the engine.
    The `handleJsonBatch` method is where you implement your logic for consuming
    and processing these events. Currently, it prints basic information about
    each event to the console.
    """

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium change events.
        """
        print(f"Received {len(records)} records")
        for record in records:
            # Get destination/topic
            destination = record.destination()
            
            # Parse key and value JSON
            key_data = record.key()
            value_data = record.value()
            
            # Ensure we have Python strings (handle java.lang.String from JPype)
            # and parse them into dicts
            key_obj = json.loads(str(key_data)) if key_data is not None else {}
            value_obj = json.loads(str(value_data)) if value_data is not None else {}
            
            # Debezium JSON usually has "schema" and "payload" top-level keys
            payload = value_obj.get("payload", {})
            op = payload.get("op")
            
            # Extract key payload
            key = key_obj.get("payload", {}) if isinstance(key_obj, dict) else key_obj
            
            # Extract value based on operation
            # op types: c=create, u=update, d=delete, r=read (snapshot), m=message (heartbeat/wal message)
            if op in ('c', 'u', 'r'):
                value = payload.get("after")
            elif op == 'd':
                value = payload.get("before")
            elif op == 'm':
                # For 'm' (message), the data is typically in the 'message' field
                value = payload.get("message")
            else:
                value = payload # fallback to the whole payload if op is unknown
            
            print(f"op: {op}, key: {key}, value: {value}, destinations: {destination}")
        print("--------------------------------------")


if __name__ == "__main__":
    props = {
        "name": "postgres-cdc-engine",  # Unique name for the engine
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": "./tmp/offsets.dat",  # Path to store offsets (adjust as needed)
        "offset.flush.interval.ms": "60000",  # How often to flush offsets
        # PostgreSQL connection properties
        "database.hostname": "172.16.62.98",  # Replace with your PostgreSQL host
        "database.port": "5455",  # Default PostgreSQL port
        "database.user": "postgres",  # Replace with your database username
        "database.password": "postgres",  # Replace with your database password
        "database.dbname": "postgres",  # Replace with your database name
        # Snapshot and CDC behavior
        "snapshot.mode": "no_data",  # Preferred in Debezium 3.x to skip data snapshot
        "slot.name": "supabase_etl_apply_1",  # PostgreSQL replication slot name (must be unique)
        "plugin.name": "pgoutput",  # Decoder plugin (pgoutput is standard for PostgreSQL 10+)
        # Heartbeat configuration to prevent WAL growth
        "heartbeat.interval.ms": "10000",  # Send heartbeat every 10 seconds (adjust as needed)
        "heartbeat.action.query": "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar)",  # Query to advance WAL without a table
        # Optional: Include/exclude specific schemas or tables
        "schema.include.list": "public",  # Comma-separated list of schemas to include
        "table.include.list": "public.tbl_sales_stream_company",  # Comma-separated list of tables to include
        "publication.name": "my_publication",
        # Additional production configurations
        "slot.drop.on.stop": "false",  # Do not drop replication slot on connector stop (default: false)
        "max.batch.size": "2048",  # Maximum number of events per batch (adjust for performance)
        "max.queue.size": "8192",  # Maximum queue size (adjust for memory constraints)
        "poll.interval.ms": "500",  # How often to poll for new changes
        "slot.max.retries": "6",  # Number of retries if slot is not available
        "slot.retry.delay.ms": "10000",  # Delay between retries
        "topic.prefix": "rosetta",
        # Additional properties as needed (e.g., for SSL, transforms, etc.)
        # "database.sslmode": "require",  # If using SSL
    }

    # CRITICAL: To skip old data, you must manually reset the following:
    # 1. Delete the offset file: ./tmp/offsets.dat
    # 2. Drop the replication slot in PostgreSQL:
    #    SELECT pg_drop_replication_slot('supabase_etl_apply_1');
    # Note: Ensure your PostgreSQL is configured for CDC:
    # - Set wal_level = logical in postgresql.conf
    # - Grant replication permissions to the user
    # - Restart PostgreSQL after changes
    # For heartbeat: Ensure PostgreSQL version supports pg_logical_emit_message (PG 9.4+)
    # Create a DebeziumJsonEngine instance, passing the configuration properties and the custom change event handler.
    engine = DebeziumJsonEngine(properties=props, handler=PrintChangeHandler())
    # Start the Debezium engine to begin consuming and processing change events.
    engine.run()
