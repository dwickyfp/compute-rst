import logging
import sys
from compute.core.database import DatabaseSession, init_connection_pool

logging.basicConfig(filename='schema_fix.log', level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_schema():
    init_connection_pool()
    with DatabaseSession() as session:
        logger.info("Starting schema fix...")
        
        # 1. Pipeline Metadata
        try:
            # Check if constraint exists
            session.execute("""
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'uq_pipeline_metadata_pipeline_id'
            """)
            if not session.fetchone():
                logger.info("Adding constraint uq_pipeline_metadata_pipeline_id")
                # Drop old if exists under different name or just in case
                session.execute("ALTER TABLE pipeline_metadata DROP CONSTRAINT IF EXISTS uq_pipeline_metadata_pipeline_id")
                session.execute("ALTER TABLE pipeline_metadata ADD CONSTRAINT uq_pipeline_metadata_pipeline_id UNIQUE (pipeline_id)")
                logger.info("Constraint added successfully")
            else:
                logger.info("Constraint uq_pipeline_metadata_pipeline_id already exists")
        except Exception as e:
            logger.error(f"Error fixing pipeline_metadata: {e}")

        # 2. Data Flow Record Monitoring
        try:
            session.execute("""
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'uq_data_flow_monitoring_unique'
            """)
            if not session.fetchone():
                logger.info("Adding constraint uq_data_flow_monitoring_unique")
                session.execute("ALTER TABLE data_flow_record_monitoring DROP CONSTRAINT IF EXISTS uq_data_flow_monitoring_unique")
                session.execute("ALTER TABLE data_flow_record_monitoring ADD CONSTRAINT uq_data_flow_monitoring_unique UNIQUE (pipeline_id, pipeline_destination_id, pipeline_destination_table_sync_id, table_name)")
                logger.info("Constraint added successfully")
            else:
                logger.info("Constraint uq_data_flow_monitoring_unique already exists")
        except Exception as e:
            logger.error(f"Error fixing data_flow_record_monitoring: {e}")

if __name__ == "__main__":
    try:
        fix_schema()
        print("Schema fix execution completed")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
