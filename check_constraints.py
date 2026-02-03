from compute.core.database import DatabaseSession, init_connection_pool

def check_schema():
    init_connection_pool()
    with DatabaseSession() as session:
        # Check pipeline_metadata constraint
        print("Checking pipeline_metadata constraints:")
        session.execute("""
            SELECT conname, pg_get_constraintdef(oid) 
            FROM pg_constraint 
            WHERE conrelid = 'pipeline_metadata'::regclass
        """)
        for row in session.fetchall():
            print(f"- {row['conname']}: {row['pg_get_constraintdef']}")

        # Check data_flow_record_monitoring constraint
        print("\nChecking data_flow_record_monitoring constraints:")
        session.execute("""
            SELECT conname, pg_get_constraintdef(oid) 
            FROM pg_constraint 
            WHERE conrelid = 'data_flow_record_monitoring'::regclass
        """)
        for row in session.fetchall():
            print(f"- {row['conname']}: {row['pg_get_constraintdef']}")

if __name__ == "__main__":
    try:
        check_schema()
    except Exception as e:
        print(f"Error: {e}")
