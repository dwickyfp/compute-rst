# Rosetta Compute Engine

Modular Debezium-based CDC (Change Data Capture) engine for streaming data from PostgreSQL to multiple destinations.

## Features

- **Multiple Sources**: PostgreSQL with extensible base class for future sources
- **Multiple Destinations**: Snowflake (Snowpipe Streaming), PostgreSQL (DuckDB MERGE INTO)
- **Multiprocessing**: Each pipeline runs in its own process for fault isolation
- **Filter & Transform**: Apply SQL filters and custom transformations before writing
- **Monitoring**: Track record counts in `data_flow_record_monitoring` table

## Installation

```bash
# Install dependencies
uv sync

# For Snowflake support
uv sync --extra snowflake
```

## Configuration

Copy `.env.example` to `.env` and configure:

```env
# PostgreSQL Database (Rosetta Config DB)
ROSETTA_DB_HOST=localhost
ROSETTA_DB_PORT=5432
ROSETTA_DB_NAME=rosetta
ROSETTA_DB_USER=postgres
ROSETTA_DB_PASSWORD=postgres

# Pipeline Settings
PIPELINE_ID=                    # Optional: Run specific pipeline
DEBUG=false                     # Enable debug logging

# Logging
LOG_LEVEL=INFO
```

## Usage

### Run the Engine

```bash
# Run all active pipelines (status = 'START')
uv run rosetta-compute

# Run specific pipeline (set PIPELINE_ID in .env)
PIPELINE_ID=1 uv run rosetta-compute

# Enable debug logging
DEBUG=true uv run rosetta-compute

# Alternative: run as module
uv run python -m compute.main
```

### Database Setup

Run the migrations to create required tables:

```bash
psql -h localhost -U postgres -d rosetta -f migrations/001_create_table.sql
```

### Create a Pipeline

1. Add a source in `sources` table
2. Add a destination in `destinations` table
3. Create a pipeline in `pipelines` table linking source
4. Add destination mappings in `pipelines_destination`
5. Configure table syncs in `pipelines_destination_table_sync`
6. Set pipeline status to `'START'`

## Architecture

```
compute/
├── config/          # Configuration management
├── core/            # Database, models, engine
├── sources/         # BaseSource, PostgreSQLSource
└── destinations/    # BaseDestination, Snowflake, PostgreSQL
```

## PostgreSQL Destination Flow

1. **Create Table**: Auto-create from CDC schema
2. **Filter**: Apply `filter_sql` (e.g., `column_1 = '11';column_2>1`)
3. **Transform**: Execute `custom_sql` with table reference
4. **Merge**: DuckDB MERGE INTO for upserts

## License

MIT
