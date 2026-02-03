# Destinations module
from compute.destinations.base import BaseDestination
from compute.destinations.snowflake import SnowflakeDestination
from compute.destinations.postgresql import PostgreSQLDestination

__all__ = ["BaseDestination", "SnowflakeDestination", "PostgreSQLDestination"]
