"""
Snowflake destination module using native Connect REST API.

Provides real-time data ingestion to Snowflake without external SDK dependencies.
"""

from compute.destinations.snowflake.destination import SnowflakeDestination

__all__ = ["SnowflakeDestination"]
