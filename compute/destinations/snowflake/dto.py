"""
Data transfer objects for Snowflake Snowpipe Streaming API.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OpenChannelRequest:
    """Request payload for opening a streaming channel."""
    role: str


@dataclass
class OpenChannelResponse:
    """Response from opening a streaming channel."""
    client_sequencer: Optional[int] = None
    next_continuation_token: Optional[str] = None


@dataclass
class SnowflakeSyncConfig:
    """Configuration for a table sync destination."""
    id: Optional[int]
    target_table: str
