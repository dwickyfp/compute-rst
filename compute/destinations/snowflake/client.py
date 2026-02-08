"""
Snowpipe Streaming REST API Client.

Handles communication with Snowflake's streaming ingest REST API,
including authentication, channel management, and row insertion.
"""

import json
import logging
from typing import Any, Optional

import httpx

from compute.destinations.snowflake.auth import AuthManager
from compute.destinations.snowflake.dto import OpenChannelResponse

logger = logging.getLogger(__name__)


class SnowpipeClient:
    """
    REST client for Snowflake Snowpipe Streaming API.
    
    Handles:
    - Hostname discovery
    - JWT authentication
    - Channel open/create operations
    - Row insertion with NDJSON format
    - Automatic token refresh on expiry
    """

    def __init__(
        self,
        account_id: str,
        user: str,
        private_key_pem: str,
        database: str,
        schema: str,
        role: str,
        landing_database: Optional[str] = None,
        landing_schema: Optional[str] = None,
        passphrase: Optional[str] = None,
    ):
        """
        Initialize Snowpipe client.

        Args:
            account_id: Snowflake account identifier
            user: Snowflake username
            private_key_pem: PEM-encoded private key
            database: Target database
            schema: Target schema
            role: Snowflake role for operations
            landing_database: Override database for landing tables
            landing_schema: Override schema for landing tables
            passphrase: Optional passphrase for encrypted key
        """
        self._auth = AuthManager(account_id, user, private_key_pem, passphrase)
        self._account_id = account_id
        self._database = database
        self._schema = schema
        self._role = role
        self._landing_database = landing_database or database
        self._landing_schema = landing_schema or schema
        
        # Compute base URL
        account_url_format = account_id.replace("_", "-").lower()
        self._base_url = f"https://{account_url_format}.snowflakecomputing.com"
        
        # State
        self._ingest_host: Optional[str] = None
        self._scoped_token: Optional[str] = None
        self._channel_sequencer: int = 0
        
        # Persistent HTTP client (like Rust's reqwest::Client)
        # Initialized once at construction for connection stability
        self._http: httpx.AsyncClient = httpx.AsyncClient(
            timeout=60.0,
            headers={"User-Agent": "rosetta/0.1.0"},
        )

    def _get_http_client(self) -> httpx.AsyncClient:
        """Get the persistent HTTP client, recreating if closed."""
        if self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=60.0,
                headers={"User-Agent": "rosetta/0.1.0"},
            )
        return self._http

    async def authenticate(self) -> None:
        """
        Authenticate with Snowflake and discover ingest host.
        
        Performs hostname discovery to find the streaming ingest endpoint.
        """
        logger.info("[SnowpipeClient] Starting authentication...")
        jwt_token = self._auth.generate_jwt()
        logger.info("[SnowpipeClient] JWT token generated successfully")
        
        # Discover ingest host if not already known
        if self._ingest_host is None:
            discover_url = f"{self._base_url}/v2/streaming/hostname"
            logger.info(f"[SnowpipeClient] Discovering ingest host at: {discover_url}")
            
            http = self._get_http_client()
            response = await http.get(
                discover_url,
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                    "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
                },
            )
            
            logger.info(f"[SnowpipeClient] Discovery response status: {response.status_code}")
            
            if not response.is_success:
                logger.error(f"[SnowpipeClient] Discovery failed: {response.text}")
                raise Exception(f"Discovery failed: {response.text}")
            
            # Discovery returns raw hostname string, not JSON
            raw_host = response.text.strip()
            self._ingest_host = f"https://{raw_host.replace('_', '-')}"
            logger.info(f"[SnowpipeClient] Ingest Host discovered: {self._ingest_host}")
        
        self._scoped_token = jwt_token
        logger.info("[SnowpipeClient] Authentication completed successfully")

    async def open_channel(
        self,
        table_name: str,
        channel_suffix: str = "default",
    ) -> OpenChannelResponse:
        """
        Open or create a streaming channel for a table.

        Args:
            table_name: Target table name (will be uppercased)
            channel_suffix: Suffix for channel name

        Returns:
            Continuation token for subsequent operations
        """
        logger.info(f"[SnowpipeClient] Opening channel for table: {table_name}")
        
        if self._scoped_token is None:
            await self.authenticate()
        
        pipe_name = f"{table_name.upper()}-STREAMING"
        channel_name = f"{pipe_name}_{channel_suffix}"
        
        url = (
            f"{self._ingest_host}/v2/streaming/databases/{self._landing_database}"
            f"/schemas/{self._landing_schema}/pipes/{pipe_name}/channels/{channel_name}"
        )
        
        logger.info(f"[SnowpipeClient] Opening channel at: {url}")
        
        http = self._get_http_client()
        response = await http.put(
            url,
            headers={
                "Authorization": f"Bearer {self._scoped_token}",
                "Content-Type": "application/json",
            },
            json={"role": self._role},
        )
        
        logger.info(f"[SnowpipeClient] Open channel response status: {response.status_code}")
        
        # Handle token expiry
        if response.status_code == 401:
            logger.warning("Token expired, re-authenticating...")
            await self.authenticate()
            return await self.open_channel(table_name, channel_suffix)
        
        if not response.is_success:
            raise Exception(f"Open channel failed: {response.text}")
        
        body = response.json()
        
        channel_response = OpenChannelResponse(
            client_sequencer=body.get("client_sequencer"),
            next_continuation_token=body.get("next_continuation_token"),
        )
        
        self._channel_sequencer = channel_response.client_sequencer or 0
        
        return channel_response

    async def insert_rows(
        self,
        table_name: str,
        channel_suffix: str,
        rows: list[dict[str, Any]],
        continuation_token: Optional[str] = None,
    ) -> str:
        """
        Insert rows into a streaming channel.

        Args:
            table_name: Target table name
            channel_suffix: Channel suffix
            rows: List of row dictionaries to insert
            continuation_token: Token from previous operation
            offset_token: Offset token for persistence tracking
            
        Returns:
            Next continuation token
        """
        logger.info(f"[SnowpipeClient] Inserting {len(rows)} rows to table: {table_name}")
        
        pipe_name = f"{table_name.upper()}-STREAMING"
        channel_name = f"{pipe_name}_{channel_suffix}"
        
        url = (
            f"{self._ingest_host}/v2/streaming/data/databases/{self._landing_database}"
            f"/schemas/{self._landing_schema}/pipes/{pipe_name}/channels/{channel_name}/rows"
        )
        
        # Build NDJSON body
        ndjson_body = "\n".join(json.dumps(row) for row in rows) + "\n"

        logger.info(f"[SnowpipeClient] Insert URL: {url}")
        
        # Build request
        headers = {
            "Authorization": f"Bearer {self._scoped_token}",
            "Content-Type": "application/x-ndjson",
            "X-Snowflake-Client-Sequencer": str(self._channel_sequencer),
        }
        
        params = {}
        if continuation_token:
            params["continuationToken"] = continuation_token
        
        http = self._get_http_client()
        response = await http.post(
            url,
            headers=headers,
            params=params if params else None,
            content=ndjson_body,
        )
        
        logger.info(f"[SnowpipeClient] Insert response status: {response.status_code}")
        
        # Handle token expiry
        if response.status_code == 401:
            logger.warning("Token expired, re-authenticating...")
            await self.authenticate()
            # Re-open channel after re-auth
            # Note: We need to handle the response type change of open_channel here too
            channel_resp = await self.open_channel(table_name, channel_suffix)
            # Use the new tokens from re-open
            return await self.insert_rows(
                table_name, 
                channel_suffix, 
                rows, 
                channel_resp.next_continuation_token,
            )
        
        if not response.is_success:
            # Log exact error response for debugging
            logger.error(f"[SnowpipeClient] Insert failed details: {response.text}")
            raise Exception(f"Insert rows failed: {response.text}")
        
        res_json = response.json()
        return res_json.get("next_continuation_token", "")

    async def close(self) -> None:
        """Close the persistent HTTP client."""
        if not self._http.is_closed:
            await self._http.aclose()
