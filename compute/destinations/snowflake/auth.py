"""
Snowflake JWT Authentication Manager.

Handles RSA key-based authentication with JWT token generation,
matching the Snowflake Connect API requirements.
"""

import base64
import hashlib
import time
from typing import Optional

import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_pem_private_key,
)


class AuthManager:
    """
    Manages Snowflake key-pair authentication.
    
    Handles:
    - Loading RSA private keys (encrypted/unencrypted PKCS8 PEM)
    - Computing public key fingerprint (SHA256 of DER-encoded public key)
    - Generating JWT tokens with Snowflake-compliant claims
    """

    def __init__(
        self,
        account_id: str,
        user: str,
        private_key_pem: str,
        passphrase: Optional[str] = None,
    ):
        """
        Initialize authentication manager.

        Args:
            account_id: Snowflake account identifier
            user: Snowflake username
            private_key_pem: PEM-encoded private key string
            passphrase: Optional passphrase if key is encrypted
        """
        self.account_id = account_id
        self.user = user
        
        # Normalize key content
        key_bytes = private_key_pem.encode("utf-8")
        passphrase_bytes = passphrase.encode("utf-8") if passphrase else None
        
        # Load private key
        self._private_key = load_pem_private_key(
            key_bytes,
            password=passphrase_bytes,
            backend=default_backend(),
        )
        
        # Compute public key fingerprint
        self._fingerprint = self._compute_fingerprint()

    def _compute_fingerprint(self) -> str:
        """
        Compute the public key fingerprint.
        
        Snowflake expects: SHA256:<base64-encoded-hash>
        Hash is computed over the DER-encoded public key.
        
        Returns:
            Fingerprint string in format "SHA256:<base64>"
        """
        # Get public key in DER format
        public_key = self._private_key.public_key()
        public_key_der = public_key.public_bytes(
            encoding=Encoding.DER,
            format=PublicFormat.SubjectPublicKeyInfo,
        )
        
        # Compute SHA256 hash
        sha256_hash = hashlib.sha256(public_key_der).digest()
        
        # Encode as base64
        fingerprint_b64 = base64.b64encode(sha256_hash).decode("utf-8")
        
        return f"SHA256:{fingerprint_b64}"

    def generate_jwt(self) -> str:
        """
        Generate a signed JWT token for Snowflake authentication.
        
        Token structure follows Snowflake Connect API requirements:
        - iss: ACCOUNT.USER.SHA256:FINGERPRINT
        - sub: ACCOUNT.USER
        - iat: Current timestamp
        - exp: Current timestamp + 1 hour
        - aud: https://<account>.snowflakecomputing.com
        
        Returns:
            Signed JWT token string
        """
        now = int(time.time())
        
        # Qualified username: ACCOUNT.USER (uppercase)
        qualified_username = f"{self.account_id.upper()}.{self.user.upper()}"
        
        # Issuer: ACCOUNT.USER.SHA256:FINGERPRINT
        issuer = f"{qualified_username}.{self._fingerprint}"
        
        # Audience URL (replace underscores with hyphens for URL)
        account_url_format = self.account_id.replace("_", "-").lower()
        audience = f"https://{account_url_format}.snowflakecomputing.com"
        
        # JWT claims
        claims = {
            "iss": issuer,
            "sub": qualified_username,
            "iat": now,
            "exp": now + 3600,  # 1 hour expiry
            "aud": audience,
        }
        
        # Get private key in PEM format for PyJWT
        private_key_pem = self._private_key.private_bytes(
            encoding=Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        
        # Sign and return JWT
        return jwt.encode(claims, private_key_pem, algorithm="RS256")
