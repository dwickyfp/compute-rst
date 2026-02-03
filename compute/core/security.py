"""
Security utilities for credential encryption.

Implements AES-256-GCM encryption for securing sensitive data at rest.
"""

import base64
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _get_encryption_key() -> str:
    """Get encryption key from environment variable."""
    key = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")
    if not key:
        raise ValueError("CREDENTIAL_ENCRYPTION_KEY environment variable is not set")
    return key


def get_cipher() -> AESGCM:
    """
    Get AESGCM cipher instance using the configured encryption key.
    
    The key must be 32 bytes (256 bits) for AES-256.
    """
    key = _get_encryption_key()
    
    # Try decoding if it looks like base64
    try:
        decoded = base64.b64decode(key)
        if len(decoded) == 32:
            return AESGCM(decoded)
    except Exception:
        pass
        
    # If not base64 or length mismatch, check if the string itself is 32 bytes
    if len(key.encode()) == 32:
        return AESGCM(key.encode())
        
    # Fallback
    return AESGCM(key.encode() if isinstance(key, str) else key)


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value using AES-256-GCM.
    
    Format: base64(nonce + ciphertext + tag)
    """
    if not value:
        return value
        
    aesgcm = get_cipher()
    nonce = os.urandom(12)  # 96-bit nonce
    
    ciphertext = aesgcm.encrypt(nonce, value.encode(), None)
    combined = nonce + ciphertext
    return base64.b64encode(combined).decode('utf-8')


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt a base64 encoded string using AES-256-GCM.
    
    If decryption fails (e.g., value is not encrypted), returns the original value.
    """
    if not encrypted_value:
        return encrypted_value
    
    # Check if encryption key is configured
    try:
        key = _get_encryption_key()
    except ValueError:
        # No encryption key configured, assume value is not encrypted
        return encrypted_value
        
    try:
        combined = base64.b64decode(encrypted_value)
        
        # Extract nonce (first 12 bytes)
        nonce = combined[:12]
        ciphertext = combined[12:]
        
        aesgcm = get_cipher()
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode('utf-8')
    except Exception:
        # If decryption fails, assume value is not encrypted (legacy)
        return encrypted_value
