use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use std::env;

/// Decrypts a base64 encoded string using AES-256-GCM.
/// 
/// Expectations:
/// - Environment variable `CREDENTIAL_ENCRYPTION_KEY` must be set (32 bytes).
/// - Input `encrypted_value` is base64 encoded string containing `Nonce (12 bytes) + Ciphertext + Tag (16 bytes)`.
pub fn decrypt_value(encrypted_value: &str) -> Result<String> {
    if encrypted_value.is_empty() {
        return Ok(encrypted_value.to_string());
    }

    let key_str = env::var("CREDENTIAL_ENCRYPTION_KEY")
        .map_err(|_| anyhow!("CREDENTIAL_ENCRYPTION_KEY must be set"))?;

    // Prepare key
    let key_bytes = if let Ok(decoded) = general_purpose::STANDARD.decode(&key_str) {
        if decoded.len() == 32 {
            decoded
        } else {
            // If decode success but length wrong, maybe it was a raw string that coincidentally is valid base64?
            // Safer to assume if it decodes to 32 bytes it is the key.
            // If not, fallback to raw bytes logic or error.
            // Python side: "if len(decoded) == 32: return AESGCM(decoded)"
            // "if len(key.encode()) == 32: return AESGCM(key.encode())"
            
            if key_str.len() == 32 {
                key_str.as_bytes().to_vec()
            } else {
                 return Err(anyhow!("Invalid key length. Must be 32 bytes (raw) or base64 encoded 32 bytes."));
            }
        }
    } else {
        if key_str.len() == 32 {
            key_str.as_bytes().to_vec()
        } else {
            return Err(anyhow!("Invalid key length. Must be 32 bytes."));
        }
    };

    let cipher = Aes256Gcm::new_from_slice(&key_bytes)
        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;

    // Decode base64 input
    let combined = general_purpose::STANDARD
        .decode(encrypted_value)
        .map_err(|e| anyhow!("Failed to decode base64 value: {}", e))?;

    if combined.len() < 12 {
        return Err(anyhow!("Encrypted value too short"));
    }

    // Extract Nonce (first 12 bytes)
    let (nonce_bytes, ciphertext) = combined.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    // Decrypt
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow!("Decryption failed: {}", e))?;

    Ok(String::from_utf8(plaintext)?)
}
