use crate::config::SnowflakeConfig;
use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::{
    RsaPrivateKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
    aud: String,
}

// Struct untuk menyimpan key yang sudah di-load agar tidak baca file terus menerus
#[derive(Debug, Clone)]
pub struct AuthManager {
    private_key_pem: String,
    public_key_fingerprint: String,
    account_id: String,
    user: String,
}

impl AuthManager {
    pub fn new(config: &SnowflakeConfig) -> Result<Self> {
        // 1. Get Private Key Content
        let key_content = &config.private_key;

        // 2. Parse Private Key (Support Encrypted & Unencrypted)
        let private_key = if let Some(pass) = &config.private_key_passphrase {
            // Jika ada password
            RsaPrivateKey::from_pkcs8_encrypted_pem(&key_content, pass)
                .map_err(|e| anyhow!("Failed to decrypt private key: {}", e))?
        } else {
            // Jika tidak ada password
            RsaPrivateKey::from_pkcs8_pem(&key_content)
                .map_err(|e| anyhow!("Failed to parse private key: {}", e))?
        };

        // 3. Extract Public Key -> DER -> SHA256 -> Base64 (Fingerprint)
        // Ini ekuivalen dengan logika Python documentation Snowflake
        let public_key_der = private_key
            .to_public_key()
            .to_public_key_der()
            .map_err(|e| anyhow!("Failed to get public key DER: {}", e))?;

        let mut hasher = Sha256::new();
        hasher.update(public_key_der.as_bytes());
        let result = hasher.finalize();
        let fingerprint = BASE64.encode(result);

        // Format Fingerprint Snowflake: "SHA256:<base64>"
        let full_fingerprint = format!("SHA256:{}", fingerprint);

        Ok(Self {
            // Kita simpan versi PEM string murni untuk library jsonwebtoken
            // (Library jsonwebtoken butuh PEM string untuk signing)
            private_key_pem: private_key.to_pkcs8_pem(Default::default())?.to_string(),
            public_key_fingerprint: full_fingerprint,
            account_id: config.account_id.clone(),
            user: config.user.clone(),
        })
    }

    pub fn generate_jwt(&self) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let qualified_username = format!(
            "{}.{}",
            self.account_id.to_uppercase(),
            self.user.to_uppercase()
        );
        // Issuer format: ACCOUNT.USER.SHA256:FINGERPRINT
        let issuer = format!("{}.{}", qualified_username, self.public_key_fingerprint);

        let account_url_format = self.account_id.replace('_', "-").to_lowercase();
        // Audience should be the Token Endpoint URL? Or just the Account URL?
        // Usually: https://<account>.snowflakecomputing.com
        let aud_url = format!("https://{}.snowflakecomputing.com", account_url_format);

        let claims = Claims {
            iss: issuer.clone(),
            sub: qualified_username.clone(),
            iat: now,
            exp: now + 3600, // 1 hour
            aud: aud_url,
        };
        let header = Header::new(Algorithm::RS256);
        let key = EncodingKey::from_rsa_pem(self.private_key_pem.as_bytes())?;

        encode(&header, &claims, &key).context("Failed to sign JWT")
    }
}
