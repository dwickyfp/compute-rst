use super::auth::AuthManager;
use super::dto::OpenChannelResponse;
use crate::config::SnowflakeConfig;
use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::{Value, json};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct SnowpipeClient {
    http: Client,
    config: SnowflakeConfig,
    auth: AuthManager, // AuthManager handle fingerprint & jwt
    base_url: String,
    ingest_host: Option<String>,
    scoped_token: Option<String>,
    channel_sequencer: u64,
}

impl SnowpipeClient {
    pub fn new(config: SnowflakeConfig) -> Result<Self> {
        let auth = AuthManager::new(&config)?;

        let account_url_format = config.account_id.replace('_', "-").to_lowercase();
        let base_url = format!("https://{}.snowflakecomputing.com", account_url_format);

        Ok(Self {
            http: Client::builder().user_agent("rosetta/0.1.0").build()?,
            config,
            auth,
            base_url,
            ingest_host: None,
            scoped_token: None,
            channel_sequencer: 0,
        })
    }

    pub async fn authenticate(&mut self) -> Result<()> {
        info!("Authenticating with Snowflake...");
        let jwt = self.auth.generate_jwt()?;

        // 1. Discovery
        if self.ingest_host.is_none() {
            let discover_url = format!("{}/v2/streaming/hostname", self.base_url);
            let resp = self
                .http
                .get(&discover_url)
                .header("Authorization", format!("Bearer {}", jwt))
                .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                .send()
                .await?;

            let status = resp.status();
            let body_text = resp.text().await?;
            info!("Discovery response ({}): '{}'", status, body_text);

            if !status.is_success() {
                return Err(anyhow!("Discovery failed: {}", body_text));
            }

            // Discovery returns raw hostname string, not JSON
            let raw_host = body_text.trim();
            self.ingest_host = Some(format!("https://{}", raw_host.replace('_', "-")));
            debug!("Ingest Host discovered: {:?}", self.ingest_host);
        }
        self.scoped_token = Some(jwt);

        Ok(())
    }

    pub async fn open_channel(&mut self, table_name: &str, channel_suffix: &str) -> Result<String> {
        if self.scoped_token.is_none() {
            self.authenticate().await?;
        }

        let pipe_name = format!("{}-STREAMING", table_name.to_uppercase());
        let channel_name = format!("{}_{}", pipe_name, channel_suffix);

        let target_db = self
            .config
            .landing_database
            .as_deref()
            .unwrap_or(&self.config.database);
        let target_schema = self
            .config
            .landing_schema
            .as_deref()
            .unwrap_or(&self.config.schema);

        let url = format!(
            "{}/v2/streaming/databases/{}/schemas/{}/pipes/{}/channels/{}",
            self.ingest_host.as_ref().unwrap(),
            target_db,
            target_schema,
            pipe_name,
            channel_name
        );

        let resp = self
            .http
            .put(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.scoped_token.as_ref().unwrap()),
            )
            .header("Content-Type", "application/json")
            .json(&json!({ "role": self.config.role }))
            .send()
            .await?;

        // Handle Token Expiry
        if resp.status().as_u16() == 401 {
            warn!("Token expired, re-authenticating...");
            self.authenticate().await?;
            return Box::pin(self.open_channel(table_name, channel_suffix)).await;
        }

        if !resp.status().is_success() {
            return Err(anyhow!("Open channel failed: {}", resp.text().await?));
        }

        let body: OpenChannelResponse = resp.json().await?;
        self.channel_sequencer = body.client_sequencer.unwrap_or(0);

        Ok(body.next_continuation_token.unwrap_or_default())
    }

    pub async fn insert_rows(
        &self,
        table_name: &str,
        channel_suffix: &str,
        rows: Vec<Value>,
        continuation_token: Option<String>,
    ) -> Result<String> {
        let pipe_name = format!("{}-STREAMING", table_name.to_uppercase());
        let channel_name = format!("{}_{}", pipe_name, channel_suffix);

        let target_db = self
            .config
            .landing_database
            .as_deref()
            .unwrap_or(&self.config.database);
        let target_schema = self
            .config
            .landing_schema
            .as_deref()
            .unwrap_or(&self.config.schema);

        let url = format!(
            "{}/v2/streaming/data/databases/{}/schemas/{}/pipes/{}/channels/{}/rows",
            self.ingest_host.as_ref().unwrap(),
            target_db,
            target_schema,
            pipe_name,
            channel_name
        );

        let mut ndjson_body = String::new();
        for row in rows {
            ndjson_body.push_str(&row.to_string());
            ndjson_body.push('\n');
        }

        let mut req = self
            .http
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.scoped_token.as_ref().unwrap()),
            )
            .header("Content-Type", "application/x-ndjson")
            .header(
                "X-Snowflake-Client-Sequencer",
                self.channel_sequencer.to_string(),
            );

        if let Some(token) = continuation_token {
            if !token.is_empty() {
                req = req.query(&[("continuationToken", token)]);
            }
        }

        let resp = req.body(ndjson_body).send().await?;

        if !resp.status().is_success() {
            return Err(anyhow!("Insert rows failed: {}", resp.text().await?));
        }

        let res_json: Value = resp.json().await?;
        let next_token = res_json
            .get("next_continuation_token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Ok(next_token)
    }
}
