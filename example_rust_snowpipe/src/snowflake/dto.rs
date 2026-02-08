use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct OpenChannelRequest {
    pub role: String,
}

#[derive(Deserialize, Debug)]
pub struct OpenChannelResponse {
    pub client_sequencer: Option<u64>,
    pub next_continuation_token: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct TokenResponse {
    pub access_token: String,
}
