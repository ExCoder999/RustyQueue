use axum::{
    async_trait,
    body::Bytes,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::de::DeserializeOwned;
use serde_json::json;

const MAX_PAYLOAD_BYTES: usize = 512 * 1024; // 512 KB

pub struct BoundedJson<T>(pub T);

pub enum BoundedJsonRejection {
    TooLarge,
    InvalidJson(String),
}

impl IntoResponse for BoundedJsonRejection {
    fn into_response(self) -> Response {
        match self {
            BoundedJsonRejection::TooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "error": "Payload exceeds 512 KB limit" })),
            )
                .into_response(),
            BoundedJsonRejection::InvalidJson(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": msg })),
            )
                .into_response(),
        }
    }
}

#[async_trait]
impl<T, S> FromRequest<S> for BoundedJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = BoundedJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| BoundedJsonRejection::InvalidJson("Failed to read body".to_string()))?;

        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(BoundedJsonRejection::TooLarge);
        }

        let value: T = serde_json::from_slice(&bytes)
            .map_err(|e| BoundedJsonRejection::InvalidJson(e.to_string()))?;

        Ok(BoundedJson(value))
    }
}
