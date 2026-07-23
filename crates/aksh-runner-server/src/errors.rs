use super::*;
use serde_json::Value;

/// Convert handler/extractor failures on runner-facing protocol routes into the
/// wire envelopes emitted by Azure DevOps and the Actions runner services.
///
/// Native `/api/v1` endpoints intentionally retain [`ApiError`]'s historical
/// `{ "error": ... }` shape; this middleware is path-scoped to the protocol
/// surfaces only.
/// Explicit fallback keeps unmatched runner paths inside the middleware stack.
pub(crate) async fn protocol_not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}
pub(crate) async fn protocol_error_envelope(request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let response = next.run(request).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }

    let envelope = if path.starts_with("/twirp/") {
        Some(ProtocolEnvelope::Twirp)
    } else if path.starts_with("/broker/") {
        Some(ProtocolEnvelope::Broker)
    } else if path.contains("/_apis") {
        Some(ProtocolEnvelope::Azdo)
    } else {
        None
    };
    let Some(envelope) = envelope else {
        return response;
    };

    let mut headers = response.headers().clone();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let message = response_error_message(&body, status);
    let (content_type, payload) = match envelope {
        ProtocolEnvelope::Azdo => (
            "application/json; charset=utf-8",
            azdo_error_payload(status, &message),
        ),
        ProtocolEnvelope::Broker => ("application/json", broker_error_payload(status, &message)),
        ProtocolEnvelope::Twirp => ("application/json", twirp_error_payload(status, &message)),
    };
    headers.remove(header::CONTENT_LENGTH);
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    let mut output = Response::new(Body::from(payload.to_string()));
    *output.status_mut() = status;
    *output.headers_mut() = headers;
    output
}

#[derive(Clone, Copy)]
enum ProtocolEnvelope {
    Azdo,
    Broker,
    Twirp,
}

fn response_error_message(body: &[u8], status: StatusCode) -> String {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        for key in ["message", "msg", "errorMessage", "error_description"] {
            if let Some(message) = value.get(key).and_then(Value::as_str) {
                return message.to_owned();
            }
        }
        if let Some(message) = value.get("error").and_then(Value::as_str) {
            return message.to_owned();
        }
    }
    let text = String::from_utf8_lossy(body).trim().to_owned();
    if text.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("Request failed")
            .to_owned()
    } else {
        text
    }
}

fn azdo_error_payload(status: StatusCode, message: &str) -> Value {
    let type_key = match status {
        StatusCode::UNAUTHORIZED => "UnauthorizedRequestException",
        StatusCode::FORBIDDEN => "UnauthorizedRequestException",
        StatusCode::NOT_FOUND => "ResourceNotFoundException",
        StatusCode::BAD_REQUEST => "VssInvalidRequestException",
        _ => "VssServerException",
    };
    let type_name = format!(
        "Microsoft.VisualStudio.Services.Common.{type_key}, Microsoft.VisualStudio.Services.Common"
    );
    json!({
        "$type": "Microsoft.VisualStudio.Services.Common.VssException, Microsoft.VisualStudio.Services.Common",
        "$id": "1",
        "innerException": null,
        "message": message,
        "typeName": type_name,
        "typeKey": type_key,
        "errorCode": 0,
        "eventId": 3000
    })
}

fn broker_error_payload(status: StatusCode, message: &str) -> Value {
    let error_kind = match status {
        StatusCode::UNAUTHORIZED => "Unauthorized",
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "RunnerNotFound",
        StatusCode::BAD_REQUEST => "InvalidRequest",
        _ => "InternalServerError",
    };
    json!({
        "source": "actions-broker-listener",
        "errorMessage": message,
        "errorKind": error_kind,
        "statusCode": status.as_u16()
    })
}

fn twirp_error_payload(status: StatusCode, message: &str) -> Value {
    let code = match status {
        StatusCode::BAD_REQUEST => "invalid_argument",
        StatusCode::UNAUTHORIZED => "unauthenticated",
        StatusCode::FORBIDDEN => "permission_denied",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::METHOD_NOT_ALLOWED => "unimplemented",
        StatusCode::CONFLICT => "already_exists",
        _ => "internal",
    };
    json!({ "code": code, "msg": message })
}

/// API error.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    pub(crate) fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }
}

impl From<aksh_gha_parser::ParserError> for ApiError {
    fn from(value: aksh_gha_parser::ParserError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_gha_protocol::ProtocolError> for ApiError {
    fn from(value: aksh_gha_protocol::ProtocolError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_gha_protocol::crypto::CryptoError> for ApiError {
    fn from(value: aksh_gha_protocol::crypto::CryptoError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_cache::CacheError> for ApiError {
    fn from(value: aksh_cache::CacheError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_artifacts::ArtifactError> for ApiError {
    fn from(value: aksh_artifacts::ArtifactError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(value: std::io::Error) -> Self {
        Self::internal(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
