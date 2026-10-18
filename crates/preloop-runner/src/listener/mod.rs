//! Listener — polls for jobs and dispatches them to worker processes.

pub mod broker_listener;
pub mod job_dispatcher;
pub mod message_listener;
pub mod oauth;

use anyhow::{Context, Result};
use tracing::info;

use crate::cli::{GlobalArgs, ProtocolPath, RunArgs};
use crate::client::http::HttpClient;
use crate::control_bridge;
use crate::settings::RunnerConfig;

/// Run the listener (main loop of `preloop-runner run`).
pub async fn run_listener(mut args: RunArgs, global: &GlobalArgs) -> Result<()> {
    let root = global.runner_root();
    let jit_payload = args.take_jit_payload();
    let jit_mode = jit_payload.is_some();
    let config = if let Some(payload) = jit_payload {
        // GitHub has already created and registered this runner.  Materialize
        // only the three configuration files; do not call any configure API.
        let config = crate::jitconfig::materialize(payload.expose(), &root)
            .context("decoding and materializing JIT runner configuration")?;
        // JIT configs are single-use by contract.  The payload normally has
        // Ephemeral=true, but forcing --once also protects us from a malformed
        // or older payload that omits that field.
        args.once = true;
        config
    } else {
        RunnerConfig::load(&root)
            .context("loading runner configuration — run `preloop-runner configure` first")?
    };

    info!(
        "Starting runner '{}' (agent {}, pool {})",
        config.settings.agent_name, config.settings.agent_id, config.settings.pool_id
    );

    // Held for the lifetime of the listener so job subprocesses — `git` inside
    // `actions/checkout`, Node actions, `curl` in a step — can reach the
    // control plane at the origin it advertises.
    let _control_bridge = control_bridge::spawn_from_env().await;

    let http = match HttpClient::new(global.ca_bundle.as_deref()) {
        Ok(http) => http,
        Err(error) => {
            if jit_mode {
                let _ = RunnerConfig::remove_files(&root);
            }
            return Err(error);
        }
    };

    // A JIT root has by definition never run `configure`, which is what
    // downloads the Node runtimes JavaScript actions need — the official
    // runner ships them inside its distribution instead. Fetch them before
    // the session exists so no job can be claimed without a runtime; a host
    // that already has them, or that cannot reach nodejs.org, keeps working.
    if jit_mode && let Err(error) = crate::configure::ensure_node_externals(&http, &root).await {
        tracing::warn!(
            "provisioning Node externals for this JIT runner failed: {error:#}; \
             JavaScript actions will need a matching system Node"
        );
    }

    // Get OAuth token and create the listener session. Keep this inside a
    // result so JIT materialization is cleaned up even when token exchange or
    // session creation fails.
    let result = async {
        let (token, expires_at) = oauth::get_oauth_token(&http, &config).await?;

        // Current JIT payloads include ServerUrlV2 for the broker flow. Older
        // payloads predate V2 and must use the distributed-task listener instead.
        // A configured runner still honors the explicit --via selection.
        let via = if jit_mode
            && (!config.settings.use_v2_flow || config.settings.server_url_v2.is_none())
        {
            ProtocolPath::Azdo
        } else {
            args.via
        };

        // Create session and start listening
        match via {
            ProtocolPath::Broker => {
                broker_listener::run_broker_loop(
                    &http, &config, &token, expires_at, args.once, &root,
                )
                .await
            }
            ProtocolPath::Azdo => {
                message_listener::run_message_loop(&http, &config, &token, args.once, &root).await
            }
        }
    }
    .await;

    if jit_mode && let Err(error) = RunnerConfig::remove_files(&root) {
        if result.is_ok() {
            return Err(error).context("cleaning up JIT runner configuration");
        }
        tracing::warn!("failed to clean up JIT runner configuration: {error:#}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use std::path::Path;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end;
        loop {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "mock server received an empty request");
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                header_end = end + 4;
                break;
            }
        }
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .or_else(|| {
                headers
                    .lines()
                    .find_map(|line| line.strip_prefix("Content-Length:"))
            })
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "mock server request body was truncated");
            bytes.extend_from_slice(&chunk[..count]);
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }

    async fn respond(socket: &mut tokio::net::TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }

    /// Give the root valid `externals/node20` and `externals/node24` entries.
    ///
    /// The JIT launch provisions Node for a fresh root; a test that seeded
    /// none would try to download from nodejs.org.
    fn seed_node_externals(root: &Path) {
        for (runtime, version) in [
            ("node20", crate::NODE20_EXTERNALS_VERSION),
            ("node24", crate::NODE24_EXTERNALS_VERSION),
        ] {
            let version = version.trim().trim_start_matches('v');
            let dir = root.join("externals").join(runtime);
            std::fs::create_dir_all(dir.join("bin")).unwrap();
            let node = if cfg!(windows) {
                dir.join("node.exe")
            } else {
                dir.join("bin").join("node")
            };
            std::fs::write(&node, format!("#!/bin/sh\necho v{version}\n")).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&node, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            crate::node_externals::write_manifest(
                &dir,
                &crate::node_externals::NodeManifest::new(
                    runtime,
                    version,
                    &crate::node_externals::current_platform(),
                    "test",
                    "test",
                ),
            )
            .unwrap();
            assert!(
                crate::node_externals::is_valid_externals_dir(&dir, runtime, version),
                "seeded externals for {runtime} must validate"
            );
        }
    }

    fn encoded_jit_config(oauth_url: &str, broker_url: &str) -> String {
        let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
        let runner = serde_json::json!({
            "AgentId": "7",
            "AgentName": "jit-session-test",
            "PoolId": "1",
            "PoolName": null,
            "ServerUrl": broker_url,
            "WorkFolder": "_work",
            "Ephemeral": "True",
            "UseV2Flow": "true",
            "ServerUrlV2": broker_url,
        });
        let credentials = serde_json::json!({
            "Scheme": "OAuth",
            "Data": {
                "ClientId": "jit-client",
                "AuthorizationUrl": oauth_url,
            }
        });
        // The service serializes the RSA fields in .NET's web-default casing;
        // `InverseQ` is the one field that is not a plain lowercasing.
        let rsa_params: serde_json::Map<String, serde_json::Value> =
            serde_json::to_value(keypair.to_rsaparams())
                .unwrap()
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| {
                    let camel = if key == "InverseQ" {
                        "inverseQ".to_string()
                    } else {
                        key.to_ascii_lowercase()
                    };
                    (camel, value.clone())
                })
                .collect();
        let files = serde_json::json!({
            ".runner": STANDARD.encode(serde_json::to_vec(&runner).unwrap()),
            ".credentials": STANDARD.encode(serde_json::to_vec(&credentials).unwrap()),
            ".credentials_rsaparams": STANDARD.encode(serde_json::to_vec(&rsa_params).unwrap()),
        });
        STANDARD.encode(serde_json::to_vec(&files).unwrap())
    }

    #[tokio::test]
    async fn jit_run_materializes_and_creates_broker_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");
        let jitconfig =
            encoded_jit_config(&format!("{base_url}/oauth"), &format!("{base_url}/broker"));

        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..8 {
                let Ok(Ok((mut socket, _))) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept()).await
                else {
                    break;
                };
                let request = read_request(&mut socket).await;
                let request_line = request.lines().next().unwrap_or_default().to_string();
                requests.push(request.clone());
                if request_line.starts_with("POST /oauth ") {
                    respond(
                        &mut socket,
                        "200 OK",
                        r#"{"access_token":"jit-token","expires_in":3600}"#,
                    )
                    .await;
                } else if request_line.starts_with("POST /broker/session ") {
                    respond(
                        &mut socket,
                        "200 OK",
                        r#"{"sessionId":"jit-session","useFipsEncryption":false}"#,
                    )
                    .await;
                } else if request_line.starts_with("GET /broker/message?") {
                    respond(
                        &mut socket,
                        "200 OK",
                        r#"{"messageId":1,"messageType":"RunnerShutdown","body":{"reason":"test"}}"#,
                    )
                    .await;
                } else if request_line.starts_with("DELETE /broker/session ") {
                    respond(&mut socket, "204 No Content", "").await;
                    break;
                } else {
                    respond(
                        &mut socket,
                        "404 Not Found",
                        r#"{"error":"unexpected request"}"#,
                    )
                    .await;
                }
            }
            requests
        });

        let root = tempfile::TempDir::new().unwrap();
        // JIT mode provisions Node externals for the fresh root; seed valid
        // entries so the test does not depend on nodejs.org.
        seed_node_externals(root.path());
        let args = RunArgs {
            once: false,
            via: ProtocolPath::Broker,
            jitconfig: Some(jitconfig),
        };
        let global = GlobalArgs {
            ca_bundle: None,
            runner_root: Some(root.path().to_path_buf()),
        };
        let result =
            tokio::time::timeout(Duration::from_secs(10), run_listener(args, &global)).await;
        assert!(result.is_ok(), "JIT listener timed out");
        result.unwrap().unwrap();

        let requests = server.await.unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("POST /oauth ")),
            "JIT OAuth exchange was not sent"
        );
        let session = requests
            .iter()
            .find(|request| request.starts_with("POST /broker/session "))
            .expect("JIT broker session was not created");
        assert!(
            session.contains(r#""id":7"#),
            "session omitted agent id: {session}"
        );
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("GET /broker/message?")),
            "JIT broker message poll was not sent"
        );
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("DELETE /broker/session ")),
            "JIT broker session was not cleaned up"
        );
        // Ephemeral JIT credentials do not remain on disk after the listener
        // exits, matching the official runner's DeleteLocalRunnerConfig path.
        assert!(!root.path().join(".runner").exists());
        assert!(!root.path().join(".credentials").exists());
        assert!(!root.path().join(".credentials_rsaparams").exists());
    }
}
