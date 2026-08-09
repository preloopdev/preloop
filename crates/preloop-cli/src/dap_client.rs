//! Small interactive DAP client for `preloop dap`.

use anyhow::{Context, Result};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::Write;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

#[derive(Debug, Args)]
pub struct DapArgs {
    /// Run id whose DAP endpoint should be opened.
    pub run_id: String,

    /// Native engine URL. Defaults to PRELOOP_URL.
    #[arg(long)]
    pub url: Option<String>,

    /// Native API token. Defaults to PRELOOP_TOKEN/PRELOOP_SYSTEM_TOKEN.
    #[arg(long)]
    pub token: Option<String>,
}

pub async fn run(args: DapArgs, base_url: String, token: Option<String>) -> Result<()> {
    let base_url = args.url.unwrap_or(base_url);
    let ws_url = format!(
        "{}/api/v1/runs/{}/debug",
        base_url.trim_end_matches('/'),
        args.run_id
    )
    .replace("http://", "ws://")
    .replace("https://", "wss://");
    let token = args.token.or(token);
    let mut request = ws_url
        .into_client_request()
        .context("building DAP WebSocket request")?;
    if let Some(token) = token {
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {token}")
                .parse()
                .context("invalid API token")?,
        );
    }
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("connecting to preloop DAP endpoint")?;
    println!("Connected. Commands: init, ready, wait, source, scopes, vars REF, eval EXPR, continue, quit");

    let mut seq = 0_i64;
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = tokio::io::AsyncBufReadExt::lines(stdin);
    loop {
        print!("dap> ");
        std::io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else {
            break;
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "quit" || line == "exit" {
            break;
        }
        if line == "wait" {
            let value = next_event(&mut socket).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
            continue;
        }
        let (command, arguments) = parse_command(line)?;
        seq += 1;
        socket
            .send(Message::Text(
                json!({
                    "seq": seq,
                    "type": "request",
                    "command": command,
                    "arguments": arguments,
                })
                .to_string(),
            ))
            .await
            .context("sending DAP request")?;
        let response = response_for(&mut socket, seq).await?;
        println!("{}", serde_json::to_string_pretty(&response)?);
    }
    let _ = socket.send(Message::Close(None)).await;
    Ok(())
}

fn parse_command(line: &str) -> Result<(&str, Value)> {
    let mut parts = line.splitn(2, char::is_whitespace);
    let command = parts.next().unwrap_or(line);
    let rest = parts.next().unwrap_or("").trim();
    let args = match command {
        "init" => json!({"adapterID":"preloop","clientID":"preloop"}),
        "ready" => json!({}),
        "source" => json!({"source":{"name":"execution.yml"}}),
        "threads" => json!({}),
        "scopes" => json!({"frameId":1}),
        "vars" => {
            json!({"variablesReference": rest.parse::<i64>().context("vars requires a numeric reference")?})
        }
        "eval" => json!({"expression":rest,"context":"repl"}),
        "continue" => json!({"threadId":1}),
        "help" => {
            println!("init ready wait source scopes vars REF eval EXPR continue quit");
            return Ok(("threads", json!({})));
        }
        other => anyhow::bail!("unknown DAP command `{other}`"),
    };
    let command = match command {
        "init" => "initialize",
        "ready" => "configurationDone",
        "vars" => "variables",
        "eval" => "evaluate",
        other => other,
    };
    Ok((command, args))
}

async fn response_for<S>(socket: &mut S, request_seq: i64) -> Result<Value>
where
    S: futures_util::Sink<Message>
        + futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    while let Some(message) = socket.next().await {
        let Message::Text(text) = message.context("reading DAP response")? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).context("invalid DAP JSON")?;
        if value.get("type").and_then(Value::as_str) == Some("event") {
            println!("event: {}", serde_json::to_string_pretty(&value)?);
            continue;
        }
        if value.get("request_seq").and_then(Value::as_i64) == Some(request_seq) {
            return Ok(value);
        }
    }
    anyhow::bail!("DAP connection closed while waiting for response")
}

async fn next_event<S>(socket: &mut S) -> Result<Value>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(message) = socket.next().await {
        let Message::Text(text) = message.context("reading DAP event")? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).context("invalid DAP JSON")?;
        if value.get("type").and_then(Value::as_str) == Some("event") {
            return Ok(value);
        }
    }
    anyhow::bail!("DAP connection closed while waiting for event")
}
