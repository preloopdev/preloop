//! `preloop webhooks` — inspect, replay and health-check the durable webhook
//! queue.
//!
//! GitHub keeps three days of delivery history and never resends on its own.
//! preloop keeps the delivery payload for thirty days, so long after GitHub
//! can no longer help, `preloop webhooks replay` still can — from the local
//! copy, with no App JWT and no GitHub round trip.

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct WebhooksArgs {
    #[command(subcommand)]
    pub(crate) command: WebhooksCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum WebhooksCommand {
    /// List durable webhook deliveries, newest first.
    List(ListArgs),

    /// Requeue a delivery from its retained local payload.
    ///
    /// Works on dead-lettered and already-processed deliveries. Processing
    /// is idempotent: a replayed delivery reuses its run and patches the
    /// existing check runs instead of creating duplicates.
    Replay(ReplayArgs),

    /// Show queue depth, watchdog freshness, breaker state and App config
    /// drift in one read.
    Health(HealthArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ListArgs {
    /// Only deliveries in this state: received, processing, done, failed.
    #[arg(long, value_name = "STATE")]
    state: Option<String>,

    /// Maximum rows to print.
    #[arg(long, default_value_t = 50)]
    limit: usize,

    /// Print the raw JSON document.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ReplayArgs {
    /// Delivery id (the `X-GitHub-Delivery` GUID).
    delivery_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct HealthArgs {
    /// Print the raw JSON document.
    #[arg(long)]
    json: bool,
}

pub(crate) async fn run(args: WebhooksArgs) -> anyhow::Result<()> {
    match args.command {
        WebhooksCommand::List(args) => list(args).await,
        WebhooksCommand::Replay(args) => replay(args).await,
        WebhooksCommand::Health(args) => health(args).await,
    }
}

/// Every call shares the CLI's one client, base URL and native bearer.
async fn get(path: &str) -> anyhow::Result<serde_json::Value> {
    request(reqwest::Method::GET, path).await
}

async fn request(method: reqwest::Method, path: &str) -> anyhow::Result<serde_json::Value> {
    let base = crate::server_url();
    let url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let mut builder = crate::build_client().request(method, &url);
    if let Some(token) = crate::api_token() {
        builder = builder.bearer_auth(token);
    }
    let response = builder.send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|error| error.as_str())
                    .map(str::to_owned)
            })
            .unwrap_or(body);
        anyhow::bail!("server returned {status}: {detail}");
    }
    if body.trim().is_empty() {
        Ok(serde_json::Value::Null)
    } else {
        Ok(serde_json::from_str(&body)?)
    }
}

fn human_age(seconds: f64) -> String {
    if seconds < 90.0 {
        format!("{seconds:.0}s")
    } else if seconds < 5400.0 {
        format!("{:.0}m", seconds / 60.0)
    } else if seconds < 172_800.0 {
        format!("{:.1}h", seconds / 3600.0)
    } else {
        format!("{:.1}d", seconds / 86_400.0)
    }
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let head: String = text.chars().take(width.saturating_sub(1)).collect();
    format!("{head}…")
}

async fn list(args: ListArgs) -> anyhow::Result<()> {
    let mut path = format!("/api/v1/webhooks/deliveries?limit={}", args.limit);
    if let Some(state) = &args.state {
        path.push_str(&format!("&state={state}"));
    }
    let document = get(&path).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&document)?);
        return Ok(());
    }
    let stats = &document["stats"];
    println!(
        "queue: {} received, {} processing, {} done, {} dead-letter",
        stats["received"], stats["processing"], stats["done"], stats["dead_letters"]
    );
    let empty = Vec::new();
    let deliveries = document["deliveries"].as_array().unwrap_or(&empty);
    if deliveries.is_empty() {
        println!("no deliveries retained");
        return Ok(());
    }
    println!(
        "{:<38} {:<16} {:<11} {:>4} {:>7}  LAST ERROR",
        "DELIVERY", "EVENT", "STATE", "TRY", "AGE"
    );
    for delivery in deliveries {
        println!(
            "{:<38} {:<16} {:<11} {:>4} {:>7}  {}",
            truncate(delivery["delivery_id"].as_str().unwrap_or("?"), 38),
            truncate(delivery["event"].as_str().unwrap_or("?"), 16),
            delivery["state"].as_str().unwrap_or("?"),
            delivery["attempts"].as_u64().unwrap_or(0),
            human_age(delivery["age_seconds"].as_f64().unwrap_or_default()),
            truncate(delivery["last_error"].as_str().unwrap_or(""), 60),
        );
    }
    Ok(())
}

async fn replay(args: ReplayArgs) -> anyhow::Result<()> {
    let path = format!("/api/v1/webhooks/deliveries/{}/replay", args.delivery_id);
    let document = request(reqwest::Method::POST, &path).await?;
    println!(
        "delivery {} {}",
        document["delivery_id"]
            .as_str()
            .unwrap_or(&args.delivery_id),
        document["status"].as_str().unwrap_or("requeued")
    );
    Ok(())
}

async fn health(args: HealthArgs) -> anyhow::Result<()> {
    let document = get("/api/v1/webhooks/health").await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&document)?);
        return Ok(());
    }

    let queue = &document["queue"];
    println!(
        "queue      {} received, {} processing, {} dead-letter",
        queue["received"], queue["processing"], queue["dead_letters"]
    );
    match queue["oldest_pending_age_seconds"].as_f64() {
        Some(age) => println!("oldest     {} unprocessed", human_age(age)),
        None => println!("oldest     nothing pending"),
    }

    let watchdog = &document["watchdog"];
    if watchdog["enabled"].as_bool().unwrap_or(false) {
        match watchdog["last_success_age_seconds"].as_f64() {
            // A watchdog that stopped polling looks exactly like a period
            // with no failed deliveries, so say it loudly.
            Some(age) if age > 1800.0 => println!(
                "watchdog   STALE — last successful poll {} ago{}",
                human_age(age),
                watchdog["last_error"]
                    .as_str()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            ),
            Some(age) => println!(
                "watchdog   ok — last poll {} ago, {} examined, {} redeliveries requested",
                human_age(age),
                watchdog["last_examined"],
                watchdog["redeliveries_requested"]
            ),
            None => println!(
                "watchdog   NEVER SUCCEEDED{}",
                watchdog["last_error"]
                    .as_str()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            ),
        }
        let open = watchdog["open_repairs"].as_u64().unwrap_or(0);
        if open > 0 {
            println!("repairs    {open} redelivered deliveries have not arrived yet");
        }
    } else {
        println!("watchdog   disabled (no GitHub App configured, or turned off)");
    }

    let breaker = &document["github_breaker"];
    if breaker["open"].as_bool().unwrap_or(false) {
        println!(
            "github     UNAVAILABLE — retrying in {}s{}",
            breaker["retry_in_seconds"].as_u64().unwrap_or(0),
            if breaker["rate_limited"].as_bool().unwrap_or(false) {
                " (rate limited)"
            } else {
                ""
            }
        );
        if let Some(error) = breaker["last_error"].as_str() {
            println!("           last error: {error}");
        }
    } else {
        println!("github     reachable");
    }

    let reconciler = &document["reconciler"];
    if reconciler["enabled"].as_bool().unwrap_or(false) {
        println!(
            "reconciler {} repositories scanned, {} events synthesized",
            reconciler["repositories_scanned"], reconciler["synthesized"]
        );
        if let Some(error) = reconciler["last_error"].as_str() {
            println!("           last error: {error}");
        }
    }

    let empty = Vec::new();
    let apps = document["apps"].as_array().unwrap_or(&empty);
    if apps.is_empty() {
        println!("apps       no App configuration read yet");
    }
    for app in apps {
        let id = app["app_id"].as_str().unwrap_or("?");
        if app["healthy"].as_bool().unwrap_or(false) {
            println!("app {id:<7} webhook configuration ok");
            continue;
        }
        println!("app {id:<7} NEEDS ATTENTION");
        if app["url_drifted"].as_bool().unwrap_or(false) {
            println!(
                "           delivery URL {} != expected {}",
                app["hook_url"].as_str().unwrap_or("<none>"),
                app["expected_url"].as_str().unwrap_or("<unknown>")
            );
        }
        if let Some(events) = app["missing_events"].as_array().filter(|e| !e.is_empty()) {
            let names: Vec<&str> = events.iter().filter_map(|e| e.as_str()).collect();
            println!(
                "           not subscribed to: {} (fix in App settings; GitHub has no API)",
                names.join(", ")
            );
        }
        if let Some(permissions) = app["missing_permissions"]
            .as_array()
            .filter(|p| !p.is_empty())
        {
            let names: Vec<&str> = permissions.iter().filter_map(|p| p.as_str()).collect();
            println!("           missing permissions: {}", names.join(", "));
        }
        if let Some(error) = app["error"].as_str() {
            println!("           {error}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_render_at_a_useful_resolution() {
        assert_eq!(human_age(12.0), "12s");
        assert_eq!(human_age(600.0), "10m");
        assert_eq!(human_age(7200.0), "2.0h");
        assert_eq!(human_age(432_000.0), "5.0d");
    }

    #[test]
    fn truncation_keeps_the_column_width() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("0123456789", 5).chars().count(), 5);
    }
}
