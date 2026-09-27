//! A TCP proxy between engine nodes and Postgres that injects real network
//! faults on a schedule: added latency, a frozen link (partition), and a
//! reset of every connection (what a failover or a pooler restart does).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::metrics::Metrics;

/// One scheduled fault: `kind@start_secs+duration_secs[:param]`, e.g.
/// `latency@60+30:50` (50 ms each way), `partition@120+15`, `reset@200+0`.
#[derive(Clone, Debug)]
pub struct Fault {
    pub kind: String,
    pub at: Duration,
    pub duration: Duration,
    pub param: u64,
}

impl std::str::FromStr for Fault {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        let (kind, rest) = s.split_once('@').ok_or_else(|| anyhow::anyhow!("fault: {s}"))?;
        let (window, param) = rest.split_once(':').unwrap_or((rest, "0"));
        let (at, duration) = window
            .split_once('+')
            .ok_or_else(|| anyhow::anyhow!("fault window: {s}"))?;
        Ok(Self {
            kind: kind.to_owned(),
            at: Duration::from_secs(at.parse()?),
            duration: Duration::from_secs(duration.parse()?),
            param: param.parse()?,
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Healthy,
    Latency(u64),
    Partition,
}

pub async fn run_proxy(
    listen: String,
    upstream: String,
    faults: Vec<Fault>,
    metrics: Arc<Metrics>,
    started: Instant,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&listen).await?;
    let (mode_tx, mode_rx) = watch::channel(Mode::Healthy);
    // Bumped on a reset fault; connections from an older epoch close.
    let epoch = Arc::new(AtomicU64::new(0));
    let (epoch_tx, epoch_rx) = watch::channel(0u64);

    // Fault scheduler.
    {
        let metrics = metrics.clone();
        let epoch = epoch.clone();
        tokio::spawn(async move {
            let mut ordered = faults;
            ordered.sort_by_key(|f| f.at);
            for fault in ordered {
                let wait = fault.at.saturating_sub(started.elapsed());
                tokio::time::sleep(wait).await;
                eprintln!("[chaos] {} for {:?} (param {})", fault.kind, fault.duration, fault.param);
                metrics.incr(&format!("chaos.{}", fault.kind));
                match fault.kind.as_str() {
                    "reset" => {
                        let next = epoch.fetch_add(1, Ordering::SeqCst) + 1;
                        let _ = epoch_tx.send(next);
                    }
                    "latency" => {
                        let _ = mode_tx.send(Mode::Latency(fault.param));
                        tokio::time::sleep(fault.duration).await;
                        let _ = mode_tx.send(Mode::Healthy);
                    }
                    "partition" => {
                        let _ = mode_tx.send(Mode::Partition);
                        tokio::time::sleep(fault.duration).await;
                        let _ = mode_tx.send(Mode::Healthy);
                    }
                    other => eprintln!("[chaos] unknown fault {other}"),
                }
                eprintln!("[chaos] {} over", fault.kind);
            }
        });
    }

    loop {
        let (client, _) = listener.accept().await?;
        let upstream = upstream.clone();
        let mode = mode_rx.clone();
        let born = epoch.load(Ordering::SeqCst);
        let epochs = epoch_rx.clone();
        tokio::spawn(async move {
            let Ok(server) = TcpStream::connect(&upstream).await else {
                return;
            };
            let _ = client.set_nodelay(true);
            let _ = server.set_nodelay(true);
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            tokio::select! {
                _ = pump(cr, sw, mode.clone()) => {}
                _ = pump(sr, cw, mode) => {}
                _ = wait_reset(epochs, born) => {}
            }
        });
    }
}

async fn wait_reset(mut epochs: watch::Receiver<u64>, born: u64) {
    while epochs.changed().await.is_ok() {
        if *epochs.borrow() > born {
            return;
        }
    }
    std::future::pending::<()>().await;
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut mode: watch::Receiver<Mode>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        loop {
            let current = *mode.borrow();
            match current {
                Mode::Healthy => break,
                Mode::Latency(ms) => {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    break;
                }
                // Hold bytes until the partition heals (a silent network).
                Mode::Partition => {
                    if mode.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}
