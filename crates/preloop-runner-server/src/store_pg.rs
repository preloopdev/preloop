//! Postgres connection helpers shared by the control backend and the
//! webhook watchdog's health probe: TLS connector construction, the
//! libpq→tokio-postgres `sslmode` rewrite, the connect deadline, and the
//! session timeouts every engine session carries. The `PgStore` snapshot
//! backend is gone — `control/pg` owns all scheduling state now.
use postgres_rustls::MakeTlsConnector;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// Deadline for a Postgres session to become usable: resolution, the TCP
/// connect, the TLS handshake, authentication, and the session-setup
/// statements.
///
/// `tokio-postgres`' own `connect_timeout` bounds only the TCP connect; the
/// startup packet, TLS handshake and authentication that follow it are not
/// covered by it, so a database that accepts TCP and then never answers — a
/// hung primary, a stopped server behind a live load balancer, a stalled
/// trust daemon during TLS — parks the caller forever. That is the shape of
/// the incident this deadline exists for: engine startup, every pooled
/// reconnect, and the wake listener waited on it with no bound at all. 10 s
/// is far beyond a healthy connect (milliseconds on a LAN, tens with TLS)
/// yet short enough that a stalled database fails while someone is watching,
/// and matches the deadline the OS credential store already uses.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Server-side guard: the longest one control statement may run.
///
/// Every control command is one short transaction of targeted statements
/// (`control/pg/mod.rs`): a conditional `UPDATE`, a `FOR UPDATE SKIP LOCKED`
/// queue claim, a bounded batch verdict from the reaper, a statistics count.
/// Real durations are milliseconds; 30 s is ~1000x the slowest of them and
/// equal to the pool's `CHECKOUT_TIMEOUT`, so a statement that reaches it
/// means the database is stuck rather than busy. Postgres then cancels the
/// statement and the command fails with an error that names the timeout,
/// instead of holding a runtime worker, a pooled connection and its row
/// locks forever. Admin and migration work does NOT carry this guard: it
/// runs on `store_admin`'s own connection, and the one migration a serving
/// process can run (test-support schema creation) lifts the guards for its
/// batch — see `control::pg::create_control_schema`.
pub const STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Server-side guard: the longest a control statement may wait for a lock.
///
/// The incident this family of deadlines exists for began with Postgres
/// killing sessions that sat idle inside a transaction: a stuck transaction
/// keeps its row locks, and every command that needs the same run row queues
/// behind it. `statement_timeout` alone would report that as a slow
/// statement; 15 s — half of it, so a lock wait is reported as a lock wait —
/// names the actual cause (`canceling statement due to lock timeout`) while
/// still being ~1000x the row-lock waits `control::txn_stats` records. It
/// also covers `pg_advisory_lock` waits, which is why the setup advisory
/// lock is only taken on connections that lifted these guards.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(15);

/// Server-side guard: how long a session may sit idle inside an open
/// transaction.
///
/// The incident began with Postgres killing idle-in-transaction sessions.
/// Setting the value here makes that behaviour deterministic instead of
/// whatever the server happens to be configured with: 60 s is far beyond the
/// gap between two statements of a control command (no transaction in
/// `control/pg` is held across an await of anything but its next statement),
/// so a healthy command never sees it, while a leaked transaction can no
/// longer hold a run's row locks indefinitely — it is killed with a named
/// error that says which session timed out.
pub const IDLE_IN_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// The `SET`s that put `statement_timeout`, `lock_timeout` and
/// `idle_in_transaction_session_timeout` on a session, for `batch_execute`
/// at session start. Milliseconds: Postgres takes these GUCs as milliseconds
/// or a unit-qualified string.
pub fn session_timeouts_sql() -> String {
    session_timeouts_sql_with(STATEMENT_TIMEOUT, LOCK_TIMEOUT, IDLE_IN_TRANSACTION_TIMEOUT)
}

/// [`session_timeouts_sql`] with the durations given, for a caller that must
/// not be bound by the serving values — a migration batch, or a test that
/// cannot wait the serving deadline. The durations are parameters so the SQL
/// cannot drift from the documented values.
pub fn session_timeouts_sql_with(
    statement: Duration,
    lock: Duration,
    idle_in_transaction: Duration,
) -> String {
    format!(
        "SET statement_timeout = {}; SET lock_timeout = {}; \
         SET idle_in_transaction_session_timeout = {}",
        statement.as_millis(),
        lock.as_millis(),
        idle_in_transaction.as_millis()
    )
}

/// The driver config for `url`: the libpq→tokio-postgres `sslmode` rewrite,
/// the TCP connect bounded by [`CONNECT_TIMEOUT`], and TCP keepalives.
///
/// A `connect_timeout` in the URL is overridden, so no connection string can
/// reinstate an unbounded connect. Note that this setting covers only the
/// TCP connect of each resolved address: the handshake after it needs the
/// caller's own deadline (see [`open_session`]). Keepalives make a peer that
/// vanished without a FIN (host reset, network dropped) fail its statements
/// at the kernel's probe interval instead of hanging on a socket nothing
/// will ever answer.
pub fn config(url: &str) -> anyhow::Result<tokio_postgres::Config> {
    let mut config: tokio_postgres::Config = connect_url(url)
        .parse()
        .map_err(|error| anyhow::anyhow!("parsing the Postgres connection string: {error}"))?;
    config.connect_timeout(CONNECT_TIMEOUT);
    config.keepalives(true);
    config.keepalives_idle(Duration::from_secs(30));
    config.keepalives_interval(Duration::from_secs(10));
    config.keepalives_retries(3);
    Ok(config)
}

/// The error for a Postgres session that did not open within `deadline`:
/// names the dependency and the deadline, and says what the stall looks like,
/// so an operator can tell a hung database from a rejected credential or a
/// slow query.
pub fn connect_deadline_error(deadline: Duration) -> anyhow::Error {
    anyhow::anyhow!(
        "Postgres did not open a session within {}s (connect deadline): resolution, the TCP \
         connect, the TLS handshake, authentication or the session-setup statements never \
         finished. A database that accepts connections and then stops answering looks like \
         this — treat it as hung, not as a rejected credential or a slow query",
        deadline.as_secs_f32()
    )
}

/// A connected session's driver connection.
///
/// `tokio-postgres` makes no progress — the startup handshake, a statement, a
/// `LISTEN` — unless this is polled, which is why [`open_session`] hands it
/// to its caller before running the session's `SET`s. Command connections
/// spawn [`Driver::run`]; a connection that carries the traffic itself (the
/// wake listener's notifications) polls [`Driver::poll_message`].
///
/// Boxed: one `Connection` is ~1.3 kB, and this enum travels through the
/// spawned driver tasks.
pub(crate) enum Driver {
    /// TLS, per the URL's `sslmode` (see [`tls_connector`]).
    Tls(
        Box<
            tokio_postgres::Connection<
                tokio_postgres::Socket,
                postgres_rustls::TlsStream<tokio_postgres::Socket>,
            >,
        >,
    ),
    /// No TLS (`sslmode=disable`, or no `sslmode`).
    Plain(
        Box<tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>>,
    ),
}

impl Driver {
    /// Poll for the next asynchronous message (a notification, a notice) or
    /// the connection's close: `None` and `Some(Err(_))` are terminal.
    pub(crate) fn poll_message(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<tokio_postgres::AsyncMessage, tokio_postgres::Error>>> {
        match self {
            Self::Tls(connection) => connection.as_mut().poll_message(cx),
            Self::Plain(connection) => connection.as_mut().poll_message(cx),
        }
    }

    /// Drive the connection until it closes, logging a drop. What a command
    /// connection needs: its statements are the traffic.
    pub(crate) async fn run(self) {
        let closed = match self {
            Self::Tls(connection) => (*connection).await,
            Self::Plain(connection) => (*connection).await,
        };
        if let Err(error) = closed {
            tracing::warn!(%error, "postgres control connection closed");
        }
    }
}

/// Open a Postgres session on `url` under one `deadline` — resolution, TCP
/// connect, TLS, authentication, and the `setup` batch — and return its
/// client.
///
/// `setup` is a `batch_execute` payload run at session start: the search
/// path, the origin stamp and the session timeouts ([`session_timeouts_sql`])
/// for a control connection; the `LISTEN`s for the wake listener. `drive`
/// receives the driver connection before that batch runs and is spawned, so
/// the connection is polled while the batch is in flight; it must keep
/// polling it for the life of the session.
///
/// Every engine Postgres session opens through here, so a database that
/// accepts TCP and then stops answering fails the caller at the deadline
/// (naming Postgres and the deadline) instead of parking it forever — engine
/// startup, a pooled reconnect and the wake listener included. The deadline
/// is absolute, not per phase: setup runs on what the connect left, so a
/// database that is slow to authenticate *and* then stalls cannot hold the
/// caller for twice the advertised deadline.
pub(crate) async fn open_session<Fut>(
    deadline: Duration,
    url: &str,
    setup: &str,
    drive: impl FnOnce(Driver) -> Fut,
) -> anyhow::Result<tokio_postgres::Client>
where
    Fut: Future<Output = ()> + Send + 'static,
{
    let started = Instant::now();
    let (client, driver) = connect_bounded(deadline, url).await?;
    let driver_task = tokio::spawn(drive(driver));
    // The budget the connect left, not a fresh copy of it: authentication
    // that succeeds just under the deadline must not grant the setup batch
    // another full one.
    let remaining = deadline.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, client.batch_execute(setup)).await {
        Ok(Ok(())) => Ok(client),
        Ok(Err(error)) => {
            driver_task.abort();
            Err(error.into())
        }
        Err(_elapsed) => {
            // Both failure paths abandon the session, and the client alone
            // cannot end the driver: tokio-postgres keeps a connection with
            // an in-flight request alive until its response arrives, so a
            // server that never answers the batch would leave the spawned
            // task and its socket behind on every retry. Abort it.
            driver_task.abort();
            Err(connect_deadline_error(deadline))
        }
    }
}

/// Connect under `deadline` and return the client with its driver.
///
/// `Config::connect_timeout` (set by [`config`]) bounds only the TCP connect;
/// the startup packet, TLS handshake and authentication that follow it are
/// not covered, so the deadline here is what a stalled database fails.
async fn connect_bounded(
    deadline: Duration,
    url: &str,
) -> anyhow::Result<(tokio_postgres::Client, Driver)> {
    let config = config(url)?;
    let (client, driver) = match tls_connector(url)? {
        Some(tls) => {
            let (client, connection) = tokio::time::timeout(deadline, config.connect(tls))
                .await
                .map_err(|_| connect_deadline_error(deadline))?
                .map_err(anyhow::Error::from)?;
            (client, Driver::Tls(Box::new(connection)))
        }
        None => {
            let (client, connection) =
                tokio::time::timeout(deadline, config.connect(tokio_postgres::NoTls))
                    .await
                    .map_err(|_| connect_deadline_error(deadline))?
                    .map_err(anyhow::Error::from)?;
            (client, Driver::Plain(Box::new(connection)))
        }
    };
    Ok((client, driver))
}

/// rustls config for every Postgres connection, built once per process from
/// the shared root set ([`crate::shared_http::tls_roots`]) so connecting
/// never reads the OS trust store on the runtime thread.
static TLS_CONFIG: LazyLock<Result<Arc<rustls::ClientConfig>, String>> =
    LazyLock::new(build_tls_config);

fn build_tls_config() -> Result<Arc<rustls::ClientConfig>, String> {
    // rustls 0.23 needs a process-level crypto provider. The server
    // binaries install one at startup; installing here makes the library
    // self-sufficient (tests, embedded use) without breaking an
    // already-installed provider. The config itself is built with an explicit
    // provider because several are compiled in (ring via this crate's
    // rustls feature, aws-lc-rs via reqwest), which makes the implicit
    // `ClientConfig::builder()` ambiguous.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut roots = rustls::RootCertStore::empty();
    for cert in crate::shared_http::tls_roots() {
        roots
            .add(cert.clone())
            .map_err(|error| format!("adding TLS root cert: {error}"))?;
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| format!("TLS protocol versions: {error}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    postgres_rustls::set_postgresql_alpn(&mut config);
    Ok(Arc::new(config))
}

/// Build a rustls connector when the URL asks for TLS (`sslmode=require`,
/// `verify-ca`, or `verify-full`); `None` for `sslmode=disable` or when no
/// `sslmode` is present. Chain + hostname verification always use the shared
/// in-process root set, so `verify-full` semantics apply to every TLS mode.
pub fn tls_connector(url: &str) -> anyhow::Result<Option<MakeTlsConnector>> {
    let query = url.split('?').nth(1).unwrap_or("");
    let sslmode = query
        .split('&')
        .find_map(|part| part.strip_prefix("sslmode="));
    match sslmode {
        Some("require" | "verify-ca" | "verify-full") => {
            let config = TLS_CONFIG
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(Some(MakeTlsConnector::new(
                tokio_rustls::TlsConnector::from(config.clone()),
            )))
        }
        _ => Ok(None),
    }
}

/// URL to hand to `tokio-postgres::connect`. `sslmode` values `verify-ca` /
/// `verify-full` are libpq extensions that tokio-postgres rejects; the
/// rustls connector always performs full chain + hostname verification, so
/// mapping them onto `require` (TLS stays mandatory) loses nothing.
pub fn connect_url(url: &str) -> String {
    let (before, after) = match url.split_once("sslmode=verify-") {
        Some((before, after)) => (before, after),
        None => return url.to_owned(),
    };
    let (mode, rest) = match after.split_once('&') {
        Some((mode, rest)) => (mode, rest),
        None => (after, ""),
    };
    if !matches!(mode, "ca" | "full") {
        return url.to_owned();
    }
    let rest = if rest.is_empty() {
        ""
    } else {
        &format!("&{rest}")
    };
    format!("{before}sslmode=require{rest}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Postgres stand-in that accepts the connection, spends
    /// `handshake_after` before completing the startup handshake, and then
    /// never answers a statement.
    ///
    /// The delay is what makes a per-phase deadline observable: the connect
    /// phase spends most of the budget and still succeeds, so a setup batch
    /// on a deadline of its own would push the caller past the advertised
    /// one. The returned receiver resolves when the client end of the socket
    /// is dropped, which is how a test sees the driver task die.
    async fn stalling_after_the_startup_handshake(
        handshake_after: Duration,
    ) -> (u16, tokio::sync::oneshot::Receiver<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::time::sleep(handshake_after).await;
            // AuthenticationOk, then ReadyForQuery (idle): the client's
            // connect resolves on the second and sends the setup batch next.
            const HANDSHAKE: &[u8] = b"R\x00\x00\x00\x08\x00\x00\x00\x00Z\x00\x00\x00\x05I";
            if socket.write_all(HANDSHAKE).await.is_err() {
                return;
            }
            // Hold the socket open, so the caller fails at its deadline
            // rather than on a closed connection.
            let mut scratch = [0u8; 4096];
            while matches!(socket.read(&mut scratch).await, Ok(read) if read > 0) {}
            let _ = closed_tx.send(());
        });
        (port, closed_rx)
    }

    /// One deadline covers the connect *and* the setup batch: a database
    /// slow to authenticate and then silent must fail the caller at the
    /// deadline, not at twice it.
    #[tokio::test]
    async fn the_setup_batch_shares_the_connect_deadline() {
        let deadline = Duration::from_millis(600);
        let (port, _closed) =
            stalling_after_the_startup_handshake(Duration::from_millis(400)).await;
        let url = format!("postgres://preloop@127.0.0.1:{port}/control");
        let started = Instant::now();
        let error = open_session(deadline, &url, "SET search_path TO control", |driver| {
            driver.run()
        })
        .await
        .expect_err("a database that never answers the setup batch must not open a session");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(500),
            "the attempt failed in {elapsed:?}, before its {deadline:?} deadline: {error}"
        );
        assert!(
            elapsed < deadline + deadline / 2,
            "the attempt took {elapsed:?}: the setup batch must spend what the connect left \
             of one {deadline:?} deadline, not a fresh one"
        );
        assert!(error.to_string().contains("connect deadline"), "{error}");
    }

    /// An abandoned session takes its driver down with it. `tokio-postgres`
    /// keeps a connection with an in-flight request alive until the response
    /// arrives, so dropping the client is not enough: the task and its
    /// socket would survive every retry of the wake listener. The stand-in
    /// sees the client end of the socket close, which only an aborted driver
    /// produces.
    #[tokio::test]
    async fn an_abandoned_session_aborts_its_driver() {
        let (port, closed) = stalling_after_the_startup_handshake(Duration::from_millis(50)).await;
        let url = format!("postgres://preloop@127.0.0.1:{port}/control");
        open_session(
            Duration::from_millis(300),
            &url,
            "SET search_path TO control",
            |driver| driver.run(),
        )
        .await
        .expect_err("the setup batch is never answered, so the session must not open");
        tokio::time::timeout(Duration::from_secs(2), closed)
            .await
            .expect("the driver task must be aborted with the abandoned session")
            .expect("the stand-in reports the closed socket");
    }

    /// The driver config carries the connect deadline, and a connection
    /// string cannot raise it back: `connect_timeout` in the URL is
    /// overridden, and the `sslmode` rewrite still happens underneath.
    #[test]
    fn the_driver_config_bounds_the_tcp_connect() {
        let config = config(
            "postgres://preloop@db.internal:5432/control\
             ?sslmode=verify-full&connect_timeout=3600",
        )
        .unwrap();
        assert_eq!(config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );
        assert_eq!(config.get_hosts().len(), 1);
    }

    /// The connect deadline error names the dependency and the deadline, so
    /// an operator can tell a hung database from a rejected credential.
    #[test]
    fn the_connect_deadline_error_names_postgres_and_the_deadline() {
        let message = connect_deadline_error(CONNECT_TIMEOUT).to_string();
        for needle in ["Postgres", "connect deadline", "10"] {
            assert!(message.contains(needle), "{message}");
        }
    }

    /// The guard SQL carries the durations in milliseconds, which is what
    /// Postgres reads for these GUCs.
    #[test]
    fn the_session_guard_sql_carries_the_durations() {
        assert_eq!(
            session_timeouts_sql(),
            "SET statement_timeout = 30000; SET lock_timeout = 15000; \
             SET idle_in_transaction_session_timeout = 60000"
        );
        assert_eq!(
            session_timeouts_sql_with(
                Duration::from_millis(250),
                Duration::from_millis(250),
                Duration::from_millis(250)
            ),
            "SET statement_timeout = 250; SET lock_timeout = 250; \
             SET idle_in_transaction_session_timeout = 250"
        );
    }
}
