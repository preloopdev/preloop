//! Per-test PostgreSQL databases on a shared server.
//!
//! `PRELOOP_TEST_POSTGRES_URL` names a server (the smolvm Postgres locally,
//! an in-job cluster in CI). Each test creates its own database there and
//! drops it when the guard is dropped, so Postgres tests run in parallel
//! without seeing each other's rows.

/// Environment variable naming the shared test server's admin URL.
pub const TEST_POSTGRES_URL_ENV: &str = "PRELOOP_TEST_POSTGRES_URL";

/// Drops its database on drop.
pub struct TestDatabase {
    admin_url: String,
    name: String,
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // `Drop` is sync and usually runs inside a tokio runtime: drop the
        // database from a plain thread with its own runtime.
        let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
        let _ = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    if let Ok((client, conn)) =
                        tokio_postgres::connect(&admin_url, tokio_postgres::NoTls).await
                    {
                        tokio::spawn(conn);
                        let _ = client
                            .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                            .await;
                    }
                })
        })
        .join();
    }
}

/// A fresh database on the shared server and its URL, or `None` when
/// `PRELOOP_TEST_POSTGRES_URL` is unset.
pub async fn fresh_database() -> Option<(TestDatabase, String)> {
    let admin_url = std::env::var(TEST_POSTGRES_URL_ENV)
        .ok()
        .filter(|url| !url.trim().is_empty())?;
    let name = format!("preloop_t_{}", uuid::Uuid::new_v4().simple());
    let (client, conn) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|error| panic!("{TEST_POSTGRES_URL_ENV} set but unreachable: {error}"));
    tokio::spawn(conn);
    client
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .expect("create per-test database");
    let (server, _) = admin_url
        .rsplit_once('/')
        .unwrap_or_else(|| panic!("{TEST_POSTGRES_URL_ENV} must name a database"));
    let url = format!("{server}/{name}");
    Some((TestDatabase { admin_url, name }, url))
}
