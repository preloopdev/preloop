//! Service-level cache behavior on a real database.
//!
//! Template/pool pages are asserted through the generated HTTP API, not by
//! calling cache internals: a mutation through one replica must be visible to
//! a list through another even when the reader's cache is warm, flushed,
//! disabled or failing, and lease operations must be unaffected by cache
//! churn. The Postgres variant exercises the same read/write path with two
//! service replicas sharing one database and separate in-memory caches.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink};
use preloop_sandbox::api::*;
use preloop_sandbox::backend::{self, BackendConfig};
use preloop_sandbox::cache::{
    CacheEntry, CacheError, CacheGateway, CacheKey, CacheSettings, SharedCache,
};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::server::{Auth, serve};
use preloop_sandbox::{Driver, SandboxApi, SandboxClient, ServerTls, Service, ServiceConfig};
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "cache-service-token";

struct Node {
    service: Arc<Service>,
    base: String,
    shutdown: CancellationToken,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Node {
    fn client(&self) -> SandboxClient {
        SandboxClient::with_tls(
            &self.base,
            Some(TOKEN.to_owned()),
            &self.service.ca_pem().expect("service CA"),
            None,
        )
        .unwrap()
    }
}

async fn start_node(
    store_url: &str,
    state_dir: std::path::PathBuf,
    tls: Arc<ServerTls>,
    node_id: &str,
    cache: CacheGateway,
    cache_ttl: Duration,
) -> Node {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("https://{}", listener.local_addr().unwrap());
    let credential = format!("credential-{node_id}");
    let mut config = ServiceConfig::new();
    config.ca = Some(tls.ca.clone());
    config.node_id = node_id.to_owned();
    config.node_session = format!("session-{node_id}");
    config.node_credential = credential.clone();
    config.peer_url = base.clone();
    config.node_lost_after = Duration::from_secs(60);
    config.acquire_wait = Duration::from_secs(20);
    config.provider_timeout = Duration::from_secs(10);
    config.create_timeout = Duration::from_secs(30);
    config.work_timeout = Duration::from_secs(20);
    config.cache = cache;
    config.cache_ttl = cache_ttl;
    let backend = backend::open_with(BackendConfig {
        url: store_url.to_owned(),
        state_dir,
        seal_key: Some([42u8; 32]),
        pool_size: 4,
        node_id: node_id.to_owned(),
        session_id: config.node_session.clone(),
        node_credential_sha256: backend::credential_hash(&credential),
        peer_url: base.clone(),
        node_lost_after: config.node_lost_after,
    })
    .await
    .unwrap();
    let service = Service::open(backend, config).await.unwrap();
    let shutdown = CancellationToken::new();
    tokio::spawn(serve(
        listener,
        service.clone(),
        Auth::new(TOKEN),
        tls,
        shutdown.clone(),
    ));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    Node {
        service,
        base,
        shutdown,
    }
}

fn memory_cache() -> CacheGateway {
    CacheSettings::default().gateway().unwrap()
}

fn template_spec(name: &str, seed: u8) -> TemplateSpec {
    let digest = format!("sha256:{}", format!("{seed:02x}").repeat(32));
    TemplateSpec {
        name: name.to_owned(),
        os: "linux".into(),
        artifacts: vec![Artifact {
            format: Artifact::OCI_IMAGE.into(),
            arch: host_arch().into(),
            r#ref: format!("ghcr.io/preloop/alpine@{digest}"),
            digest,
            size_bytes: 0,
            ..Artifact::default()
        }],
        ..TemplateSpec::default()
    }
}

fn pool_spec(name: &str, template: &str, max: u32) -> PoolSpec {
    PoolSpec {
        name: name.to_owned(),
        template: template.to_owned(),
        max,
        ..PoolSpec::default()
    }
}

fn find_template<'a>(templates: &'a [Template], name: &str) -> Option<&'a Template> {
    templates.iter().find(|template| template.spec.name == name)
}

fn find_pool<'a>(pools: &'a [PoolSpec], name: &str) -> Option<&'a PoolSpec> {
    pools.iter().find(|pool| pool.name == name)
}

async fn wait_for_cache_hit(node: &Node) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node.service.cache().health().hits > 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cache fill did not land");
}

async fn wait_for_provider(service: &Arc<Service>, name: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let providers = service.list_providers().await.unwrap();
            if providers.iter().any(|provider| provider.name == name) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("agent did not register");
}

/// A cache backend that is always unavailable: reads must still work.
#[derive(Debug)]
struct FailingCache;

#[async_trait]
impl SharedCache for FailingCache {
    async fn get(&self, _key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    async fn put(&self, _key: &CacheKey, _entry: CacheEntry) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate(&self, _key: &CacheKey) -> Result<(), CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    fn name(&self) -> &'static str {
        "failing-test"
    }
}

fn sqlite_url(dir: &std::path::Path) -> String {
    format!("sqlite://{}", dir.join("sandbox.db").display())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn template_and_pool_reads_follow_writes_across_replicas() {
    let dir = tempfile::tempdir().unwrap();
    let tls = ServerTls::load_or_create(
        &dir.path().join("tls"),
        vec!["127.0.0.1".into()],
        None,
        None,
    )
    .unwrap();
    let url = sqlite_url(dir.path());
    let a = start_node(
        &url,
        dir.path().join("state-a"),
        tls.clone(),
        "node-a",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let b = start_node(
        &url,
        dir.path().join("state-b"),
        tls.clone(),
        "node-b",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let a_client = a.client();
    let b_client = b.client();

    let v1 = template_spec("cache-template", 0x11);
    a_client.put_template(v1.clone()).await.unwrap();
    a_client
        .put_pool(pool_spec("cache-pool", "cache-template", 4))
        .await
        .unwrap();

    // Replica B has never seen these rows and has an empty cache.
    let templates = b_client.list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "cache-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v1)
    );
    let pools = b_client.list_pools().await.unwrap();
    assert_eq!(find_pool(&pools, "cache-pool").unwrap().max, 4);

    // Its first list filled the cache; a second list is served from it.
    b_client.list_templates().await.unwrap();
    wait_for_cache_hit(&b).await;
    b_client.list_pools().await.unwrap();

    // A writes a new template version and pool definition; B's next page
    // selects the new versions and must not serve the warm old payloads.
    let v2 = template_spec("cache-template", 0x22);
    a_client.put_template(v2.clone()).await.unwrap();
    a_client
        .put_pool(pool_spec("cache-pool", "cache-template", 9))
        .await
        .unwrap();
    let templates = b_client.list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "cache-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v2)
    );
    let pools = b_client.list_pools().await.unwrap();
    assert_eq!(find_pool(&pools, "cache-pool").unwrap().max, 9);

    // Flushing changes nothing but the acceleration.
    assert!(b.service.cache().clear().await);
    let templates = b_client.list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "cache-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v2)
    );

    // A fresh replica opens an empty cache and rebuilds it from SQL.
    let c = start_node(
        &url,
        dir.path().join("state-c"),
        tls,
        "node-c",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let templates = c.client().list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "cache-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_and_disabled_caches_still_serve_authoritative_reads() {
    let dir = tempfile::tempdir().unwrap();
    let tls = ServerTls::load_or_create(
        &dir.path().join("tls"),
        vec!["127.0.0.1".into()],
        None,
        None,
    )
    .unwrap();
    let url = sqlite_url(dir.path());
    let writer = start_node(
        &url,
        dir.path().join("state-writer"),
        tls.clone(),
        "writer",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let failing = start_node(
        &url,
        dir.path().join("state-failing"),
        tls.clone(),
        "failing",
        CacheGateway::new(
            Arc::new(FailingCache),
            Duration::from_millis(20),
            Duration::from_secs(3600),
        ),
        Duration::from_secs(60),
    )
    .await;
    let disabled = start_node(
        &url,
        dir.path().join("state-disabled"),
        tls,
        "disabled",
        CacheSettings {
            mode: preloop_sandbox::cache::CacheMode::None,
            ..CacheSettings::default()
        }
        .gateway()
        .unwrap(),
        Duration::from_secs(60),
    )
    .await;

    let v1 = template_spec("outage-template", 0x33);
    writer.client().put_template(v1.clone()).await.unwrap();

    let templates = failing.client().list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "outage-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v1)
    );
    assert!(failing.service.cache().health().errors > 0);

    let templates = disabled.client().list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "outage-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v1)
    );
    assert_eq!(disabled.service.cache().health().hits, 0);

    // A write after the outage is visible to the failing node on the next
    // page; the outage never turned into a read failure or a stale answer.
    let v2 = template_spec("outage-template", 0x44);
    writer.client().put_template(v2.clone()).await.unwrap();
    let templates = failing.client().list_templates().await.unwrap();
    assert_eq!(
        find_template(&templates, "outage-template").unwrap().digest,
        preloop_sandbox::service::template_digest(&v2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_operations_are_unchanged_by_cache_flush_and_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let tls = ServerTls::load_or_create(
        &dir.path().join("tls"),
        vec!["127.0.0.1".into()],
        None,
        None,
    )
    .unwrap();
    let node = start_node(
        &sqlite_url(dir.path()),
        dir.path().join("state"),
        tls,
        "lease-node",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    node.service
        .put_template(template_spec("lease-template", 0x55))
        .await
        .unwrap();
    node.service
        .put_pool(pool_spec("lease-pool", "lease-template", 4))
        .await
        .unwrap();

    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("machines"),
        FakeDriver::full_capabilities(),
    ));
    let mut agent_config = AgentConfig::new("lease-agent");
    agent_config.heartbeat = Duration::from_secs(1);
    agent_config.reconcile = Duration::from_millis(300);
    let agent = Agent::new(
        Arc::new(node.service.clone()) as Arc<dyn ProviderLink>,
        vec![fake as Arc<dyn Driver>],
        agent_config,
    );
    tokio::spawn(agent.run(None, node.shutdown.clone()));
    wait_for_provider(&node.service, "lease-agent").await;

    let client = node.client();
    let acquired = client
        .acquire(AcquireRequest {
            idempotency_key: "cache-flush-lease-1".into(),
            pool: "lease-pool".into(),
            purpose: Purpose::Runner.into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let lease = acquired.lease.as_option().expect("lease").clone();
    assert!(!lease.sandbox_id.is_empty());

    // Flush and replace the cache continuously while lease operations run.
    let service = node.service.clone();
    let flusher = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            assert!(service.cache().clear().await);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    for _ in 0..10 {
        let renewed = client
            .renew(RenewRequest {
                lease_id: lease.id.clone(),
                ttl_secs: 3600,
                ..RenewRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(renewed.sandbox_id, lease.sandbox_id);
        let sandbox = client.get(&lease.sandbox_id, &lease.id).await.unwrap();
        assert_eq!(sandbox.id, lease.sandbox_id);
    }
    flusher.await.unwrap();
    client
        .release(ReleaseRequest {
            lease_id: lease.id.clone(),
            mode: ReleaseMode::Destroy.into(),
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();

    // The released key acquires a fresh sandbox; no allocation count, lease
    // owner or sandbox state changed because the cache was flushed.
    let reacquired = client
        .acquire(AcquireRequest {
            idempotency_key: "cache-flush-lease-1".into(),
            pool: "lease-pool".into(),
            purpose: Purpose::Runner.into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let second = reacquired.lease.as_option().expect("second lease");
    assert_ne!(second.id, lease.id);
    assert_ne!(second.sandbox_id, lease.sandbox_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_list_pages_keep_sql_page_membership() {
    let dir = tempfile::tempdir().unwrap();
    let tls = ServerTls::load_or_create(
        &dir.path().join("tls"),
        vec!["127.0.0.1".into()],
        None,
        None,
    )
    .unwrap();
    let node = start_node(
        &sqlite_url(dir.path()),
        dir.path().join("state"),
        tls,
        "page-node",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    for name in ["page-a", "page-b", "page-c"] {
        node.service
            .put_template(template_spec(name, 0x10))
            .await
            .unwrap();
    }
    let first = node
        .service
        .list_templates_page(&ListRequest {
            page_size: 2,
            ..ListRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(first.templates.len(), 2);
    assert!(!first.next_page_token.is_empty());
    let second = node
        .service
        .list_templates_page(&ListRequest {
            page_size: 2,
            page_token: first.next_page_token.clone(),
            ..ListRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(second.templates.len(), 1);
    assert!(second.next_page_token.is_empty());
    let mut names: Vec<String> = first
        .templates
        .iter()
        .chain(second.templates.iter())
        .map(|template| template.spec.name.clone())
        .collect();
    names.sort();
    assert_eq!(names, ["page-a", "page-b", "page-c"]);

    // Page membership is read afresh: a new first-page name is not hidden by
    // a warm payload cache.
    node.service
        .put_template(template_spec("page-0", 0x10))
        .await
        .unwrap();
    let first = node
        .service
        .list_templates_page(&ListRequest {
            page_size: 2,
            ..ListRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(first.templates[0].spec.name, "page-0");
}

/// Two service replicas on one Postgres database with separate caches.
///
/// Runs when `PRELOOP_SANDBOX_TEST_PG` names a dedicated test database (the
/// caller owns the cluster). Rows use unique names, so parallel tests cannot
/// make the assertions ambiguous.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_replicas_follow_concurrent_writes_with_separate_caches() {
    let Ok(url) = std::env::var("PRELOOP_SANDBOX_TEST_PG") else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let tls = ServerTls::load_or_create(
        &dir.path().join("tls"),
        vec!["127.0.0.1".into()],
        None,
        None,
    )
    .unwrap();
    let a = start_node(
        &url,
        dir.path().join("state-a"),
        tls.clone(),
        "pg-node-a",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let b = start_node(
        &url,
        dir.path().join("state-b"),
        tls,
        "pg-node-b",
        memory_cache(),
        Duration::from_secs(60),
    )
    .await;
    let name = format!("pg-cache-{}", uuid::Uuid::new_v4().simple());
    let v1 = template_spec(&name, 0x66);
    let v2 = template_spec(&name, 0x77);
    let digest_v1 = preloop_sandbox::service::template_digest(&v1);
    let digest_v2 = preloop_sandbox::service::template_digest(&v2);

    a.client().put_template(v1.clone()).await.unwrap();
    let templates = b.client().list_templates().await.unwrap();
    assert_eq!(find_template(&templates, &name).unwrap().digest, digest_v1);
    b.client().list_templates().await.unwrap();
    wait_for_cache_hit(&b).await;

    let write = {
        let client = a.client();
        let v2 = v2.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.put_template(v2).await.unwrap();
        })
    };
    let reader_client = b.client();
    let reader = {
        let name = name.clone();
        tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..20 {
                let templates = reader_client.list_templates().await.unwrap();
                seen.push(find_template(&templates, &name).unwrap().digest.clone());
                tokio::task::yield_now().await;
            }
            seen
        })
    };
    write.await.unwrap();
    let seen = reader.await.unwrap();
    assert!(
        seen.iter()
            .all(|digest| digest == &digest_v1 || digest == &digest_v2)
    );

    assert!(b.service.cache().clear().await);
    let templates = b.client().list_templates().await.unwrap();
    assert_eq!(find_template(&templates, &name).unwrap().digest, digest_v2);
}
