//! Shared backend behavior: the same assertions run against SQLite and, when
//! `PRELOOP_TEST_POSTGRES_URL` is set, against a real Postgres database.

use std::sync::Arc;
use std::time::Duration;

use crate::api::*;
use crate::error::Code;

use super::backend::SnapshotCommand;
use super::logic;
use super::types::SnapshotStart;
use super::types::*;
use super::{SandboxBackend, open_embedded};

fn config(url: String, dir: &std::path::Path) -> BackendConfig {
    BackendConfig {
        url,
        state_dir: dir.to_path_buf(),
        seal_key: None,
        pool_size: 4,
        node_id: "test-node".into(),
        session_id: "test-session".into(),
        node_credential_sha256: "credential".into(),
        peer_url: "https://127.0.0.1:1".into(),
        node_lost_after: Duration::from_secs(5),
    }
}

async fn sqlite(dir: &std::path::Path) -> Arc<dyn SandboxBackend> {
    open_embedded(config(
        format!("sqlite://{}", dir.join("sandbox.db").display()),
        dir,
    ))
    .await
    .unwrap()
}

/// Run the same suite against Postgres when `PRELOOP_TEST_POSTGRES_URL`
/// points at a disposable database. Each test gets its own database so tests
/// never share the `sandbox` schema.
async fn backend(dir: &std::path::Path) -> Arc<dyn SandboxBackend> {
    match std::env::var("PRELOOP_TEST_POSTGRES_URL") {
        Ok(url) if !url.is_empty() => {
            let (admin, database) = unique_pg_database(&url).await;
            let mut config = config(database, dir);
            config.seal_key = Some([7u8; 32]);
            config.pool_size = 2;
            let backend = open_embedded(config).await.unwrap();
            drop(admin);
            backend
        }
        _ => sqlite(dir).await,
    }
}

async fn unique_pg_database(url: &str) -> (tokio_postgres::Client, String) {
    let (admin_url, _) = url.rsplit_once('/').unwrap_or((url, ""));
    let admin_url = format!("{admin_url}/postgres");
    let (client, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = format!(
        "sbx_test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    client
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap();
    let base = url.rsplit_once('/').map(|(base, _)| base).unwrap_or(url);
    (client, format!("{base}/{name}"))
}

/// A SQLite-only backend for tests that inspect the database file.
async fn sqlite_only(dir: &std::path::Path) -> Arc<dyn SandboxBackend> {
    sqlite(dir).await
}

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        source: Some(template_spec::Source::Recipe("test".into())),
        requires: Requirements {
            driver: "fake".into(),
            arch: host_arch().into(),
            ..Requirements::default()
        }
        .into(),
        ..TemplateSpec::default()
    }
}

fn provider(name: &str) -> RegisterRequest {
    RegisterRequest {
        name: name.into(),
        protocol_version: crate::PROTOCOL_VERSION,
        drivers: vec![DriverInfo {
            driver: "fake".into(),
            capabilities: Capabilities {
                os: "linux".into(),
                arch: vec![host_arch().into()],
                slots: 64,
                pause: true,
                snapshot: true,
                host_mounts: true,
                socket_mounts: true,
                default_shape: Shape {
                    cpus: 1,
                    memory_mib: 256,
                    disk_gib: 1,
                    ..Shape::default()
                }
                .into(),
                ..Capabilities::default()
            }
            .into(),
            ..DriverInfo::default()
        }],
        ..RegisterRequest::default()
    }
}

fn acquire_request(key: &str, name: &str, ttl: u64) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        template: "base".into(),
        name: name.into(),
        ttl_secs: ttl,
        ..AcquireRequest::default()
    }
}

fn owner() -> Owner {
    Owner {
        node_id: "test-node".into(),
        session_id: "test-session".into(),
    }
}

async fn setup(backend: &Arc<dyn SandboxBackend>) -> (Template, RegisterResponse) {
    let template = backend.put_template(template("base")).await.unwrap();
    let registered = backend
        .register(provider("host-a"), None, &owner())
        .await
        .unwrap();
    (template, registered)
}

async fn new_member(backend: &Arc<dyn SandboxBackend>, name: &str) -> NewSandbox {
    let template = backend.template_get("base").await.unwrap().unwrap().value;
    NewSandbox {
        template,
        pool: String::new(),
        pool_revision: 0,
        purpose: Purpose::Runner,
        name: name.into(),
        provider_id: backend.list_providers_all().await.unwrap()[0].id.clone(),
        driver: "fake".into(),
        shape: Shape::default(),
        network: NetworkPolicy::default(),
        mounts: Vec::new(),
        sockets: Vec::new(),
        options: StringMap::default(),
        requires: Requirements::default(),
        snapshot_capable: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn template_and_pool_reads_are_versioned_and_aba_safe() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let put = backend.put_template(template("base")).await.unwrap();
    assert_eq!(put.spec.name, "base");
    let first = backend.template_get("base").await.unwrap().unwrap();
    assert_eq!(
        backend
            .resource_version(ResourceKind::Template, "base")
            .await
            .unwrap(),
        Some(first.version)
    );
    let again = backend.put_template(template("base")).await.unwrap();
    assert_eq!(again.digest, put.digest);
    assert_eq!(
        backend.template_get("base").await.unwrap().unwrap().version,
        first.version,
        "an identical put is idempotent"
    );
    backend.delete_template("base").await.unwrap();
    assert!(backend.template_get("base").await.unwrap().is_none());
    backend.put_template(template("base")).await.unwrap();
    let recreated = backend.template_get("base").await.unwrap().unwrap();
    assert!(
        recreated.version > first.version,
        "a delete/recreate never reuses a version"
    );
    assert!(
        backend
            .template_get_at("base", first.version)
            .await
            .unwrap()
            .is_none()
    );
    let (meta, more) = backend.template_page_meta(None, 10).await.unwrap();
    assert_eq!(meta.len(), 1);
    assert!(!more);
    assert_eq!(meta[0].id, "base");
    let pool = PoolSpec {
        name: "p".into(),
        template: "base".into(),
        warm: 2,
        max: 4,
        lease_ttl_secs: 60,
        ..PoolSpec::default()
    };
    let revision = backend.put_pool(pool.clone()).await.unwrap();
    let mut scaled = revision.clone();
    scaled.warm = 4;
    scaled.max = 8;
    let scaled = backend.put_pool(scaled).await.unwrap();
    assert_eq!(scaled.warm, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acquire_create_report_release_is_authoritative() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let (template, registered) = setup(&backend).await;
    let member = new_member(&backend, "one").await;
    let command = AcquireCommand {
        request: acquire_request("key-1", "one", 60),
        fingerprint: "fp".into(),
        ttl_secs: 60,
        unclaimed_ttl_secs: 600,
        create_timeout: Duration::from_secs(60),
        placement: None,
        owner: owner(),
    };
    assert!(matches!(
        backend.acquire(command.clone()).await.unwrap(),
        AcquireStep::PlacementNeeded
    ));
    let command = AcquireCommand {
        placement: Some(NewSandbox {
            template: template.clone(),
            ..member
        }),
        ..command
    };
    let step = backend.acquire(command.clone()).await.unwrap();
    assert!(matches!(step, AcquireStep::Wait { .. }), "{step:?}");
    let work = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let create = work
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Create(_))))
        .expect("create work delivered");
    assert!(create.attempt > 0);
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: create.id.clone(),
            attempt: create.attempt,
            ok: true,
            native_ref: "native-1".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    let step = backend.acquire(command).await.unwrap();
    let AcquireStep::Ready { lease, sandbox } = step else {
        panic!("expected ready, got {step:?}");
    };
    assert_eq!(sandbox.native_ref, "native-1");
    assert_eq!(sandbox.state.known(), SandboxState::Leased);
    assert_eq!(lease.sandbox_id, sandbox.id);
    // A stale delivery attempt is fenced.
    let error = backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: create.id.clone(),
            attempt: create.attempt + 1,
            ok: true,
            native_ref: "native-1".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Aborted);
    backend
        .release(&lease.id, ReleaseMode::Destroy, 0, 0)
        .await
        .unwrap();
    let destroy = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let destroy = destroy
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Destroy(_))))
        .expect("destroy delivered");
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: destroy.id.clone(),
            attempt: destroy.attempt,
            ok: true,
            native_ref: String::new(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    assert!(backend.list_all().await.unwrap().is_empty());
}

/// A provider's inventory is a snapshot taken before the report is sent. A
/// machine whose create landed in that window is missing from a report that
/// is otherwise current; failing it would fail a live sandbox.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_create_is_not_lost_to_a_stale_inventory_sample() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let (template, registered) = setup(&backend).await;
    let member = new_member(&backend, "fresh").await;
    let command = AcquireCommand {
        request: acquire_request("key-fresh", "fresh", 60),
        fingerprint: "fp".into(),
        ttl_secs: 60,
        unclaimed_ttl_secs: 600,
        create_timeout: Duration::from_secs(60),
        placement: None,
        owner: owner(),
    };
    assert!(matches!(
        backend.acquire(command.clone()).await.unwrap(),
        AcquireStep::PlacementNeeded
    ));
    let command = AcquireCommand {
        placement: Some(NewSandbox { template, ..member }),
        ..command
    };
    assert!(matches!(
        backend.acquire(command.clone()).await.unwrap(),
        AcquireStep::Wait { .. }
    ));
    let work = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let create = work
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Create(_))))
        .expect("create work delivered");
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: create.id.clone(),
            attempt: create.attempt,
            ok: true,
            native_ref: "native-fresh".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    let AcquireStep::Ready { sandbox, .. } = backend.acquire(command).await.unwrap() else {
        panic!("the create report completes the acquire")
    };
    assert_eq!(sandbox.native_ref, "native-fresh");

    // The sample the provider reports predates the machine it just created.
    let response = backend
        .reconcile(ReconcileRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            sandboxes: Vec::new(),
            ..ReconcileRequest::default()
        })
        .await
        .unwrap();
    assert!(response.destroy.is_empty(), "no orphans to destroy");
    let record = backend
        .list_all()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.id == sandbox.id)
        .expect("the record survives the stale sample");
    assert_eq!(
        record.state.known(),
        SandboxState::Leased,
        "a create inside the grace window is not reported as lost"
    );
    assert!(record.error.is_empty(), "{}", record.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_acquires_over_two_backends_produce_one_lease_per_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.db");
    let a = open_embedded(config(format!("sqlite://{}", path.display()), dir.path()))
        .await
        .unwrap();
    let b = open_embedded(config(format!("sqlite://{}", path.display()), dir.path()))
        .await
        .unwrap();
    let _ = setup(&a).await;
    // Re-register through B so both share the same durable provider row.
    let template = a.template_get("base").await.unwrap().unwrap().value;
    let provider_id = a.list_providers_all().await.unwrap()[0].id.clone();
    let member = NewSandbox {
        template,
        pool: String::new(),
        pool_revision: 0,
        purpose: Purpose::Runner,
        name: String::new(),
        provider_id,
        driver: "fake".into(),
        shape: Shape::default(),
        network: NetworkPolicy::default(),
        mounts: Vec::new(),
        sockets: Vec::new(),
        options: StringMap::default(),
        requires: Requirements::default(),
        snapshot_capable: false,
    };
    let mut tasks = Vec::new();
    for index in 0..200 {
        let backend = if index % 2 == 0 { a.clone() } else { b.clone() };
        let key = format!("key-{}", index % 50);
        let member = member.clone();
        tasks.push(tokio::spawn(async move {
            let mut command = AcquireCommand {
                request: acquire_request(&key, "", 60),
                fingerprint: "fp".into(),
                ttl_secs: 60,
                unclaimed_ttl_secs: 600,
                create_timeout: Duration::from_secs(60),
                placement: None,
                owner: owner(),
            };
            if matches!(
                backend.acquire(command.clone()).await.unwrap(),
                AcquireStep::PlacementNeeded
            ) {
                command.placement = Some(member);
                let _ = backend.acquire(command).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let sandboxes = a.list_all().await.unwrap();
    assert_eq!(sandboxes.len(), 50, "exactly one new sandbox per key");
    let sandboxes_b = b.list_all().await.unwrap();
    assert_eq!(sandboxes_b.len(), 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_name_has_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let (template, _) = setup(&backend).await;
    let member = new_member(&backend, "same").await;
    let mut handles = Vec::new();
    for index in 0..8 {
        let backend = backend.clone();
        let template = template.clone();
        let member = member.clone();
        handles.push(tokio::spawn(async move {
            let command = AcquireCommand {
                request: acquire_request(&format!("name-key-{index}"), "same", 60),
                fingerprint: "fp".into(),
                ttl_secs: 60,
                unclaimed_ttl_secs: 600,
                create_timeout: Duration::from_secs(60),
                placement: Some(NewSandbox { template, ..member }),
                owner: owner(),
            };
            backend.acquire(command).await
        }));
    }
    let mut winners = 0;
    for handle in handles {
        match handle.await.unwrap().unwrap() {
            AcquireStep::Wait { .. } => winners += 1,
            AcquireStep::Failed(error) => assert_eq!(
                error.code,
                Code::AlreadyExists,
                "{}",
                crate::error::message(&error)
            ),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(winners, 1, "one create wins the live name");
    assert_eq!(backend.list_all().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_receipt_survives_source_consumption_and_is_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let (template, registered) = setup(&backend).await;
    let command = AcquireCommand {
        request: acquire_request("snap-key", "", 60),
        fingerprint: "fp".into(),
        ttl_secs: 60,
        unclaimed_ttl_secs: 600,
        create_timeout: Duration::from_secs(60),
        placement: Some(NewSandbox {
            template,
            ..new_member(&backend, "").await
        }),
        owner: owner(),
    };
    let AcquireStep::Wait { .. } = backend.acquire(command.clone()).await.unwrap() else {
        panic!("expected wait");
    };
    let work = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let create = work
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Create(_))))
        .unwrap();
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: create.id.clone(),
            attempt: create.attempt,
            ok: true,
            native_ref: "native-snap".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    let AcquireStep::Ready { lease, .. } = backend.acquire(command).await.unwrap() else {
        panic!("expected ready");
    };
    let start = backend
        .snapshot_start(SnapshotCommand {
            sandbox_id: lease.sandbox_id.clone(),
            name: "snap".into(),
            lease_id: lease.id.clone(),
            idempotency_key: "snapshot-request-1".into(),
            owner: owner(),
            timeout: Duration::from_secs(30),
        })
        .await
        .unwrap();
    let SnapshotStart::Queued { work_id } = start else {
        panic!("expected queued");
    };
    let work = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let snapshot = work
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Snapshot(_))))
        .unwrap();
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: snapshot.id.clone(),
            attempt: snapshot.attempt,
            ok: true,
            native_ref: "snapshot-native".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    assert!(
        backend.list_all().await.unwrap().is_empty(),
        "source consumed"
    );
    assert!(
        backend.lease_env(&lease.id).await.is_err(),
        "lease consumed"
    );
    let replay = backend
        .snapshot_start(SnapshotCommand {
            sandbox_id: lease.sandbox_id.clone(),
            name: "snap".into(),
            lease_id: lease.id.clone(),
            idempotency_key: "snapshot-request-1".into(),
            owner: owner(),
            timeout: Duration::from_secs(30),
        })
        .await
        .unwrap();
    let SnapshotStart::Replay(template) = replay else {
        panic!("expected replay");
    };
    assert_eq!(template.spec.name, "snap");
    assert_eq!(template.native_ref, "snapshot-native");
    assert_eq!(
        backend
            .work_result(&work_id)
            .await
            .unwrap()
            .unwrap()
            .native_ref,
        "snapshot-native"
    );
    let wrong_capability = backend
        .snapshot_start(SnapshotCommand {
            sandbox_id: lease.sandbox_id,
            name: "snap".into(),
            lease_id: "lse-other".into(),
            idempotency_key: "snapshot-request-1".into(),
            owner: owner(),
            timeout: Duration::from_secs(30),
        })
        .await
        .unwrap_err();
    assert_eq!(wrong_capability.code, Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_env_and_ticket_tokens_are_not_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let backend = sqlite_only(dir.path()).await;
    backend
        .node_register(&config(
            format!("sqlite://{}", dir.path().join("sandbox.db").display()),
            dir.path(),
        ))
        .await
        .unwrap();
    let (template, registered) = setup(&backend).await;
    let mut env = StringMap::default();
    env.insert("TOKEN".into(), "super-secret".into());
    let request = AcquireRequest {
        inject: Inject {
            env: env.clone(),
            ..Inject::default()
        }
        .into(),
        ..acquire_request("env-key", "", 60)
    };
    let command = AcquireCommand {
        request,
        fingerprint: "fp".into(),
        ttl_secs: 60,
        unclaimed_ttl_secs: 600,
        create_timeout: Duration::from_secs(60),
        placement: Some(NewSandbox {
            template,
            ..new_member(&backend, "").await
        }),
        owner: owner(),
    };
    let AcquireStep::Wait { .. } = backend.acquire(command.clone()).await.unwrap() else {
        panic!("expected wait");
    };
    let work = backend
        .poll_work(
            &registered.provider_id,
            registered.generation,
            8,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let create = work
        .iter()
        .find(|item| matches!(item.work, Some(work_item::Work::Create(_))))
        .unwrap();
    backend
        .report(ReportRequest {
            provider_id: registered.provider_id.clone(),
            generation: registered.generation,
            work_id: create.id.clone(),
            attempt: create.attempt,
            ok: true,
            native_ref: "native-env".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    let AcquireStep::Ready { lease, .. } = backend.acquire(command).await.unwrap() else {
        panic!("expected ready");
    };
    assert_eq!(
        backend.lease_env(&lease.id).await.unwrap().get("TOKEN"),
        Some(&"super-secret".to_owned())
    );
    // The durable file never contains the plaintext value.
    let raw = std::fs::read(dir.path().join("sandbox.db")).unwrap();
    assert!(
        !raw.windows(b"super-secret".len())
            .any(|window| window == b"super-secret"),
        "sealed env must not appear in the database file"
    );
    let ticket_id = "tkt-1";
    backend
        .create_ticket(
            ticket_id,
            &lease.sandbox_id,
            &lease.id,
            StreamKind::Upload,
            "/",
            &logic::token_hash("ticket-token"),
            0,
            &owner_for_ticket(),
        )
        .await
        .unwrap_err();
    backend
        .create_ticket(
            ticket_id,
            &lease.sandbox_id,
            &lease.id,
            StreamKind::Upload,
            "/",
            &logic::token_hash("ticket-token"),
            super::store::now_secs() + 60,
            &owner_for_ticket(),
        )
        .await
        .unwrap();
    let bad = backend
        .claim_ticket(
            ticket_id,
            TicketSide::Producer,
            StreamKind::Upload,
            300,
            TicketAuthority::Caller {
                token: "wrong-token",
            },
        )
        .await
        .unwrap_err();
    assert_eq!(bad.code, Code::Unauthenticated);
    let claim = backend
        .claim_ticket(
            ticket_id,
            TicketSide::Producer,
            StreamKind::Upload,
            300,
            TicketAuthority::Caller {
                token: "ticket-token",
            },
        )
        .await
        .unwrap();
    assert!(!claim.both_claimed);
    let duplicate = backend
        .claim_ticket(
            ticket_id,
            TicketSide::Producer,
            StreamKind::Upload,
            300,
            TicketAuthority::Caller {
                token: "ticket-token",
            },
        )
        .await
        .unwrap_err();
    assert_eq!(duplicate.code, Code::FailedPrecondition);
}

fn owner_for_ticket() -> Owner {
    Owner {
        node_id: "test-node".into(),
        session_id: "test-session".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_store_key_fails_closed_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sealed.db");
    let mut first = config(format!("sqlite://{}", path.display()), dir.path());
    first.seal_key = Some([1u8; 32]);
    open_embedded(first).await.unwrap();
    let mut second = config(format!("sqlite://{}", path.display()), dir.path());
    second.seal_key = Some([2u8; 32]);
    let error = match open_embedded(second).await {
        Ok(_) => panic!("a different store key must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.code, Code::FailedPrecondition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_credentials_are_hashed_and_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend(dir.path()).await;
    let node_id = "node-a";
    let session = "session-a";
    let credential = logic::token_hash("node-secret");
    backend
        .node_register(&BackendConfig {
            url: "sqlite::memory:".into(),
            state_dir: dir.path().to_path_buf(),
            seal_key: None,
            pool_size: 1,
            node_id: node_id.into(),
            session_id: session.into(),
            node_credential_sha256: credential.clone(),
            peer_url: "https://127.0.0.1:9".into(),
            node_lost_after: Duration::from_secs(5),
        })
        .await
        .unwrap();
    assert!(
        backend
            .node_authenticate(node_id, session, &credential)
            .await
            .unwrap()
    );
    assert!(
        !backend
            .node_authenticate(node_id, session, "wrong")
            .await
            .unwrap()
    );
    backend.node_close(node_id, session).await.unwrap();
    assert!(
        !backend
            .node_authenticate(node_id, session, &credential)
            .await
            .unwrap()
    );
}
