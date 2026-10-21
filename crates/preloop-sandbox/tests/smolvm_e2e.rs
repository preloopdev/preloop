//! Real SmolVM end to end: bake a golden through the service, snapshot it,
//! serve a warm pool of branches, exec, move files, pause/resume, release.
//!
//! Needs a working `smolvm` on PATH; runs only with
//! `PRELOOP_SANDBOX_E2E_SMOLVM=1` (`cargo test -p preloop-sandbox --test
//! smolvm_e2e -- --nocapture`).

use std::sync::Arc;
use std::time::Duration;

use preloop_sandbox::agent::{Agent, AgentConfig, HostAccess, ProviderLink};
use preloop_sandbox::api::*;
use preloop_sandbox::drivers::smolvm::SmolvmDriver;
use preloop_sandbox::drivers::smolvm::cli::SmolVmProvider;
use preloop_sandbox::{Driver, Service, ServiceConfig, store};
use tokio_util::sync::CancellationToken;

async fn run(service: &Arc<Service>, sandbox: &str, lease_id: &str, script: &str) -> (i32, String) {
    let (code, stdout, stderr) = service
        .exec_collect(ExecRequest {
            sandbox_id: sandbox.into(),
            lease_id: lease_id.into(),
            argv: vec!["sh".into(), "-c".into(), script.into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    (
        code,
        String::from_utf8_lossy(&stdout).into_owned() + &String::from_utf8_lossy(&stderr),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smolvm_golden_fork_pool_end_to_end() {
    if std::env::var("PRELOOP_SANDBOX_E2E_SMOLVM").as_deref() != Ok("1") {
        eprintln!("skipped: set PRELOOP_SANDBOX_E2E_SMOLVM=1");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.acquire_wait = Duration::from_secs(600);
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    let driver = Arc::new(SmolvmDriver::new(SmolVmProvider::default(), 0));
    let shutdown = CancellationToken::new();
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![driver.clone() as Arc<dyn Driver>],
        AgentConfig::new("e2e-smolvm", dir.path().join("scratch")).with_access(HostAccess {
            mounts: Vec::new(),
            // The build sandbox mounts a host socket from the temp dir.
            sockets: vec![dir.path().to_path_buf()],
        }),
    );
    tokio::spawn(agent.run(shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    while service.list_providers().is_empty() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A host socket the guest should see through --mount-socket.
    let socket = dir.path().join("engine.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();

    service
        .put_template(TemplateSpec {
            name: "alpine".into(),
            os: "linux".into(),
            image: Some("alpine:3.20".into()),
            ..TemplateSpec::default()
        })
        .await
        .unwrap();
    let build = service
        .acquire(AcquireRequest {
            idempotency_key: "bake".into(),
            template: "alpine".into(),
            purpose: Purpose::Build,
            shape: Shape {
                cpus: 2,
                memory_mib: 1024,
                disk_gib: 4,
            },
            network: NetworkPolicy {
                mode: NetworkMode::Unrestricted,
                ..NetworkPolicy::default()
            },
            options: [("dns".to_owned(), "8.8.8.8".to_owned())].into(),
            sockets: vec![SocketMount {
                host: socket.display().to_string(),
                guest: "/run/engine.sock".into(),
            }],
            ..AcquireRequest::default()
        })
        .await
        .expect("build sandbox");
    let (code, out) = run(
        &service,
        &build.sandbox.id,
        &build.lease.id,
        "echo baked > /etc/golden-marker && uname -m",
    )
    .await;
    assert_eq!(code, 0, "{out}");
    let golden = service
        .snapshot(SnapshotRequest {
            sandbox_id: build.sandbox.id.clone(),
            lease_id: build.lease.id.clone(),
            name: "alpine-golden".into(),
        })
        .await
        .expect("snapshot");
    eprintln!("golden = {}", golden.native_ref);

    service
        .put_pool(PoolSpec {
            name: "alpine-pool".into(),
            template: "alpine-golden".into(),
            warm: 1,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    let mut leases = Vec::new();
    for key in ["job-1", "job-2"] {
        let started = std::time::Instant::now();
        let mut request = AcquireRequest {
            idempotency_key: key.into(),
            pool: "alpine-pool".into(),
            purpose: Purpose::Runner,
            ..AcquireRequest::default()
        };
        request.inject.env.insert("JOB".into(), key.into());
        request.inject.files.push(FileSpec {
            path: "/run/preloop/token".into(),
            data: format!("token-{key}").into_bytes(),
            mode: 0o600,
        });
        let lease = service.acquire(request).await.expect("pool acquire");
        eprintln!(
            "{key}: {} in {:?}",
            lease.sandbox.native_ref,
            started.elapsed()
        );
        let (code, out) = run(
            &service,
            &lease.sandbox.id,
            &lease.lease.id,
            "cat /etc/golden-marker; echo $JOB; cat /run/preloop/token; echo; test -S /run/engine.sock && echo socket",
        )
        .await;
        assert_eq!(code, 0, "{out}");
        assert_eq!(out, format!("baked\n{key}\ntoken-{key}\nsocket\n"));
        leases.push(lease);
    }

    // Data plane round trip.
    let src = dir.path().join("in.tar");
    {
        let mut builder = tar::Builder::new(std::fs::File::create(&src).unwrap());
        let mut header = tar::Header::new_gnu();
        header.set_size(5);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "w/hello.txt", &b"hello"[..])
            .unwrap();
        builder.finish().unwrap();
    }
    let sandbox = leases[0].sandbox.id.clone();
    let lease_id = leases[0].lease.id.clone();
    service
        .upload_path(&sandbox, &lease_id, "/work", &src)
        .await
        .unwrap();
    let (code, out) = run(&service, &sandbox, &lease_id, "cat /work/w/hello.txt").await;
    assert_eq!((code, out.as_str()), (0, "hello"));
    let out_tar = dir.path().join("out.tar");
    service
        .download_path(&sandbox, &lease_id, "/work/w", &out_tar)
        .await
        .unwrap();
    assert!(std::fs::metadata(&out_tar).unwrap().len() > 0);

    // Streamed output with a non-zero exit.
    let (code, out) = run(
        &service,
        &sandbox,
        &lease_id,
        "for i in 1 2 3; do echo line$i; sleep 0.2; done; exit 7",
    )
    .await;
    assert_eq!(code, 7);
    assert_eq!(out, "line1\nline2\nline3\n");

    for lease in leases {
        service
            .release(ReleaseRequest {
                lease_id: lease.lease.id,
                mode: ReleaseMode::Destroy,
                ..ReleaseRequest::default()
            })
            .await
            .unwrap();
    }
    service.delete_pool("alpine-pool").await.unwrap();
    for _ in 0..600 {
        if service.list().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(service.list().is_empty(), "{:?}", service.list());
    // Only the golden remains on the host; drop it.
    let left: Vec<String> = driver
        .inventory()
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.native_ref)
        .collect();
    assert_eq!(left, vec![golden.native_ref.clone()]);
    driver.drop_snapshot(&golden.native_ref).await.unwrap();
    shutdown.cancel();
}
