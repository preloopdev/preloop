// scripts/record_dap_conformance.js
const { spawn, spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const REPO_ROOT = path.resolve(__dirname, "..");
const MITM_DIR = path.resolve(REPO_ROOT, "../mitm-proxy/experiments/mitm");
const MITMDUMP_BIN = path.resolve(MITM_DIR, ".venv/bin/mitmdump");
const CAPTURES_DIR = path.resolve(REPO_ROOT, ".runner-watch/dap-captures");
const OFFICIAL_RUNNER_DIR = path.resolve(MITM_DIR, ".cache/runner-official");
const AKSH_RUNNER_DIR = path.resolve(REPO_ROOT, "target/release");
const SYSTEM_TOKEN = process.env.AKSH_SYSTEM_TOKEN || "aksh-system-token";

// Ensure capture directories exist
fs.mkdirSync(CAPTURES_DIR, { recursive: true });
fs.mkdirSync(path.join(CAPTURES_DIR, "official"), { recursive: true });
fs.mkdirSync(path.join(CAPTURES_DIR, "aksh"), { recursive: true });

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper to kill a process group/tree or process safely
function killProcess(proc) {
    try {
        proc.kill("SIGINT");
    } catch (e) {}
}

async function runScenario(backend) {
    console.log(`\n==================================================`);
    console.log(`Starting DAP E2E Conformance Recording for: ${backend}`);
    console.log(`==================================================`);

    const tempStateDir = fs.mkdtempSync(path.join("/tmp", `aksh-state-${backend}-`));
    const tempCaptureDir = fs.mkdtempSync(path.join("/tmp", `aksh-capture-${backend}-`));
    console.log(`Temp state dir: ${tempStateDir}`);
    console.log(`Temp capture dir: ${tempCaptureDir}`);

    // 1. Start aksh-runner-server
    console.log("Starting aksh-runner-server...");
    const serverProc = spawn(
        path.join(AKSH_RUNNER_DIR, "aksh-runner-server"),
        ["serve", "--listen", "0.0.0.0:9090", "--state-dir", tempStateDir],
        {
            stdio: "pipe",
            env: {
                ...process.env,
                RUST_LOG: "info",
                AKSH_SYSTEM_TOKEN: SYSTEM_TOKEN,
                // Official runner needs LAN IP (port 80 redirect via mitm).
                // Aksh runner connects directly to localhost.
                AKSH_PUBLIC_URL: backend === "official"
                    ? "http://192.168.1.221:9090"
                    : "http://127.0.0.1:9090",
            }
        }
    );
    
    let serverOutput = "";
    serverProc.stdout.on("data", (data) => {
        serverOutput += data.toString();
        process.stdout.write("[Server] " + data.toString());
    });
    serverProc.stderr.on("data", (data) => {
        serverOutput += data.toString();
        process.stderr.write("[Server] " + data.toString());
    });

    // Wait for server to listen
    console.log("Waiting for server to start...");
    for (let i = 0; i < 30; i++) {
        if (serverOutput.includes("listening")) {
            break;
        }
        await sleep(200);
    }
    console.log("Server is listening on port 9090");

    // 2. Start mitmdump proxy on port 18080
    console.log("Starting mitmdump on port 18080...");
    const mitmProc = spawn(
        MITMDUMP_BIN,
        [
            "--listen-host", "127.0.0.1",
            "--listen-port", "18080",
            "-s", path.join(REPO_ROOT, "scripts/mitm_redirect.py"),
            "--save-stream-file", path.join(tempCaptureDir, "flows.mitm")
        ],
        {
            stdio: ["ignore", "inherit", "inherit"],
            env: {
                ...process.env,
                MITM_CAPTURE_DIR: tempCaptureDir
            }
        }
    );

    // Wait for proxy to bind
    await sleep(2000);

    // Get registration token
    console.log("Getting registration token...");
    const tokenResponse = await fetch("http://127.0.0.1:9090/api/v3/repos/owner/repo/actions/runners/registration-token", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Authorization": "RemoteAuth test"
        },
        body: "{}"
    });
    const tokenData = await tokenResponse.json();
    const token = tokenData.token;
    console.log(`Registration token: ${token}`);

    // Set proxy environment variables for the runner.
    // Official runner needs the proxy for port 80 → 9090 redirect.
    // Aksh runner connects directly — proxy breaks its WebSocket live-log connection.
    const baseEnv = {
        ...process.env,
        GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY: "1",
        RUST_LOG: "info"
    };
    const proxyEnv = {
        ...baseEnv,
        http_proxy: "http://127.0.0.1:18080",
        https_proxy: "http://127.0.0.1:18080",
        HTTP_PROXY: "http://127.0.0.1:18080",
        HTTPS_PROXY: "http://127.0.0.1:18080",
        SSL_CERT_FILE: path.join(MITM_DIR, ".cache/mitmproxy/mitmproxy-ca-cert.pem"),
    };
    const runnerEnv = backend === "official" ? proxyEnv : baseEnv;

    let runnerProc;
    if (backend === "official") {
        console.log("Configuring official runner...");
        // Remove prior configuration
        spawnSync(
            path.join(OFFICIAL_RUNNER_DIR, "config.sh"),
            ["remove", "--token", token],
            { stdio: "ignore", env: runnerEnv, cwd: OFFICIAL_RUNNER_DIR }
        );
        fs.rmSync(path.join(OFFICIAL_RUNNER_DIR, ".runner"), { force: true });
        fs.rmSync(path.join(OFFICIAL_RUNNER_DIR, ".credentials"), { force: true });
        fs.rmSync(path.join(OFFICIAL_RUNNER_DIR, ".credentials_rsaparams"), { force: true });

        // Configure
        const configRes = spawnSync(
            path.join(OFFICIAL_RUNNER_DIR, "config.sh"),
            [
                "--unattended",
                "--url", "http://192.168.1.221:9090/runner/server",
                "--token", token,
                "--name", `mitm-official-${backend}`,
                "--labels", "self-hosted,mitm,ubuntu-latest",
                "--work", "_work",
                "--replace"
            ],
            { stdio: "pipe", env: runnerEnv, cwd: OFFICIAL_RUNNER_DIR }
        );
        if (!fs.existsSync(path.join(OFFICIAL_RUNNER_DIR, ".runner"))) {
            console.error("Official runner config failed!");
            console.error(configRes.stdout.toString());
            console.error(configRes.stderr.toString());
            throw new Error("Configuration failed");
        }
        console.log("Official runner configured");

        console.log("Starting official runner...");
        runnerProc = spawn(
            path.join(OFFICIAL_RUNNER_DIR, "run.sh"),
            [],
            { stdio: "pipe", env: runnerEnv, cwd: OFFICIAL_RUNNER_DIR }
        );
    } else {
        console.log("Configuring aksh runner...");
        // Remove prior configuration
        fs.rmSync(path.join(REPO_ROOT, ".runner"), { force: true });
        fs.rmSync(path.join(REPO_ROOT, ".credentials"), { force: true });
        fs.rmSync(path.join(REPO_ROOT, ".credentials_rsaparams"), { force: true });

        // Configure
        const configRes = spawnSync(
            path.join(AKSH_RUNNER_DIR, "aksh-runner"),
            [
                "configure",
                "--unattended",
                "--url", "http://127.0.0.1:9090",
                "--token", token,
                "--name", `mitm-aksh-${backend}`,
                "--labels", "self-hosted,mitm,ubuntu-latest",
                "--work", "_work",
                "--replace"
            ],
            { stdio: "pipe", env: runnerEnv, cwd: REPO_ROOT }
        );
        if (!fs.existsSync(path.join(REPO_ROOT, ".runner"))) {
            console.error("aksh runner config failed!");
            console.error(configRes.stdout.toString());
            console.error(configRes.stderr.toString());
            throw new Error("Configuration failed");
        }
        console.log("aksh runner configured");

        console.log("Starting aksh runner...");
        runnerProc = spawn(
            path.join(AKSH_RUNNER_DIR, "aksh-runner"),
            ["run", "--once"],
            { stdio: "pipe", env: runnerEnv, cwd: REPO_ROOT }
        );
    }

    // Pipe runner output to a log file
    const runnerLogStream = fs.createWriteStream(path.join(tempCaptureDir, "runner.log"));
    runnerProc.stdout.pipe(runnerLogStream);
    runnerProc.stderr.pipe(runnerLogStream);

    // Wait for runner to startup and start polling
    await sleep(3000);

    // 3. Submit the workflow with debugger enabled
    console.log("Submitting workflow with debugger enabled...");
    const workflowYaml = `
on: push
jobs:
  build:
    runs-on: self-hosted
    steps:
      - name: Step 1
        run: echo "Executing Step 1"
      - name: Step 2
        run: echo "Executing Step 2"
`;
    const submitResponse = await fetch("http://127.0.0.1:9090/api/v1/runs", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${SYSTEM_TOKEN}`
        },
        body: JSON.stringify({
            workflow_yaml: workflowYaml,
            event: "push",
            repository: "owner/repo",
            enable_debugger: true,
            debugger_welcome_message: `DAP Debugger Session Started for ${backend}`
        })
    });
    const submitData = await submitResponse.json();
    const runId = submitData.run_id;
    console.log(`Run submitted successfully. Run ID: ${runId}`);

    // 4. Wait for debugger port registration and connect client WebSocket
    console.log("Connecting debugger client WebSocket...");
    let ws;
    let wsConnected = false;
    for (let retry = 0; retry < 60; retry++) {
        try {
            ws = new WebSocket(`ws://127.0.0.1:9090/api/v1/runs/${runId}/debug`, {
                headers: {
                    "Authorization": `Bearer ${SYSTEM_TOKEN}`
                }
            });
            await new Promise((resolve, reject) => {
                ws.onopen = () => {
                    wsConnected = true;
                    resolve();
                };
                ws.onerror = (err) => reject(err);
                // Auto close on timeout
                setTimeout(() => reject(new Error("Timeout")), 15000);
            });
            if (wsConnected) break;
        } catch (e) {
            await sleep(1000);
        }
    }

    if (!wsConnected) {
        throw new Error("Failed to connect debugger client WebSocket in time!");
    }
    console.log("Debugger client WebSocket connected successfully!");

    // 5. DAP Handshake and Step Continue flow
    // Standard DAP: client sends initialize immediately after connecting.
    let dapSeq = 1;
    let currentStep = 0;
    
    const dapFrames = [];

    // Send initialize request immediately (standard DAP client behavior).
    console.log("[DAP Client] Sending initialize...");
    const initReq = {
        seq: dapSeq++,
        type: "request",
        command: "initialize",
        arguments: { clientID: "mitm-tester", adapterID: "aksh" }
    };

    await new Promise((resolve, reject) => {
        ws.onmessage = async (event) => {
            const msg = JSON.parse(event.data);
            console.log(`[DAP Incoming] ${msg.type}: ${msg.event || msg.command || ""}`, JSON.stringify(msg));
            dapFrames.push({ direction: "a2c", message: msg });

            if (msg.type === "event" && msg.event === "output") {
                // Welcome message received — already sent initialize
                console.log("[DAP Client] Got welcome output event");
            } else if (msg.type === "response" && msg.command === "initialize") {
                // Wait for initialized event to send configurationDone
            } else if (msg.type === "event" && msg.event === "initialized") {
                console.log("[DAP Client] Sending configurationDone...");
                const configDone = {
                    seq: dapSeq++,
                    type: "request",
                    command: "configurationDone"
                };
                dapFrames.push({ direction: "c2a", message: configDone });
                ws.send(JSON.stringify(configDone));
            } else if (msg.type === "event" && msg.event === "stopped") {
                currentStep++;
                console.log(`[DAP Client] Runner paused at step ${currentStep}: ${msg.body.description}`);

                // Request scopes
                console.log("[DAP Client] Requesting scopes...");
                const scopesReq = {
                    seq: dapSeq++,
                    type: "request",
                    command: "scopes",
                    arguments: { frameId: 0 }
                };
                dapFrames.push({ direction: "c2a", message: scopesReq });
                ws.send(JSON.stringify(scopesReq));

                // Wait for scopes response to continue
            } else if (msg.type === "response" && msg.command === "scopes") {
                const scopes = msg.body.scopes;
                console.log(`[DAP Client] Scopes retrieved: ${scopes.map(s => s.name).join(", ")}`);

                // Send continue request
                console.log("[DAP Client] Sending continue...");
                const continueReq = {
                    seq: dapSeq++,
                    type: "request",
                    command: "continue",
                    arguments: { threadId: 1 }
                };
                dapFrames.push({ direction: "c2a", message: continueReq });
                ws.send(JSON.stringify(continueReq));
            } else if (msg.type === "event" && msg.event === "terminated") {
                console.log("[DAP Client] Session terminated by adapter.");
                ws.close();
                resolve();
            }
        };

        // Now send the initialize request.
        dapFrames.push({ direction: "c2a", message: initReq });
        ws.send(JSON.stringify(initReq));

        ws.onclose = () => {
            console.log("Debugger client WebSocket closed");
            resolve();
        };

        ws.onerror = (err) => {
            console.error("Debugger client WebSocket error:", err);
            reject(err);
        };
    });

    // 6. Wait for runner process to exit (with timeout)
    console.log("Waiting for runner process to complete...");
    await new Promise((resolve) => {
        const timer = setTimeout(() => {
            console.log("Runner exit timeout — proceeding with cleanup");
            resolve();
        }, 30000);
        runnerProc.on("exit", (code) => {
            clearTimeout(timer);
            console.log(`Runner process exited with code ${code}`);
            resolve();
        });
    });

    // 7. Cleanup and Teardown
    console.log("Teardown and copy captures...");
    killProcess(runnerProc);
    killProcess(mitmProc);
    killProcess(serverProc);
    await sleep(2000);

    // Write DAP frames capture file
    fs.writeFileSync(
        path.join(CAPTURES_DIR, backend, "dap_frames.json"),
        JSON.stringify(dapFrames, null, 2)
    );

    // Copy flows.jsonl and runner.log to final location
    if (fs.existsSync(path.join(tempCaptureDir, "flows.jsonl"))) {
        fs.copyFileSync(
            path.join(tempCaptureDir, "flows.jsonl"),
            path.join(CAPTURES_DIR, backend, "flows.jsonl")
        );
        console.log(`Copied flows.jsonl to ${path.join(CAPTURES_DIR, backend, "flows.jsonl")}`);
    } else {
        console.warn("WARNING: flows.jsonl not found in temp capture dir!");
    }

    if (fs.existsSync(path.join(tempCaptureDir, "runner.log"))) {
        fs.copyFileSync(
            path.join(tempCaptureDir, "runner.log"),
            path.join(CAPTURES_DIR, backend, "runner.log")
        );
    }

    // Clean temp dirs
    fs.rmSync(tempStateDir, { recursive: true, force: true });
    fs.rmSync(tempCaptureDir, { recursive: true, force: true });

    console.log(`Completed conformance recording for ${backend}`);
}

async function main() {
    try {
        await runScenario("official");
        await runScenario("aksh");
        console.log("\n==================================================");
        console.log("DAP E2E Conformance Recording Complete!");
        console.log("Captures saved in .runner-watch/dap-captures/");
        console.log("==================================================");
    } catch (e) {
        console.error("E2E recording failed:", e);
        process.exit(1);
    }
}

main();
