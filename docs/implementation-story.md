# How We're Building aksh — An Implementation Story

This document tells the story of how aksh goes from scaffold to faithful
control plane, one commit at a time. Each chapter explains **why** we're
building something, **what** we're building, and **how** it fits into
the larger picture.

---

## Chapter 1: The Protocol Is the Truth

The first thing we had to accept: we're not inventing a protocol. We're
learning one. The GitHub Actions runner protocol is a living contract
between the `actions/runner` binary (the client) and whatever server it
connects to. That contract exists in three places simultaneously:

1. **The runner source code** (`github.com/actions/runner`, C#) — the
   client side. Every HTTP call, every DTO, every crypto operation is
   right there.
2. **`runner.server`** (`github.com/ChristopherHX/runner.server`, C#) —
   the community server implementation. It uses GitHub's actual
   `DistributedTask.WebApi` SDKs, so its DTOs define the wire format.
3. **Network captures** — running the real runner against the real server
   and recording every byte on the wire.

We chose not to guess. We read the source code. We cross-referenced both
sides. And we built DTOs that match exactly.

### Why AzDO DTOs, Not Ours?

The old `RunnerJobMessage` in aksh was a native Rust struct with fields
we *thought* the runner needed. But the runner doesn't speak Rust
structs — it speaks JSON with specific field names from
`GitHub.DistributedTask.WebApi`. A field named `job` instead of `plan`,
or `status` instead of `result`, and the runner will reject the message
or silently misbehave.

The wire DTOs live in `aksh-gha-protocol::azdo`. They model the exact
JSON shapes, with `camelCase` field names matching the C# properties,
and `serde` attributes that produce/accept the exact upstream format.

### What We Built

The `azdo` module contains every DTO the runner protocol touches:

- **Runner lifecycle**: `ConnectionData`, `LocationServiceData`,
  `TaskAgent`, `TaskAgentSession`, `EncryptionKey`
- **Message queue**: `TaskAgentMessage` (the encrypted envelope)
- **Job message**: `AgentJobRequestMessage` (the full payload), with
  nested `TaskStep`, `TaskReference`, `ActionsDownloadInfo`
- **Timeline**: `TimelineRecord`, `Issue`, `TimelineRecordState`,
  `TaskResult`
- **Variables**: `VariableValue` (with `isSecret`), `MaskHint`
- **Resources**: `TaskResources`, `ServiceEndpoint`,
  `EndpointAuthorization`
- **Context**: `PipelineContextData` (the union type)
- **Completion**: `JobCompletedEvent`, `LogReference`

Each type is round-trip tested: serialize → deserialize → assert equality.
The test suite catches casing mistakes, missing fields, and wrong types
before they can reach the wire.

### The Tradeoff

We could have made the DTOs more "Rust-like" — enums instead of strings,
newtype wrappers instead of raw UUIDs. We didn't. The wire format is
the spec, and the spec uses strings and raw numbers. We add Rust
ergonomics *on top* of the wire types, not instead of them. The `azdo`
module is deliberately thin: serde attributes and struct definitions, no
business logic. Business logic lives in the server crate and operates on
these types.

---

## Chapter 2: The Encrypted Session

The runner protocol isn't HTTP-with-API-keys. It's an encrypted session:

1. Runner registers → sends RSA public key
2. Server generates AES key → RSA-wraps it → returns in `TaskAgentSession`
3. Runner decrypts the AES key with its private key
4. Every subsequent `TaskAgentMessage` has an AES-encrypted `body`
5. Runner decrypts with the session AES key + per-message `iv`

This is why `ConnectionData` and `TaskAgentSession` had to come first.
Without the service-location map, the runner can't find the server. Without
the session key exchange, the runner can't decrypt its first job.

The `EncryptionKey` type models this exactly:
```rust
pub struct EncryptionKey {
    pub value: Vec<u8>,      // raw or RSA-wrapped key bytes
    pub encrypted: bool,     // true = RSA-wrapped, false = plaintext
}
```

The crypto implementation itself (`RSA-OAEP + AES-CBC`) lives in a
separate `protocol::crypto` module (planned for Phase C). The DTOs in
`azdo` just carry the data — they don't do crypto. This separation means
we can test DTO shapes independently of crypto correctness.

---

## Chapter 3: The Message Queue

The message queue is the control plane's heartbeat. The runner long-polls
`GET /messages?sessionId=X&lastMessageId=Y` and waits. When a job is ready,
the server pushes an encrypted `TaskAgentMessage`. The runner decrypts it,
processes it, then `DELETE`s the message to acknowledge.

Three things had to be right:

1. **The message type** — `"PipelineAgentJobRequest"` tells the runner
   the body is a full job. The runner ignores unknown types.
2. **The body encoding** — base64-encoded ciphertext. The `iv` field is
   the AES initialization vector.
3. **The ack protocol** — the runner `DELETE`s the message by ID. Until
   it does, the server considers the message undelivered and will
   redeliver.

The old aksh implementation returned plaintext JSON from a FIFO queue.
The new `TaskAgentMessage` DTO models the encrypted envelope that the
runner actually expects.

---

## Chapter 4: The Job Message — Where Everything Comes Together

`AgentJobRequestMessage` is the most complex type in the protocol. It's
what the runner receives after decryption, and it contains everything
needed to execute a job:

- **Plan reference** — which run and job this belongs to
- **Steps** — the ordered list of things to execute
- **Variables** — env vars, system vars, secrets (with `isSecret` flags)
- **Mask hints** — what to redact in log output
- **Resources** — service endpoints (including `SystemVssConnection`
  with the OAuth token for API calls back to the server)
- **Context data** — `github`, `env`, `vars`, `matrix`, `strategy`,
  `needs`, `secrets` — all the context objects that `${{ }}` expressions
  can reference
- **Actions download info** — how to fetch each action's source code

Building this DTO was the most research-intensive step. We read the
runner's `JobDispatcher.cs` to see which fields it accesses, and the
server's `MessageController.cs` to see which fields it populates. Fields
the runner uses but the server doesn't populate are still in the DTO —
they'll be `None`/empty until we wire the evaluator.

---

## Chapter 5: Timeline Records — How Status Flows Back

The runner doesn't just "complete" a job. It streams status back through
`TimelineRecord` updates:

1. Create records for the job and each step (state: `pending`)
2. As steps start → PATCH state to `inProgress`
3. As steps finish → PATCH state to `completed` with `result`
4. Attach `Issue` entries for annotations (`::error::`, `::warning::`)
5. Report job completion via `JobCompletedEvent`

The `TimelineRecord` DTO models every field the runner sends:
`id`, `parentId`, `name`, `state`, `result`, `issues`, `variables`,
`startTime`, `finishTime`, `percentComplete`. The server stores these
and projects them into the NDJSON agent feed.

This is where aksh's "projections" philosophy pays off: the timeline
records are the source of truth, and the NDJSON feed is a read model
derived from them.

---

## Chapter 6: The Discovery Handshake

The runner's very first HTTP call is `GET /_apis/connectionData`. It's
not authenticated — it's the one anonymous endpoint. The response tells
the runner *where to find everything else*.

### Why a GUID Map?

The Azure DevOps protocol uses GUIDs to identify services, not URL paths.
Each service definition has a GUID and a URL template:

```json
{
  "identifier": "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7",
  "locationMapping": {"": "/_apis/v1/Message/{poolId}/{messageId}"},
  "displayName": "Message"
}
```

The runner indexes these GUIDs and uses them to construct API calls.
There are 18 of them — one for each subsystem: agent pools, sessions,
messages, timelines, logs, artifacts, cache, and more.

We copied the exact GUIDs and URL templates from `runner.server`'s
`ConnectionDataController.cs`. These are stable across runner versions —
the GUIDs are part of the protocol contract, not the implementation.

### The OAuth Dance

After discovering endpoints, the runner authenticates. The protocol uses
OAuth2 client-credentials flow: the runner sends a `POST` to the token
endpoint with its registration credentials, and receives a bearer token.
Every subsequent call includes `Authorization: Bearer <token>`.

For the initial implementation, the token endpoint accepts any credentials
and issues a UUID-based bearer token. Token validation, signing, and
expiry will come in later phases. The important thing right now is that
the runner can complete the handshake and reach the message queue.

### The Tradeoff

We could have skipped `connectionData` entirely and hard-coded the URL
paths. But then the runner would fail if it ever changes its discovery
logic (and it has — the broker migration path in `MessageListener.cs`
shows this). By faithfully implementing the discovery handshake, we
future-proof against runner protocol evolution.

---

## Chapter 7: The Encrypted Session

The runner protocol doesn't trust HTTP. Every message body is encrypted
with AES-256-CBC, and the AES key is RSA-wrapped before being sent to
the runner. This isn't optional — without it, the runner won't enter
the message loop.

### The Handshake

1. Runner sends its RSA public key during registration
2. Server generates a random 32-byte AES key
3. Server RSA-OAEP wraps the AES key with the runner's public key
4. Server returns `TaskAgentSession { encryptionKey: { value: <wrapped>, encrypted: true } }`
5. Runner decrypts the AES key with its private key
6. Every subsequent `TaskAgentMessage` has an AES-encrypted `body` + random `iv`
7. Runner decrypts: `AES-CBC(key, iv).decrypt(body)`

### The Implementation

The `crypto` module in `aksh-gha-protocol` handles the math:

- `AgentRsaKeypair::generate()` — 2048-bit RSA keypair
- `wrap_key(plaintext)` — RSA-OAEP-SHA1 encrypt
- `SessionEncryption::generate()` — random AES-256 key
- `encrypt(plaintext)` — AES-256-CBC + PKCS#7 pad, returns `(ciphertext, iv)`
- `decrypt(ciphertext, iv)` — reverse

The server generates one RSA keypair at startup (per-process). Each
session gets a fresh AES key. The AES key lives in `InnerState.session_keys`
until the session is destroyed.

### The Tradeoff

We could have used RSA-OAEP-SHA256 (FIPS mode). We didn't — the runner
defaults to SHA-1, and the upstream `runner.server` uses SHA-1. Matching
the default avoids the FIPS flag complexity. If someone needs FIPS, they
can swap the hash in one line.

---

## Chapter 8: The Evaluator — Where Expressions Meet the Wire

This is the hardest chapter. The expression engine existed but was
orphaned — no code called it. The parser existed but didn't resolve
`${{ }}`. The protocol DTOs existed but nothing built them. Phase D
connects all three.

### The Pipeline

1. **Parse** the workflow YAML → `Workflow` struct (already done)
2. **Expand** jobs with matrix → `Vec<JobPlan>` (already done)
3. **Evaluate** expressions in each field → resolved strings
4. **Build** `AgentJobRequestMessage` with resolved data
5. **Encrypt** the message body with the session's AES key
6. **Deliver** as `TaskAgentMessage` to the runner

### Step 3: Expression Resolution

The `eval` module in the parser wires `aksh-gha-expressions` to resolve
`${{ }}` in string fields. A key design decision: we resolve expressions
in fields the *server* owns (env, with, run, runs-on) but emit raw
expression strings for `if` conditions. The runner evaluates `if`
conditions itself — it needs to see the original expression to evaluate
it in the context of actual job status.

### Step 4: The Job Message Builder

`AgentJobRequestMessage` is the most complex DTO. The builder takes a
`JobPlan` + context data and produces:

- Resolved steps with evaluated `env`, `with`, `run`
- `contextData` map: `github`, `env`, `vars`, `matrix`, `strategy`,
  `needs`, `secrets`
- `variables` with `VariableValue` (value + isSecret flag)
- `maskHints` for every secret value
- `SystemVssConnection` service endpoint
- Timeline and job UUIDs

### Step 5: Encryption

The runner expects every message body to be AES-encrypted. The server
serializes the `AgentJobRequestMessage` to JSON, encrypts with the
session's AES key + a random IV, and base64-encodes the ciphertext.
The runner decrypts with the same key (unwrapped from RSA during
session creation).

### The Tradeoff

We could have skipped encryption for local use — the runner would
still work. But then you'd have two code paths: encrypted for
production, plaintext for local. That's a bug waiting to happen.
Encrypt always. The overhead is negligible (one AES op per message).

---

## Chapter 9: What Comes Next

With the runner able to receive and decrypt job messages, the next
chapters will cover:

- **Phase E**: Timeline and logs (status flowing back from the runner)
- **Phase F**: The needs DAG (multi-job scheduling)
- **Phase G**: Trigger and matrix fidelity
- **Phase H**: Actions, cache, artifacts
