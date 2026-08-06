# [smolvm] A published control socket can resolve to a stale node on the guest when its path sits inside a `--volume` mount

## Summary

A Unix control socket published into a VM via `--mount-socket` can silently
resolve to a *stale zero-byte file* (or to a dead VM's leftover socket node)
inside the guest, making the control plane permanently unreachable from the
guest side — `ECONNREFUSED` on every connect — even though the socket on the
host is live and correct. Affected VMs appear "born dead": they boot and run
normally, but can never be driven over their control socket until the host
deletes and re-creates them.

## Environment

- macOS host (arm64), smolvm fork HEAD `0470928`
- Guest: Linux VM (krun)
- Machine mounts:
  - `--mount-socket <host>/control.sock:/run/control/engine.sock`
  - `--volume <host>/shared-control:/run/control` (virtiofs tag, shared
    across many VMs)

## How it triggers (the use case)

Hosted CI runner pools: the control plane on the host publishes a socket into
a *shared* directory, and every VM in the pool mounts that directory via
`--volume` while also receiving the control socket via `--mount-socket` at a
path inside it. The guest agent then binds its own listener at that path
(`serve_mount` in `crates/smolvm-agent/src/publish_socket.rs`, which does
`remove_file(path); UnixListener::bind(path)`), landing inside the shared host
directory — so every VM binds the *same* path into the *same* shared
directory, and the node the guest actually resolves depends on boot/mount
ordering rather than on which VM owns it.

## Reproduction

The commands assume `$VM` is a running smolvm machine whose control socket is
published at a path inside a `--volume`-mounted shared directory
(`<host>/control.sock` and `<host>/shared-control` above).

### 1. The host sees a regular file where the guest sees a socket — same inode

Host side:

```sh
ls -l <host>/shared-control/engine.sock
stat -f '%i %HT %Sp %z' <host>/shared-control/engine.sock
```

Observed:

```text
-rw-------  1 user  staff  0 Aug  4 12:00 engine.sock
63332420 Regular File -rw------- 0
```

Guest side:

```sh
smolvm machine exec --name "$VM" -- ls -l /run/control/engine.sock
smolvm machine exec --name "$VM" -- stat -c '%i %F %a' /run/control/engine.sock
```

Observed:

```text
srwxrwxrwx 1 root root 0 ... /run/control/engine.sock
63332420 socket 777
```

Same inode number (`63332420`) on both sides, but virtiofs presents the node
as a regular 0-byte file to the host and a socket to the guest. The guest path
resolves to the host's shared-directory entry, not to a private node.

### 2. The guest's listener has no owning fd in its pid-ns

```sh
smolvm machine exec --name "$VM" -- cat /proc/net/unix | grep engine.sock
```

Observed:

```text
0000000000000000: 00000003 00010000 00010000 0001 01 63332420 12345 /run/control/engine.sock
```

`St=01` (LISTEN), but the owning socket has no fd in this VM's pid-ns — the
listener was bound by a *different* machine's agent sharing the directory.

### 3. The VM's own listener is alive elsewhere — only the path is wrong

A direct connect over the machine's vsock leg (CID 2, port 6100) succeeds,
proving the VM's own `serve_mount` listener is up:

```text
connect 2:6100 -> OK
```

while connecting to the guest path fails:

```sh
smolvm machine exec --name "$VM" -- socat - UNIX-CONNECT:/run/control/engine.sock
```

Observed:

```text
2026/08/04 ... ECONNREFUSED
```

### 4. mountinfo: the control directory is stacked, and the stack changes per boot

```sh
smolvm machine exec --name "$VM" -- cat /proc/self/mountinfo | grep " /run/control "
```

Observed: multiple stacked entries — virtiofs → tmpfs `/run` shadow → virtiofs
re-mount — and the layer order changes per keep-alive failure / fresh
container, i.e. the mount-order race is per boot.

### 5. The shared node churns while machines boot; freezes when idle

Host-side watch:

```sh
while true; do stat -f '%Sm %N' <host>/shared-control/engine.sock; sleep 1; done
```

Observed: the node is rewritten every ~5s while machines churn (each booting
machine's `serve_mount` re-binds the same path), and frozen when the pool is
idle.

### 6. Repro at scale: fork N machines into the same shared directory

Fork N machines from one base image with the mounts above, let the pool churn,
then attempt the control-plane connect on each:

- The machine that won the mount-order race connects normally.
- The rest get `ECONNREFUSED` on every connect, forever, until the host deletes
  and re-creates them. In a pool that forks many VMs from one base, a majority
  of freshly forked machines can be born dead per cycle.

## How we narrowed it down

1. Started from the symptom: freshly forked machines failing the control-plane
   handshake with `ECONNREFUSED`, while their TCP bridge to the host worked.
2. First hypotheses were guest-side (kernel/launcher args) — several were
   tried and none explained the pattern: some machines born dead, others fine,
   seemingly at random.
3. Compared host vs guest views of the same socket node: identical inode
   (`63332420`), but host sees a regular 0-byte file (`0600`) and the guest
   sees a socket (`0777`). The inode equality proved the guest path resolves
   to the host's shared-directory entry, not to a private node.
4. Checked `/proc/net/unix` inside the guest: the path shows LISTEN but with no
   owning fd in this VM's pid-ns — the listener belongs to another machine
   sharing the directory.
5. Probed the VM's own listener over vsock: connect OK. The listener was alive;
   only the path resolution was wrong.
6. Read `/proc/self/mountinfo`: the directory is virtiofs → tmpfs shadow →
   virtiofs re-mount, and the layer order changes per boot — the per-boot race
   that picks which node every guest sees.
7. Watched the shared node's mtime during pool churn: rewritten every ~5s,
   frozen when idle — consistent with every machine's `serve_mount` binding
   the same path into the shared host directory.

## Expected behavior

Each VM's published control socket should be reachable at its guest path
regardless of how many VMs share the directory. The listener should be scoped
to the VM that published it, and a boot/mount-order race should not make some
VMs permanently unreachable. (For comparison, the SSH-agent path injects its
socket into the container explicitly rather than binding into a shared
directory.)
