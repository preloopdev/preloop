#!/usr/bin/env bash
set -euo pipefail

# gen-sandbox-proto.sh — regenerate crates/preloop-sandbox/src/gen from
# proto/preloop/sandbox/v1/sandbox.proto.
#
# Two protoc plugins are needed (local tools, not build dependencies):
#
#   cargo install --locked --root <prefix> protoc-gen-buffa@0.9.2
#   cargo install --locked --root <prefix> connectrpc-codegen@0.9.0
#
# Both install a binary under <prefix>/bin: `protoc-gen-buffa` and
# `protoc-gen-connect-rust`. Put that directory on PATH, or point
# CONNECTRPC_TOOLS at it. protoc itself is not pinned; 36.x generated the
# checked-in output.
#
# The checked-in output is:
#   src/gen/preloop.sandbox.v1.rs          buffa messages (owned + views)
#   src/gen/preloop.sandbox.v1.connect.rs  service traits, clients, specs
#
# Both plugins are run with `file_per_package` so one file per package lands
# in a predictable place; `buffa_module` makes the service stubs reference the
# message types through their real module path.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/crates/preloop-sandbox/src/gen"
PROTO="$ROOT/proto/preloop/sandbox/v1/sandbox.proto"

PROTOC="${PROTOC:-protoc}"
if [[ -n "${CONNECTRPC_TOOLS:-}" ]]; then
    PATH="$CONNECTRPC_TOOLS/bin:$PATH"
fi

for tool in protoc-gen-buffa protoc-gen-connect-rust; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "missing $tool on PATH (install with cargo install --locked --root <prefix> ...)" >&2
        exit 1
    }
done

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT
mkdir -p "$OUT/messages" "$OUT/services"

"$PROTOC" -I "$ROOT/proto" \
    --buffa_out="$OUT/messages" \
    --buffa_opt=file_per_package=true,views=true,json=true \
    --connect-rust_out="$OUT/services" \
    --connect-rust_opt=buffa_module=crate::generated,file_per_package \
    "$PROTO"

mkdir -p "$DEST"
cp "$OUT/messages/preloop.sandbox.v1.rs" "$DEST/preloop.sandbox.v1.rs"
cp "$OUT/services/preloop.sandbox.v1.rs" "$DEST/preloop.sandbox.v1.connect.rs"

# connectrpc-codegen 0.9.0 calls the `Register` RPC as `svc.register(ctx, req)`
# inside the generated registration glue. The service has an RPC named
# `Register`, and the generated `ProvidersExt` trait also has a `register`
# method, so the call resolves to the wrong one and the generated file does not
# compile. Qualify the two call sites. Drop this once upstream disambiguates.
perl -pi -e 's/svc\.register\(ctx, (s?req)\)/Providers::register(svc.as_ref(), ctx, $1)/g' \
    "$DEST/preloop.sandbox.v1.connect.rs"
echo "regenerated $DEST"
