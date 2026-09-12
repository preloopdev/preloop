#!/usr/bin/env bash
# Let AgentENV sandboxes reach exactly one host address: the preloop engine.
#
# AgentENV denies every RFC1918/CGNAT/loopback/link-local destination inside
# each sandbox netns (`AGENTENV-EGRESS`), so a guest can reach the public
# internet but never a private host — including the control plane the preloop
# runner has to register with. `[network.egress].always_denied_cidrs` is the
# node-level lever, and it is a deny list evaluated BEFORE per-sandbox
# allowOut, so the only way to permit one address is to stop denying the CIDR
# that covers it.
#
# Rather than dropping 192.168.0.0/16 wholesale (which would open the entire
# LAN to every sandbox), the covering range is replaced by its exact
# complement around the engine's /32. Sandboxes keep their isolation from
# every other private address.
set -euo pipefail
CONFIG=/var/lib/aenv/config/config.toml
HOSTIP="${1:?engine host address, as the guest must reach it}"
# Prime the sudo timestamp once. `sudo -S` reads the password from stdin, so a
# later `sudo python3 -` with a heredoc would swallow the password line as
# program input.
echo "${SUDO_PASS:-}" | sudo -S -p '' true
SUDO() { sudo -n "$@"; }
SUDO cp "$CONFIG" "$CONFIG.bak.$(date +%s)"
SUDO python3 - "$CONFIG" "$HOSTIP" <<'PY'
import ipaddress, re, sys

config_path, host = sys.argv[1], sys.argv[2]
covering = ipaddress.ip_network("192.168.0.0/16")
allowed = ipaddress.ip_network(f"{host}/32")
complement = sorted(covering.address_exclude(allowed),
                    key=lambda n: (int(n.network_address), n.prefixlen))

text = open(config_path).read()
block = re.search(r"always_denied_cidrs = \[(.*?)\]", text, re.S)
if not block:
    sys.exit("always_denied_cidrs not found")
kept = []
for line in block.group(1).splitlines():
    cidr = line.strip().strip('",')
    if not cidr:
        continue
    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        kept.append(cidr)
        continue
    if not network.subnet_of(covering):
        kept.append(cidr)
entries = kept + [str(net) for net in complement]
rendered = "always_denied_cidrs = [\n" + "".join(f'  "{e}",\n' for e in entries) + "]"
open(config_path, "w").write(text[:block.start()] + rendered + text[block.end():])
print(f"denied {len(entries)} ranges; {host}/32 is now reachable")
PY

# The node config governs the per-sandbox netns floor. The host's own INPUT
# chain is a second gate: AgentENV inserts
#   -A INPUT -s <internal pool> -i veth-+ -j REJECT
# so a guest cannot open a new connection to the host. Allow exactly the
# engine port from the internal pools.
#
# Order matters and the insert must happen AFTER the service starts: AgentENV
# re-inserts its REJECT at the head of INPUT on every start, which would
# shadow an allow rule added earlier.
allow_engine_port() {
  local port="${1:?port}"
  local rule
  for pool in 10.11.0.0/16 10.12.0.0/16; do
    rule=(
      -s "$pool"
      -p tcp
      --dport "$port"
      -j ACCEPT
    )
    # AgentENV inserts its blanket veth REJECT after the service restart. Move
    # the narrow engine exception to the head so the REJECT cannot shadow it.
    while SUDO iptables -C INPUT "${rule[@]}" 2>/dev/null; do
      SUDO iptables -D INPUT "${rule[@]}"
    done
    SUDO iptables -I INPUT 1 "${rule[@]}"
  done
}

SUDO systemctl restart aenv
for _ in $(seq 1 10); do
  sleep 1
  systemctl is-active aenv >/dev/null
  allow_engine_port "${2:-9490}"
done
SUDO iptables -S INPUT | head -3
