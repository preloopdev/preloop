"""Host allowlist matching shared by the capture addon and offline tools.

Kept free of mitmproxy imports so corpus tooling runs without it.
"""


def parse_allowlist(raw: str) -> list[str]:
    """Comma-separated hosts; a leading dot (`.github.com`) matches the domain
    and its subdomains. Empty means "all hosts"."""
    return [h.strip().lower() for h in raw.split(",") if h.strip()]


def host_selected(host: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    host = host.lower()
    for entry in allowlist:
        if entry.startswith("."):
            if host == entry[1:] or host.endswith(entry):
                return True
        elif host == entry:
            return True
    return False
