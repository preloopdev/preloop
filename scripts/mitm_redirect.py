import sys
import os
from pathlib import Path
from mitmproxy import http

# Add standard capture addon path
sys.path.append("/Users/bnjoroge/mitm-proxy/experiments/mitm/addons")
from capture import Capture

class RedirectAndCapture(Capture):
    def request(self, flow: http.HTTPFlow):
        print(f"[mitm_redirect] Request to {flow.request.host}:{flow.request.port}{flow.request.path}", file=sys.stderr, flush=True)
        # Redirect loopback or local network IP (default port 80 or port 9090/5000) to our test server on 127.0.0.1:9090
        # by updating flow.request.host/port and setting the Host header explicitly to match the configured GHES URL.
        if flow.request.host in ("127.0.0.1", "localhost", "192.168.1.221"):
            original_host = flow.request.host
            flow.request.host = "127.0.0.1"
            flow.request.port = 9090
            flow.request.headers["Host"] = f"{original_host}:9090"
            print(f"[mitm_redirect] Redirected. Target: 127.0.0.1:9090. Host header: {flow.request.headers.get('Host')}", file=sys.stderr, flush=True)
        
        # Call base capture logic
        super().request(flow)

addons = [RedirectAndCapture()]
