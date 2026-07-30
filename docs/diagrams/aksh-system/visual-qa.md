# Visual QA report

## Scope

Reviewed all ten rendered PNG previews against their editable Excalidraw sources and the requested visual/semantic rules. Rendering used the Excalidraw skill’s Playwright renderer. Each scene is 1600 × 1000 logical pixels; PNG output is 6000 × 3732 for high-resolution inspection.

## Validation passes

### Pass 1 — whole-suite composition

- Confirmed warm off-white canvas, charcoal outlines, blue primary accent, red failure color, rounded containers, consistent 2 px structural strokes, and a shared bottom legend.
- Confirmed each file teaches one mechanism rather than combining unrelated flows.
- Found routed arrows interpreted as point offsets rather than cumulative segments in several scenes. Symptoms: diagonal failure lines, labels crossing containers, and arrows ending before their intended targets.

### Pass 2 — route and spacing corrections

- Re-routed multi-segment arrows in system map, scheduler, runner, step engine, results, security, VM, conformance, and DAP scenes.
- Moved arrow captions out of process labels and evidence blocks.
- Reduced dense evidence text to 14–16 px where necessary while retaining 18–20 px section hierarchy and 36 px titles.
- Expanded the system-map YAML evidence block so `runs-on: ubuntu-latest` is fully visible.
- Routed security and failure paths around the central evidence blocks rather than through them.

### Pass 3 — editorial cleanup

- Removed a redundant compiler failure caption; the red route and labeled error diamond already carry the meaning.
- Moved runner crash handling below the process-boundary evidence instead of crossing worker-exit text.
- Moved OIDC/JWKS verification below the secret block.
- Routed conformance mismatch around the pass state rather than through it.
- Separated DAP `framed request` and `protocol reject` captions from transport-detection evidence.

## Final checks

| Check | Result |
|---|---|
| One mechanism per diagram | Pass — ten separate scenes |
| Editable source | Pass — one `.excalidraw` file per scene |
| SVG preview | Pass — one accessible SVG per scene with `<title>` and `<desc>` |
| PNG preview | Pass — rendered and visually inspected |
| Warm off-white background | Pass — `#FAF7F0` |
| Dark charcoal outlines | Pass — `#2D2A26` |
| Single primary accent | Pass — blue `#2563EB` |
| Warning/failure color | Pass — red `#C2413A` |
| Color-independent semantics | Pass — arrows and labels state request/background/failure meaning |
| Solid requests | Pass |
| Dashed background work | Pass |
| Red failure/failover | Pass |
| Grouping of clients/services/storage/control plane | Pass — used where relevant rather than forcing empty groups |
| Important transitions labeled | Pass |
| Evidence artifacts | Pass — YAML, JSON/NDJSON, filesystem layout, endpoint and message names |
| Text clipping at final export | Pass at full-resolution SVG/PNG |
| Unintended element overlap | Pass after three review rounds |
| Arrow endpoints | Pass after routed-arrow correction |
| Consistent legend | Pass — repeated on every scene for standalone use |
| Readability | Pass — intended for 1600 px-wide viewing; SVG remains lossless when zoomed |

## Known presentation constraints

- Monospace typography is intentional: it keeps endpoint names, message types, YAML, and state fields visually consistent.
- Shadows were omitted rather than simulated with duplicate shapes; hierarchy comes from whitespace, stroke weight, and the restrained blue panel fill. This avoids non-semantic decoration and keeps Excalidraw editing clean.
- The Cloudflare URL is a temporary preview deployment, not a durable production domain; see `brief.md` assumption 9.
