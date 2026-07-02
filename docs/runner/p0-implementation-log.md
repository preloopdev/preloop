# P0 Implementation Log — 2026-07-02

Tracks each unit of work implementing P0 blockers from `roadmap.md §1`.

## Conformance protocol

For each unit, conformance is verified by:
1. **Golden wire-shape diff** — compare implementation against `.runner-watch/golden/v2.335.1/` MITM captures
2. **Unit tests** — `cargo test -p aksh-runner --quiet` (and `-p aksh-gha-expressions` for F027)
3. **Workspace build** — `cargo test --workspace --quiet`
4. **Clippy + fmt** — `cargo clippy --workspace --all-targets` + `cargo fmt --all --check`
5. **aksh secondary** — `just serve` + manual smoke where applicable

No Tier-1 (live GitHub) gate exists yet (H1/H2 harness unbuilt). Golden captures serve as the primary conformance baseline. Each commit notes what was verified.

---

## Units of work

(Entries added as each unit is implemented)
