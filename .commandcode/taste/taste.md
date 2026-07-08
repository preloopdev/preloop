# Taste (Continuously Learned by [CommandCode][cmd])

[cmd]: https://commandcode.ai/

# benchmarking
- After implementing performance/memory fixes, always run comprehensive benchmarks to validate the improvement. Confidence: 0.70

# research
- When researching how a feature/protocol works, look up the official upstream source code directly (e.g., actions/runner on GitHub) rather than guessing or relying on secondary documentation. Confidence: 0.70

# testing
- For protocol-related work (e.g., DAP, GHA runner protocols), validate against real MITM recordings/golden fixtures of the official runner rather than relying solely on source code reading. Confidence: 0.80
- Achieve 100% wire-compatibility with the official runner before claiming completion; use golden-fixture comparison (delta.json/conformance-report) to verify no disparities. Confidence: 0.85
- When the user says "DO NOT TAKE SHORTCUTS", they mean it literally — always record against the real runner/github and compare outputs for deviations. Confidence: 0.80
- When porting a C# feature, build synthetic tests derived from source code/specs first, then do live MITM recording as validation. Confidence: 0.70
