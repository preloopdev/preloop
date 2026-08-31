from __future__ import annotations
import importlib.util
import contextlib
import io
from pathlib import Path
import unittest
ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location("virtualization_bench", ROOT / "bench.py")
assert spec and spec.loader
bench = importlib.util.module_from_spec(spec); spec.loader.exec_module(bench)
class ManifestTests(unittest.TestCase):
    def setUp(self): self.manifest = bench.load_manifest(ROOT / "campaign.example.toml")
    def test_example_is_valid_and_hash_is_stable(self):
        self.assertEqual(bench.validate_manifest(self.manifest), [])
        self.assertEqual(len(bench.canonical_manifest_hash(self.manifest)), 64)
        self.assertEqual(bench.canonical_manifest_hash(self.manifest), bench.canonical_manifest_hash(dict(self.manifest)))
    def test_port_mutation_is_rejected(self):
        m=dict(self.manifest); m["isolation"]=dict(m["isolation"]); m["isolation"]["benchmark_ports"]=[9090]
        self.assertTrue(any("port" in e for e in bench.validate_manifest(m)))
    def test_production_state_mutation_is_rejected(self):
        m=dict(self.manifest); m["isolation"]=dict(m["isolation"]); m["isolation"]["state_dir"]="/var/lib/preloop/state"
        self.assertTrue(any("production path" in e for e in bench.validate_manifest(m)))
    def test_memory_matrix_over_ceiling_is_rejected(self):
        m=dict(self.manifest); m["resources"]={"too_large":{"memory_mib":19*1024}}
        self.assertTrue(any("18 GiB" in e for e in bench.validate_manifest(m)))
    def test_hash_changes_on_mutation(self):
        m=dict(self.manifest); m["operator"]="different-operator"
        self.assertNotEqual(bench.canonical_manifest_hash(self.manifest), bench.canonical_manifest_hash(m))

    def test_catalog_freezes_five_representative_workflows(self):
        catalog = bench.tomllib.loads(
            (ROOT / "workloads" / "catalog.toml").read_text(encoding="utf-8")
        )
        workflows = catalog["real_workflows"]
        self.assertEqual(
            set(workflows),
            {"ripgrep", "flask", "vite", "chi", "testcontainers_go"},
        )
        self.assertTrue(all(len(row["commit"]) == 40 for row in workflows.values()))
        self.assertTrue(all(row["workflow"].startswith(".github/workflows/")
                            for row in workflows.values()))

    def test_template_supply_is_rejected_until_digests_are_frozen(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(bench._verify_supply(self.manifest), 2)
if __name__ == "__main__": unittest.main()
