from __future__ import annotations
import importlib.util, json, tempfile, unittest
from pathlib import Path
P=Path(__file__).parents[1]/"analyze.py"
spec=importlib.util.spec_from_file_location("virtualization_analyze",P); assert spec and spec.loader
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

class AnalysisTests(unittest.TestCase):
 def _event(self, arm, repetition=0, **kwargs):
  event={"schema_version":1,"manifest_sha256":"m","campaign":"c","phase":"workload",
         "arm":arm,"repetition":repetition,"monotonic_ns":repetition+1,
         "timestamp_utc":"x","event_kind":"sample","success":True,
         "failure_class":"","diagnostic":"","values":{},"units":{},
         "workload":"noop","resource_profile":"tiny","cache_class":"snapshot-warm"}
  event.update(kwargs)
  return event

 def test_hand_computed_percentiles_and_mad(self):
  self.assertEqual(m.percentile([1,2,3,4,5],50),3)
  self.assertEqual(m.percentile([1,2,3,4,5],90),4.6)
  self.assertEqual(m.mad([1,2,3,4,5]),1)
 def test_gate_failure_never_scores_or_wins(self):
  events=[]
  for arm,ok in (("smolvm",False),("hypeman-fc",True)):
   events.append({"schema_version":1,"manifest_sha256":"m","campaign":"c","phase":"isolation","arm":arm,"repetition":0,"monotonic_ns":1,"timestamp_utc":"x","event_kind":"isolation_gate","success":ok,"failure_class":"" if ok else "isolation_failure","diagnostic":"","values":{},"units":{}})
  summary,_=m.analyze_events(events)
  self.assertIsNone(summary["scores"]["trusted_single_tenant"]["smolvm"])
  self.assertEqual(summary["decisions"]["trusted_single_tenant"],"no eligible arm")

 def test_decision_requires_all_gates_and_primary_ci(self):
  events=[]
  for arm,throughput,warm in (("smolvm",100,100),("hypeman-fc",110,89)):
   for kind in ("correctness","isolation","reliability","capacity","safety"):
    values={"isolation_posture":"multi_tenant_eligible"} if kind=="isolation" else {}
    events.append(self._event(arm,event_kind=kind+"_gate",values=values))
   for repetition in range(3):
    events.append(self._event(
     arm,repetition,values={"throughput_jobs_per_second":throughput,
      "warm_p95_ms":warm,"cold_p95":warm+10,"density":1,
      "reliability":1,"operational":1},
     cache_class="snapshot-warm"))
  summary,_=m.analyze_events(events)
  self.assertEqual(summary["gates"]["hypeman-fc"]["eligible"],True)
  self.assertEqual(summary["decisions"]["trusted_single_tenant"],"hypeman-fc")
  self.assertEqual(summary["decisions"]["untrusted_multi_tenant"],"hypeman-fc")
  self.assertEqual(summary["primary_comparisons"]["throughput"]["n"],3)

 def test_missing_primary_comparison_is_not_a_performance_win(self):
  events=[]
  for arm in ("smolvm","hypeman-fc"):
   for kind in ("correctness","isolation","reliability","capacity","safety"):
    values={"isolation_posture":"multi_tenant_eligible"} if kind=="isolation" else {}
    events.append(self._event(arm,event_kind=kind+"_gate",values=values))
   events.append(self._event(arm,values={"throughput_jobs_per_second":100,
    "warm_p95_ms":100,"cold_p95":100,"density":1,
    "reliability":1,"operational":1}))
  summary,_=m.analyze_events(events)
  self.assertEqual(summary["decisions"]["trusted_single_tenant"],
                   "no material performance winner")
 def test_analysis_outputs_are_byte_stable(self):
  event={"schema_version":1,"manifest_sha256":"m","campaign":"c","phase":"lifecycle","arm":"smolvm","repetition":0,"monotonic_ns":1,"timestamp_utc":"x","event_kind":"sample","success":True,"failure_class":"","diagnostic":"","values":{"duration_ms":10},"units":{"duration_ms":"ms"},"cache_class":"image-cold"}
  a,b=m.analyze_events([event]); c,d=m.analyze_events([event])
  self.assertEqual(json.dumps(a,sort_keys=True),json.dumps(c,sort_keys=True)); self.assertEqual(b,d)
 def test_cli_writes_summary_and_report(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td); e={"schema_version":1,"manifest_sha256":"m","campaign":"c","phase":"x","arm":"smolvm","repetition":0,"monotonic_ns":1,"timestamp_utc":"x","event_kind":"sample","success":True,"failure_class":"","diagnostic":"","values":{},"units":{}}
   (root/"raw.jsonl").write_text(json.dumps(e)+"\n"); out=root/"out"; self.assertEqual(m.main(["--campaign",str(root),"--output",str(out)]),0); self.assertTrue((out/"summary.json").exists())
if __name__=="__main__":unittest.main()
